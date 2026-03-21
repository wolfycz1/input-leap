/*
 * InputLeap -- mouse and keyboard sharing utility
 * Copyright (C) 2012-2016 Symless Ltd.
 * Copyright (C) 2004 Chris Schoeneman
 *
 * This package is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * found in the file LICENSE that should have accompanied this file.
 *
 * This package is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "net/SocketMultiplexer.h"

#include "net/ISocketMultiplexerJob.h"
#include "mt/Thread.h"
#include "arch/Arch.h"
#include "arch/XArch.h"
#include "base/Log.h"
#include <vector>

namespace inputleap {

class CursorMultiplexerJob : public ISocketMultiplexerJob {
public:
    MultiplexerJobStatus run(bool readable, bool writable, bool error) override
    {
        (void) readable;
        (void) writable;
        (void) error;

        return {false, {}};
    }

    ArchSocket getSocket() const override { return {}; }
    bool isReadable() const override { return false; }
    bool isWritable() const override { return false; }
    bool isCursor() const override { return true; }
};


SocketMultiplexer::SocketMultiplexer() :
    m_thread(nullptr),
    m_update(false)
{
    // start thread
    m_thread = new Thread([this](){ service_thread(); });
}

SocketMultiplexer::~SocketMultiplexer()
{
    m_thread->cancel();
    m_thread->unblockPollSocket();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        jobs_are_ready_ = true;
        cv_jobs_ready_.notify_all();
    }
    m_thread->wait();
    delete m_thread;
}

void SocketMultiplexer::addSocket(ISocket* socket, std::unique_ptr<ISocketMultiplexerJob>&& job)
{
    LOG_DEBUG2("SocketMultiplexer::addSocket() called: socket=%p, job=%p", socket, job.get());
    assert(socket != nullptr);
    assert(job != nullptr);

    // prevent other threads from locking the job list
    lockJobListLock();

    LOG_DEBUG2("addSocket: unblocking poll");
    // break thread out of poll
    m_thread->unblockPollSocket();

    // lock the job list
    lockJobList();

    // insert/replace job
    SocketJobMap::iterator i = m_socketJobMap.find(socket);
    if (i == m_socketJobMap.end()) {
        LOG_DEBUG2("addSocket: inserting NEW socket job");
        // we *must* put the job at the end so the order of jobs in
        // the list continue to match the order of jobs in pfds in
        // service_thread().
        JobCursor j = m_socketJobs.insert(m_socketJobs.end(), std::move(job));
        m_update     = true;
        m_socketJobMap.insert(std::make_pair(socket, j));
        LOG_DEBUG2("addSocket: socket %p added to job list", socket);
    }
    else {
        LOG_DEBUG2("addSocket: replacing EXISTING job for socket %p", socket);
        *(i->second) = std::move(job);
        m_update = true;
    }

    // unlock the job list
    unlockJobList();
    LOG_DEBUG2("SocketMultiplexer::addSocket completed");
}

void
SocketMultiplexer::removeSocket(ISocket* socket)
{
    assert(socket != nullptr);

    // prevent other threads from locking the job list
    lockJobListLock();

    // break thread out of poll
    m_thread->unblockPollSocket();

    // lock the job list
    lockJobList();

    // remove job.  rather than removing it from the map we put nullptr
    // in the list instead so the order of jobs in the list continues
    // to match the order of jobs in pfds in service_thread().
    SocketJobMap::iterator i = m_socketJobMap.find(socket);
    if (i != m_socketJobMap.end()) {
        if (*(i->second)) {
            i->second->reset();
            m_update = true;
        }
    }

    // unlock the job list
    unlockJobList();
}

void SocketMultiplexer::service_thread()
{
    LOG_DEBUG2("SocketMultiplexer::service_thread() called");
    std::vector<IArchNetwork::PollEntry> pfds;
    IArchNetwork::PollEntry pfd;

    // service the connections
    for (;;) {
        Thread::testCancel();

        // wait until there are jobs to handle
        {
            std::unique_lock<std::mutex> lock(mutex_);
            LOG_DEBUG2("service_thread: waiting for jobs...");
            cv_jobs_ready_.wait(lock, [this](){ return jobs_are_ready_; });
            LOG_DEBUG2("service_thread: jobs ready");
        }

        // lock the job list
        lockJobListLock();
        lockJobList();

        // collect poll entries
        if (m_update) {
            LOG_DEBUG2("service_thread: rebuilding poll list (m_update=true)");
            m_update = false;
            pfds.clear();
            pfds.reserve(m_socketJobMap.size());

            JobCursor cursor    = newCursor();
            JobCursor jobCursor = nextCursor(cursor);
            size_t count = 0;
            while (jobCursor != m_socketJobs.end()) {
                if (*jobCursor) {
                    pfd.m_socket = (*jobCursor)->getSocket();
                    pfd.m_events = 0;
                    if ((*jobCursor)->isReadable()) {
                        pfd.m_events |= IArchNetwork::kPOLLIN;
                    }
                    if ((*jobCursor)->isWritable()) {
                        pfd.m_events |= IArchNetwork::kPOLLOUT;
                    }
                    pfds.push_back(pfd);
                    ++count;
                }
                jobCursor = nextCursor(cursor);
            }
            deleteCursor(cursor);
            LOG_DEBUG2("service_thread: poll list built with %zu entries", count);
        }

        int poll_status;
        try {
            // check for status
            if (!pfds.empty()) {
                LOG_DEBUG2("service_thread: polling %zu sockets", pfds.size());
                poll_status = ARCH->pollSocket(&pfds[0], static_cast<int>(pfds.size()), -1);
                LOG_DEBUG2("service_thread: poll returned %d", poll_status);
            }
            else {
                LOG_DEBUG2("service_thread: no sockets to poll");
                poll_status = 0;
            }
        }
        catch (XArchNetwork& e) {
            LOG_WARN("error in socket multiplexer: %s", e.what());
            LOG_DEBUG2("service_thread: poll exception caught");
            poll_status = 0;
        }

        if (poll_status != 0) {
            LOG_DEBUG2("service_thread: processing poll events");
            // iterate over socket jobs, invoking each and saving the
            // new job.
            std::uint32_t i = 0;
            JobCursor cursor    = newCursor();
            JobCursor jobCursor = nextCursor(cursor);
            while (i < pfds.size() && jobCursor != m_socketJobs.end()) {
                if (*jobCursor != nullptr) {
                    // get poll state
                    unsigned short revents = pfds[i].m_revents;
                    bool read  = ((revents & IArchNetwork::kPOLLIN) != 0);
                    bool write = ((revents & IArchNetwork::kPOLLOUT) != 0);
                    bool error = ((revents & (IArchNetwork::kPOLLERR |
                                              IArchNetwork::kPOLLNVAL)) != 0);

                    LOG_DEBUG2("service_thread: job[%u] events: read=%d write=%d error=%d", i, read, write, error);


                    // run job
                    MultiplexerJobStatus status = (*jobCursor)->run(read, write, error);

                    if (!status.continue_servicing) {
                        LOG_DEBUG2("service_thread: job[%u] requested stop", i);
                        std::lock_guard<std::mutex> lock(mutex_);
                        jobCursor->reset();
                        m_update = true;
                    } else if (status.new_job) {
                        LOG_DEBUG2("service_thread: job[%u] replaced with new job", i);
                        std::lock_guard<std::mutex> lock(mutex_);
                        *jobCursor = std::move(status.new_job);
                        m_update = true;
                    }
                    ++i;
                }

                // next job
                jobCursor = nextCursor(cursor);
            }
            deleteCursor(cursor);
        }

        // delete any removed socket jobs
        for (SocketJobMap::iterator i = m_socketJobMap.begin();
                            i != m_socketJobMap.end();) {
            if (*(i->second) == nullptr) {
                LOG_DEBUG2("service_thread: removing socket job %p", i->first);
                m_socketJobs.erase(i->second);
                m_socketJobMap.erase(i++);
                m_update = true;
            }
            else {
                ++i;
            }
        }

        // unlock the job list
        unlockJobList();
    }
    LOG_DEBUG2("SocketMultiplexer::service_thread finished");
}

SocketMultiplexer::JobCursor
SocketMultiplexer::newCursor()
{
    std::lock_guard<std::mutex> lock(mutex_);
    return m_socketJobs.insert(m_socketJobs.begin(), std::make_unique<CursorMultiplexerJob>());
}

SocketMultiplexer::JobCursor
SocketMultiplexer::nextCursor(JobCursor cursor)
{
    std::lock_guard<std::mutex> lock(mutex_);
    JobCursor j = m_socketJobs.end();
    JobCursor i = cursor;
    while (++i != m_socketJobs.end()) {
        if (*i && !(*i)->isCursor()) {
            // found a real job (as opposed to a cursor)
            j = i;

            // move our cursor just past the job
            m_socketJobs.splice(++i, m_socketJobs, cursor);
            break;
        }
    }
    return j;
}

void
SocketMultiplexer::deleteCursor(JobCursor cursor)
{
    std::lock_guard<std::mutex> lock(mutex_);
    m_socketJobs.erase(cursor);
}

void
SocketMultiplexer::lockJobListLock()
{
    LOG_DEBUG2("SocketMultiplexer::lockJobListLock() called");
    std::unique_lock<std::mutex> lock(mutex_);
    LOG_DEBUG2("SocketMultiplexer::lockJobListLock(): mutex acquired");

    LOG_DEBUG2("lockJobListLock(): waiting for job_list_lock_lock (currently=%d)",
               job_list_lock_lock_is_locked_);
    // wait for the lock on the lock
    cv_job_list_lock_locked_.wait(lock, [this](){ return !job_list_lock_lock_is_locked_; });
    LOG_DEBUG2("lockJobListLock(): wait finished, acquiring lock");

    // take ownership of the lock on the lock
    job_list_lock_lock_is_locked_ = true;
    //m_jobListLockLocker  = new Thread(Thread::getCurrentThread());
    m_jobListLockLocker = std::this_thread::get_id();
    LOG_DEBUG2("lockJobListLock(): lock acquired by current thread");

    LOG_DEBUG2("lockJobListLock() complete");
}

void
SocketMultiplexer::lockJobList()
{
    LOG_DEBUG2("SocketMultiplexer::lockJobList() called");
    std::unique_lock<std::mutex> lock(mutex_);
    LOG_DEBUG2("lockJobList(): mutex acquired");

    // make sure we're the one that called lockJobListLock()
    if (m_jobListLockLocker == std::thread::id()) {
        LOG_DEBUG2("lockJobList(): ERROR -> m_jobListLockLocker is empty!");
    } else if (m_jobListLockLocker != std::this_thread::get_id()) {
        LOG_DEBUG2("lockJobList(): ERROR -> thread mismatch!");
    }

    assert(m_jobListLockLocker != std::thread::id());
    assert(m_jobListLockLocker == std::this_thread::get_id());

    LOG_DEBUG2("lockJobList(): waiting for jobs_list_lock (currently=%d)",
               jobs_list_lock_is_locked_);
    // wait for the job list lock
    cv_jobs_list_lock_.wait(lock, [this]() { return !jobs_list_lock_is_locked_; });

    LOG_DEBUG2("lockJobList(): wait finished, acquiring job list lock");

    // take ownership of the lock
    jobs_list_lock_is_locked_ = true;
    m_jobListLocker     = m_jobListLockLocker;
    m_jobListLockLocker = std::thread::id();

    LOG_DEBUG2("lockJobList(): ownership transferred");

    // release the lock on the lock
    job_list_lock_lock_is_locked_ = false;
    LOG_DEBUG2("lockJobList(): released job_list_lock_lock");
    cv_job_list_lock_locked_.notify_all();
    LOG_DEBUG2("lockJobList(): notified all waiting threads");

    LOG_DEBUG2("lockJobList() completed");
}

void
SocketMultiplexer::unlockJobList()
{
    std::lock_guard<std::mutex> lock(mutex_);

    // make sure we're the one that called lockJobList()
    assert(m_jobListLocker == std::this_thread::get_id());

    // release the lock
    m_jobListLocker = std::thread::id();
    jobs_list_lock_is_locked_ = false;
    cv_jobs_list_lock_.notify_one();

    // set new jobs ready state
    bool isReady = !m_socketJobMap.empty();
    if (jobs_are_ready_ != isReady) {
        jobs_are_ready_ = isReady;
        cv_jobs_ready_.notify_one();
    }
}

} // namespace inputleap
