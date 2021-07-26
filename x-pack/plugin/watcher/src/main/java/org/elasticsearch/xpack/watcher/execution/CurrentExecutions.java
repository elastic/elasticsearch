/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.watcher.WatcherState;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalState;

public final class CurrentExecutions implements Iterable<ExecutionService.WatchExecution> {

    private static final Logger logger = LogManager.getLogger(CurrentExecutions.class);
    private final ConcurrentMap<String, ExecutionService.WatchExecution> currentExecutions = new ConcurrentHashMap<>();
    // the condition of the lock is used to wait and signal the finishing of all executions on shutdown
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition empty = lock.newCondition();
    // a marker to not accept new executions, used when the watch service is powered down
    private SetOnce<Boolean> seal = new SetOnce<>();

    /**
     * Tries to put an watch execution class for a watch in the current executions
     *
     * @param id        The id of the watch
     * @param execution The watch execution class
     * @return          Returns true if watch with id already is in the current executions class, false otherwise
     */
    public boolean put(String id, ExecutionService.WatchExecution execution) {
        lock.lock();
        try {
            if (seal.get() != null) {
                // We shouldn't get here, because, ExecutionService#started should have been set to false
                throw illegalState("could not register execution [{}]. current executions are sealed and forbid registrations of " +
                        "additional executions.", id);
            }
            return currentExecutions.putIfAbsent(id, execution) != null;
        } finally {
            lock.unlock();
        }
    }

    public void remove(String id) {
        lock.lock();
        try {
            currentExecutions.remove(id);
            if (currentExecutions.isEmpty()) {
                empty.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Calling this method makes the class stop accepting new executions and throws and exception instead.
     * In addition it waits for a certain amount of time for current executions to finish before returning
     *
     * @param maxStopTimeout The maximum wait time to wait to current executions to finish
     * @param stoppedListener The listener that will set Watcher state to: {@link WatcherState#STOPPED}, may be a no-op assuming the
     *                       {@link WatcherState#STOPPED} is set elsewhere or not needed to be set.
     */
    void sealAndAwaitEmpty(TimeValue maxStopTimeout, Runnable stoppedListener) {
        assert stoppedListener != null;
        lock.lock();
        // We may have current executions still going on.
        // We should try to wait for the current executions to have completed.
        // Otherwise we can run into a situation where we didn't delete the watch from the .triggered_watches index,
        // but did insert into the history index. Upon start this can lead to DocumentAlreadyExistsException,
        // because we already stored the history record during shutdown...
        // (we always first store the watch record and then remove the triggered watch)
        try {
            seal.set(true);
            while (currentExecutions.size() > 0) {
                empty.await(maxStopTimeout.millis(), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            //fully stop Watcher after all executions are finished
            stoppedListener.run();
            lock.unlock();
        }
    }

    @Override
    public Iterator<ExecutionService.WatchExecution> iterator() {
        return currentExecutions.values().iterator();
    }
}
