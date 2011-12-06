/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.thread;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * A {@link Runnable} that changes the current thread name and reverts it back
 * when its execution ends.  To change the default thread names set by Netty,
 * use {@link #setThreadNameDeterminer(ThreadNameDeterminer)}.
 *
 *
 */
public class ThreadRenamingRunnable implements Runnable {

    private static final ESLogger logger = Loggers.getLogger(ThreadRenamingRunnable.class);

    private static volatile ThreadNameDeterminer threadNameDeterminer =
            ThreadNameDeterminer.PROPOSED;

    /**
     * Returns the {@link ThreadNameDeterminer} which overrides the proposed
     * new thread name.
     */
    public static ThreadNameDeterminer getThreadNameDeterminer() {
        return threadNameDeterminer;
    }

    /**
     * Sets the {@link ThreadNameDeterminer} which overrides the proposed new
     * thread name.  Please note that the specified {@link ThreadNameDeterminer}
     * affects only new {@link ThreadRenamingRunnable}s; the existing instances
     * are not affected at all.  Therefore, you should make sure to call this
     * method at the earliest possible point (i.e. before any Netty worker
     * thread starts) for consistent thread naming.  Otherwise, you might see
     * the default thread names and the new names appear at the same time in
     * the full thread dump.
     */
    public static void setThreadNameDeterminer(ThreadNameDeterminer threadNameDeterminer) {
        if (threadNameDeterminer == null) {
            throw new NullPointerException("threadNameDeterminer");
        }
        ThreadRenamingRunnable.threadNameDeterminer = threadNameDeterminer;
    }

    private final Runnable runnable;
    private final String proposedThreadName;

    /**
     * Creates a new instance which wraps the specified {@code runnable}
     * and changes the thread name to the specified thread name when the
     * specified {@code runnable} is running.
     */
    public ThreadRenamingRunnable(Runnable runnable, String proposedThreadName) {
        if (runnable == null) {
            throw new NullPointerException("runnable");
        }
        if (proposedThreadName == null) {
            throw new NullPointerException("proposedThreadName");
        }
        this.runnable = runnable;
        this.proposedThreadName = proposedThreadName;
    }

    public void run() {
        final Thread currentThread = Thread.currentThread();
        final String oldThreadName = currentThread.getName();
        final String newThreadName = getNewThreadName(oldThreadName);

        // Change the thread name before starting the actual runnable.
        boolean renamed = false;
        if (!oldThreadName.equals(newThreadName)) {
            try {
                currentThread.setName(newThreadName);
                renamed = true;
            } catch (SecurityException e) {
                logger.debug("Failed to rename a thread due to security restriction.", e);
            }
        }

        // Run the actual runnable and revert the name back when it ends.
        try {
            runnable.run();
        } finally {
            if (renamed) {
                // Revert the name back if the current thread was renamed.
                // We do not check the exception here because we know it works.
                currentThread.setName(oldThreadName);
            }
        }
    }

    private String getNewThreadName(String currentThreadName) {
        String newThreadName = null;

        try {
            newThreadName = getThreadNameDeterminer().determineThreadName(currentThreadName, proposedThreadName);
        } catch (Throwable t) {
            logger.warn("Failed to determine the thread name", t);
        }

        return newThreadName == null ? currentThreadName : newThreadName;
    }
}
