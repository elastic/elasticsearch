/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.test.disruption;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Suspends all threads on the specified node in order to simulate a long gc.
 */
public class LongGCDisruption extends SingleNodeDisruption {

    private static final Pattern[] unsafeClasses = new Pattern[]{
            // logging has shared JVM locks - we may suspend a thread and block other nodes from doing their thing
        Pattern.compile("logging\\.log4j")
    };

    protected final String disruptedNode;
    private Set<Thread> suspendedThreads;
    private long stoppingTimeoutInMillis;

    public LongGCDisruption(Random random, String disruptedNode) {
        super(random);
        this.disruptedNode = disruptedNode;
    }

    @Override
    public synchronized void startDisrupting() {
        if (suspendedThreads == null) {
            boolean success = false;
            try {
                suspendedThreads = ConcurrentHashMap.newKeySet();

                final String currentThreadNamme = Thread.currentThread().getName();
                assert currentThreadNamme.contains("[" + disruptedNode + "]") == false :
                    "current thread match pattern. thread name: " + currentThreadNamme + ", node: " + disruptedNode;
                // we spawn a background thread to protect against deadlock which can happen
                // if there are shared resources between caller thread and and suspended threads
                // see unsafeClasses to how to avoid that
                final AtomicReference<Exception> stoppingError = new AtomicReference<>();
                final Thread stoppingThread = new Thread(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        stoppingError.set(e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        while (stopNodeThreads(disruptedNode, suspendedThreads)) ;
                    }
                });
                stoppingThread.setName(currentThreadNamme + "[LongGCDisruption][threadStopper]");
                stoppingThread.start();
                try {
                    stoppingThread.join(getStoppingTimeoutInMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (stoppingError.get() != null) {
                    throw new RuntimeException("uknown error while stopping threads", stoppingError.get());
                }
                if (stoppingThread.isAlive()) {
                    logger.warn("failed to stop node [{}]'s thread within [{}] millis. Stopping thread stack trace:\n {}"
                        , disruptedNode, getStoppingTimeoutInMillis(), stackTrace(stoppingThread));
                    stoppingThread.interrupt(); // best effort;
                    throw new RuntimeException("stopping node threads took too long");
                }
                success = true;
            } finally {
                if (success == false) {
                    // resume threads if failed
                    resumeThreads(suspendedThreads);
                    suspendedThreads = null;
                }
            }
        } else {
            throw new IllegalStateException("can't disrupt twice, call stopDisrupting() first");
        }
    }

    private String stackTrace(Thread thread) {
        String result = "";
        for (StackTraceElement s : thread.getStackTrace()) {
            result += "\tat " + s.getClassName() + "." + s.getMethodName()
                + "(" + s.getFileName() + ":" + s.getLineNumber() + ")" + "\n";
        }
        return result;
    }

    @Override
    public synchronized void stopDisrupting() {
        if (suspendedThreads != null) {
            resumeThreads(suspendedThreads);
            suspendedThreads = null;
        }
    }

    @Override
    public void removeAndEnsureHealthy(InternalTestCluster cluster) {
        removeFromCluster(cluster);
        ensureNodeCount(cluster);
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return TimeValue.timeValueMillis(0);
    }

    @SuppressWarnings("deprecation") // stops/resumes threads intentionally
    @SuppressForbidden(reason = "stops/resumes threads intentionally")
    /**
     * resolves all threads belonging to given node and suspends them if their current stack trace
     * is "safe". Threads are added to nodeThreads if suspended.
     *
     * returns true if some live threads were found. The caller is expected to call this method
     * until no more "live" are found.
     *
     */
    protected boolean stopNodeThreads(String node, Set<Thread> nodeThreads) {
        Thread[] allThreads = null;
        while (allThreads == null) {
            allThreads = new Thread[Thread.activeCount()];
            if (Thread.enumerate(allThreads) > allThreads.length) {
                // we didn't make enough space, retry
                allThreads = null;
            }
        }
        boolean liveThreadsFound = false;
        final String nodeThreadNamePart = "[" + node + "]";
        for (Thread thread : allThreads) {
            if (thread == null) {
                continue;
            }
            String name = thread.getName();
            if (name.contains(nodeThreadNamePart)) {
                if (thread.isAlive() && nodeThreads.add(thread)) {
                    liveThreadsFound = true;
                    logger.trace("stopping thread [{}]", name);
                    thread.suspend();
                    // double check the thread is not in a shared resource like logging. If so, let it go and come back..
                    boolean safe = true;
                    safe:
                    for (StackTraceElement stackElement : thread.getStackTrace()) {
                        String className = stackElement.getClassName();
                        for (Pattern unsafePattern : getUnsafeClasses()) {
                            if (unsafePattern.matcher(className).find()) {
                                safe = false;
                                break safe;
                            }
                        }
                    }
                    if (!safe) {
                        logger.trace("resuming thread [{}] as it is in a critical section", name);
                        thread.resume();
                        nodeThreads.remove(thread);
                    }
                }
            }
        }
        return liveThreadsFound;
    }

    // for testing
    protected Pattern[] getUnsafeClasses() {
        return unsafeClasses;
    }

    // for testing
    protected long getStoppingTimeoutInMillis() {
        return 30 * 1000L;
    }

    @SuppressWarnings("deprecation") // stops/resumes threads intentionally
    @SuppressForbidden(reason = "stops/resumes threads intentionally")
    protected void resumeThreads(Set<Thread> threads) {
        for (Thread thread : threads) {
            thread.resume();
        }
    }
}
