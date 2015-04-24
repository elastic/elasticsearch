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

import org.elasticsearch.common.unit.TimeValue;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Suspends all threads on the specified node in order to simulate a long gc.
 */
public class LongGCDisruption extends SingleNodeDisruption {

    private final static Pattern[] unsafeClasses = new Pattern[]{
            // logging has shared JVM locks - we may suspend a thread and block other nodes from doing their thing
            Pattern.compile("Logger")
    };

    protected final String disruptedNode;
    private Set<Thread> suspendedThreads;

    public LongGCDisruption(Random random, String disruptedNode) {
        super(random);
        this.disruptedNode = disruptedNode;
    }

    @Override
    public synchronized void startDisrupting() {
        if (suspendedThreads == null) {
            suspendedThreads = new HashSet<>();
            stopNodeThreads(disruptedNode, suspendedThreads);
        } else {
            throw new IllegalStateException("can't disrupt twice, call stopDisrupting() first");
        }
    }

    @Override
    public synchronized void stopDisrupting() {
        if (suspendedThreads != null) {
            resumeThreads(suspendedThreads);
            suspendedThreads = null;
        }
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return TimeValue.timeValueMillis(0);
    }

    protected boolean stopNodeThreads(String node, Set<Thread> nodeThreads) {
        Set<Thread> allThreadsSet = Thread.getAllStackTraces().keySet();
        boolean stopped = false;
        final String nodeThreadNamePart = "[" + node + "]";
        for (Thread thread : allThreadsSet) {
            String name = thread.getName();
            if (name.contains(nodeThreadNamePart)) {
                if (thread.isAlive() && nodeThreads.add(thread)) {
                    stopped = true;
                    thread.suspend();
                    // double check the thread is not in a shared resource like logging. If so, let it go and come back..
                    boolean safe = true;
                    safe:
                    for (StackTraceElement stackElement : thread.getStackTrace()) {
                        String className = stackElement.getClassName();
                        for (Pattern unsafePattern : unsafeClasses) {
                            if (unsafePattern.matcher(className).find()) {
                                safe = false;
                                break safe;
                            }
                        }
                    }
                    if (!safe) {
                        thread.resume();
                        nodeThreads.remove(thread);
                    }
                }
            }
        }
        return stopped;
    }

    protected void resumeThreads(Set<Thread> threads) {
        for (Thread thread : threads) {
            thread.resume();
        }
    }
}
