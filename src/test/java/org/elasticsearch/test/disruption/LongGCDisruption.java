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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class LongGCDisruption extends SingleNodeDisruption {

    volatile boolean disrupting;
    volatile Thread worker;

    final long intervalBetweenDelaysMin;
    final long intervalBetweenDelaysMax;
    final long delayDurationMin;
    final long delayDurationMax;


    public LongGCDisruption(Random random) {
        this(null, random);
    }

    public LongGCDisruption(String disruptedNode, Random random) {
        this(disruptedNode, random, 100, 200, 300, 20000);
    }

    public LongGCDisruption(String disruptedNode, Random random, long intervalBetweenDelaysMin,
                            long intervalBetweenDelaysMax, long delayDurationMin, long delayDurationMax) {
        this(random, intervalBetweenDelaysMin, intervalBetweenDelaysMax, delayDurationMin, delayDurationMax);
        this.disruptedNode = disruptedNode;
    }

    public LongGCDisruption(Random random,
                            long intervalBetweenDelaysMin, long intervalBetweenDelaysMax, long delayDurationMin,
                            long delayDurationMax) {
        super(random);
        this.intervalBetweenDelaysMin = intervalBetweenDelaysMin;
        this.intervalBetweenDelaysMax = intervalBetweenDelaysMax;
        this.delayDurationMin = delayDurationMin;
        this.delayDurationMax = delayDurationMax;
    }

    final static AtomicInteger thread_ids = new AtomicInteger();

    @Override
    public void startDisrupting() {
        disrupting = true;
        worker = new Thread(new BackgroundWorker(), "long_gc_simulation_" + thread_ids.incrementAndGet());
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public void stopDisrupting() {
        if (worker == null) {
            return;
        }
        logger.info("stopping long GCs on [{}]", disruptedNode);
        disrupting = false;
        worker.interrupt();
        try {
            worker.join(2 * (intervalBetweenDelaysMax + delayDurationMax));
        } catch (InterruptedException e) {
            logger.info("background thread failed to stop");
        }
        worker = null;
    }

    final static Pattern[] unsafeClasses = new Pattern[]{
            // logging has shared JVM locks - we may suspend a thread and block other nodes from doing their thing
            Pattern.compile("Logger")
    };

    private boolean stopNodeThreads(String node, Set<Thread> nodeThreads) {
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

    private void resumeThreads(Set<Thread> threads) {
        for (Thread thread : threads) {
            thread.resume();
        }
    }

    private void simulateLongGC(final TimeValue duration) throws InterruptedException {
        final String disruptionNodeCopy = disruptedNode;
        if (disruptionNodeCopy == null) {
            return;
        }
        logger.info("node [{}] goes into GC for for [{}]", disruptionNodeCopy, duration);
        final Set<Thread> nodeThreads = new HashSet<>();
        try {
            while (stopNodeThreads(disruptionNodeCopy, nodeThreads)) ;
            if (!nodeThreads.isEmpty()) {
                Thread.sleep(duration.millis());
            }
        } finally {
            logger.info("node [{}] resumes from GC", disruptionNodeCopy);
            resumeThreads(nodeThreads);
        }
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return TimeValue.timeValueMillis(0);
    }

    class BackgroundWorker implements Runnable {

        @Override
        public void run() {
            while (disrupting && disruptedNode != null) {
                try {
                    TimeValue duration = new TimeValue(delayDurationMin + random.nextInt((int) (delayDurationMax - delayDurationMin)));
                    simulateLongGC(duration);

                    duration = new TimeValue(intervalBetweenDelaysMin + random.nextInt((int) (intervalBetweenDelaysMax - intervalBetweenDelaysMin)));
                    if (disrupting && disruptedNode != null) {
                        Thread.sleep(duration.millis());
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    logger.error("error in background worker", e);
                }
            }
        }
    }

}
