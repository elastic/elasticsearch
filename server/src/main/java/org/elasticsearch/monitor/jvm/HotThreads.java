/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

public class HotThreads {

    private static final Object mutex = new Object();

    private static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("date_optional_time");

    private int busiestThreads = 3;
    private TimeValue interval = new TimeValue(500, TimeUnit.MILLISECONDS);
    private TimeValue threadElementsSnapshotDelay = new TimeValue(10);
    private int threadElementsSnapshotCount = 10;
    private String type = "cpu";
    private boolean ignoreIdleThreads = true;

    public HotThreads interval(TimeValue interval) {
        this.interval = interval;
        return this;
    }

    public HotThreads busiestThreads(int busiestThreads) {
        this.busiestThreads = busiestThreads;
        return this;
    }

    public HotThreads ignoreIdleThreads(boolean ignoreIdleThreads) {
        this.ignoreIdleThreads = ignoreIdleThreads;
        return this;
    }

    public HotThreads threadElementsSnapshotDelay(TimeValue threadElementsSnapshotDelay) {
        this.threadElementsSnapshotDelay = threadElementsSnapshotDelay;
        return this;
    }

    public HotThreads threadElementsSnapshotCount(int threadElementsSnapshotCount) {
        this.threadElementsSnapshotCount = threadElementsSnapshotCount;
        return this;
    }

    public HotThreads type(String type) {
        if ("cpu".equals(type) || "wait".equals(type) || "block".equals(type)) {
            this.type = type;
        } else {
            throw new IllegalArgumentException("type not supported [" + type + "]");
        }
        return this;
    }

    public String detect() throws Exception {
        synchronized (mutex) {
            return innerDetect(ManagementFactory.getThreadMXBean(), Thread.currentThread().getId());
        }
    }

    static boolean isIdleThread(ThreadInfo threadInfo) {
        String threadName = threadInfo.getThreadName();

        // NOTE: these are likely JVM dependent
        if (threadName.equals("Signal Dispatcher") ||
            threadName.equals("Finalizer") ||
            threadName.equals("Reference Handler")) {
            return true;
        }

        for (StackTraceElement frame : threadInfo.getStackTrace()) {
            String className = frame.getClassName();
            String methodName = frame.getMethodName();
            if (className.equals("java.util.concurrent.ThreadPoolExecutor") &&
                methodName.equals("getTask")) {
                return true;
            }
            if (className.equals("sun.nio.ch.SelectorImpl") &&
                methodName.equals("select")) {
                return true;
            }
            if (className.equals("org.elasticsearch.threadpool.ThreadPool$CachedTimeThread") &&
                methodName.equals("run")) {
                return true;
            }
            if (className.equals("org.elasticsearch.indices.ttl.IndicesTTLService$Notifier") &&
                methodName.equals("await")) {
                return true;
            }
            if (className.equals("java.util.concurrent.LinkedTransferQueue") &&
                methodName.equals("poll")) {
                return true;
            }
        }

        return false;
    }

    String innerDetect(ThreadMXBean threadBean, long currentThreadId) throws Exception {
        if (threadBean.isThreadCpuTimeSupported() == false) {
            throw new ElasticsearchException("thread CPU time is not supported on this JDK");
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Hot threads at ");
        sb.append(DATE_TIME_FORMATTER.format(LocalDateTime.now(Clock.systemUTC())));
        sb.append(", interval=");
        sb.append(interval);
        sb.append(", busiestThreads=");
        sb.append(busiestThreads);
        sb.append(", ignoreIdleThreads=");
        sb.append(ignoreIdleThreads);
        sb.append(":\n");

        Map<Long, MyThreadInfo> threadInfos = new HashMap<>();
        for (long threadId : threadBean.getAllThreadIds()) {
            // ignore our own thread...
            if (currentThreadId == threadId) {
                continue;
            }
            long cpu = threadBean.getThreadCpuTime(threadId);
            if (cpu == -1) {
                continue;
            }
            ThreadInfo info = threadBean.getThreadInfo(threadId, 0);
            if (info == null) {
                continue;
            }
            threadInfos.put(threadId, new MyThreadInfo(cpu, info));
        }
        Thread.sleep(interval.millis());
        for (long threadId : threadBean.getAllThreadIds()) {
            // ignore our own thread...
            if (currentThreadId == threadId) {
                continue;
            }
            long cpu = threadBean.getThreadCpuTime(threadId);
            if (cpu == -1) {
                threadInfos.remove(threadId);
                continue;
            }
            ThreadInfo info = threadBean.getThreadInfo(threadId, 0);
            if (info == null) {
                threadInfos.remove(threadId);
                continue;
            }
            MyThreadInfo data = threadInfos.get(threadId);
            if (data != null) {
                data.setDelta(cpu, info);
            } else {
                threadInfos.remove(threadId);
            }
        }
        // sort by delta CPU time on thread.
        List<MyThreadInfo> hotties = new ArrayList<>(threadInfos.values());
        final int busiestThreads = Math.min(this.busiestThreads, hotties.size());
        // skip that for now
        final ToLongFunction<MyThreadInfo> getter;
        if ("cpu".equals(type)) {
            getter = o -> {
                assert o.cpuTime >= -1 : "cpu time should not be negative, but was " + o.cpuTime + ", thread info: " + o.info;
                return o.cpuTime;
            };
        } else if ("wait".equals(type)) {
            getter = o -> {
                assert o.waitedTime >= -1 : "waited time should not be negative, but was " + o.waitedTime + ", thread info: " + o.info;
                return o.waitedTime;
            };
        } else if ("block".equals(type)) {
            getter = o -> {
                assert o.blockedTime >= -1 : "blocked time should not be negative, but was " + o.blockedTime + ", thread info: " + o.info;
                return o.blockedTime;
            };
        } else {
            throw new IllegalArgumentException("expected thread type to be either 'cpu', 'wait', or 'block', but was " + type);
        }

        CollectionUtil.introSort(hotties, Comparator.comparingLong(getter).reversed());

        // analyse N stack traces for M busiest threads
        long[] ids = new long[busiestThreads];
        for (int i = 0; i < busiestThreads; i++) {
            MyThreadInfo info = hotties.get(i);
            ids[i] = info.info.getThreadId();
        }
        ThreadInfo[][] allInfos = new ThreadInfo[threadElementsSnapshotCount][];
        for (int j = 0; j < threadElementsSnapshotCount; j++) {
            // NOTE, javadoc of getThreadInfo says: If a thread of the given ID is not alive or does not exist,
            // null will be set in the corresponding element in the returned array. A thread is alive if it has
            // been started and has not yet died.
            allInfos[j] = threadBean.getThreadInfo(ids, Integer.MAX_VALUE);
            Thread.sleep(threadElementsSnapshotDelay.millis());
        }
        for (int t = 0; t < busiestThreads; t++) {
            long time = getter.applyAsLong(hotties.get(t));
            String threadName = null;
            for (ThreadInfo[] info : allInfos) {
                if (info != null && info[t] != null) {
                    if (ignoreIdleThreads && isIdleThread(info[t])) {
                        info[t] = null;
                        continue;
                    }
                    threadName = info[t].getThreadName();
                    break;
                }
            }
            if (threadName == null) {
                continue; // thread is not alive yet or died before the first snapshot - ignore it!
            }
            double percent = (((double) time) / interval.nanos()) * 100;
            sb.append(String.format(Locale.ROOT, "%n%4.1f%% (%s out of %s) %s usage by thread '%s'%n",
                percent, TimeValue.timeValueNanos(time), interval, type, threadName));
            // for each snapshot (2nd array index) find later snapshot for same thread with max number of
            // identical StackTraceElements (starting from end of each)
            boolean[] done = new boolean[threadElementsSnapshotCount];
            for (int i = 0; i < threadElementsSnapshotCount; i++) {
                if (done[i]) continue;
                int maxSim = 1;
                boolean[] similars = new boolean[threadElementsSnapshotCount];
                for (int j = i + 1; j < threadElementsSnapshotCount; j++) {
                    if (done[j]) continue;
                    int similarity = similarity(allInfos[i][t], allInfos[j][t]);
                    if (similarity > maxSim) {
                        maxSim = similarity;
                        similars = new boolean[threadElementsSnapshotCount];
                    }
                    if (similarity == maxSim) similars[j] = true;
                }
                // print out trace maxSim levels of i, and mark similar ones as done
                int count = 1;
                for (int j = i + 1; j < threadElementsSnapshotCount; j++) {
                    if (similars[j]) {
                        done[j] = true;
                        count++;
                    }
                }
                if (allInfos[i][t] != null) {
                    final StackTraceElement[] show = allInfos[i][t].getStackTrace();
                    if (count == 1) {
                        sb.append(String.format(Locale.ROOT, "  unique snapshot%n"));
                        for (int l = 0; l < show.length; l++) {
                            sb.append(String.format(Locale.ROOT, "    %s%n", show[l]));
                        }
                    } else {
                        sb.append(String.format(Locale.ROOT, "  %d/%d snapshots sharing following %d elements%n",
                            count, threadElementsSnapshotCount, maxSim));
                        for (int l = show.length - maxSim; l < show.length; l++) {
                            sb.append(String.format(Locale.ROOT, "    %s%n", show[l]));
                        }
                    }
                }
            }
        }
        return sb.toString();
    }

    private static final StackTraceElement[] EMPTY = new StackTraceElement[0];

    int similarity(ThreadInfo threadInfo, ThreadInfo threadInfo0) {
        StackTraceElement[] s1 = threadInfo == null ? EMPTY : threadInfo.getStackTrace();
        StackTraceElement[] s2 = threadInfo0 == null ? EMPTY : threadInfo0.getStackTrace();
        int i = s1.length - 1;
        int j = s2.length - 1;
        int rslt = 0;
        while (i >= 0 && j >= 0 && s1[i].equals(s2[j])) {
            rslt++;
            i--;
            j--;
        }
        return rslt;
    }


    class MyThreadInfo {
        long cpuTime;
        long blockedCount;
        long blockedTime;
        long waitedCount;
        long waitedTime;
        boolean deltaDone;
        ThreadInfo info;

        MyThreadInfo(long cpuTime, ThreadInfo info) {
            blockedCount = info.getBlockedCount();
            blockedTime = info.getBlockedTime();
            waitedCount = info.getWaitedCount();
            waitedTime = info.getWaitedTime();
            this.cpuTime = cpuTime;
            this.info = info;
        }

        void setDelta(long cpuTime, ThreadInfo info) {
            if (deltaDone) throw new IllegalStateException("setDelta already called once");
            blockedCount = info.getBlockedCount() - blockedCount;
            blockedTime = info.getBlockedTime() - blockedTime;
            waitedCount = info.getWaitedCount() - waitedCount;
            waitedTime = info.getWaitedTime() - waitedTime;
            this.cpuTime = cpuTime - this.cpuTime;
            deltaDone = true;
            this.info = info;
        }
    }
}
