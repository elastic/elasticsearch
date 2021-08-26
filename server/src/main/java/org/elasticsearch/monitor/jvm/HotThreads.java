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
import java.util.Arrays;
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
    private static final StackTraceElement[] EMPTY_STACK = new StackTraceElement[0];

    private int busiestThreads = 3;
    private TimeValue interval = new TimeValue(500, TimeUnit.MILLISECONDS);
    private TimeValue threadElementsSnapshotDelay = new TimeValue(10, TimeUnit.MILLISECONDS);
    private int threadElementsSnapshotCount = 10;
    private ReportType type = ReportType.CPU;
    private boolean ignoreIdleThreads = true;

    private static final List<String[]> knownElasticIdleFrames = Arrays.asList(
        new String[] {"java.util.concurrent.ThreadPoolExecutor", "getTask"},
        new String[] {"sun.nio.ch.SelectorImpl", "select"},
        new String[] {"org.elasticsearch.threadpool.ThreadPool$CachedTimeThread", "run"},
        new String[] {"org.elasticsearch.indices.ttl.IndicesTTLService$Notifier", "await"},
        new String[] {"java.util.concurrent.LinkedTransferQueue", "poll"}
    );

    // NOTE: these are JVM dependent
    private static final List<String> knownJvmInternalThreads = Arrays.asList(
        "Signal Dispatcher", "Finalizer", "Reference Handler", "Notification Thread", "Common-Cleaner"
    );

    public enum ReportType {

        CPU("cpu"),
        WAIT("wait"),
        BLOCK("block");

        private final String type;

        ReportType(String type) {
            this.type = type;
        }

        public String getTypeValue() {
            return type;
        }

        public static ReportType of(String type) {
            for (var report : values()) {
                if (report.type.equals(type)) {
                    return report;
                }
            }
            throw new IllegalArgumentException("type not supported [" + type + "]");
        }
    }

    static class ThreadTimeAccumulator {
        long cpuTime;
        long blockedCount;
        long blockedTime;
        long waitedCount;
        long waitedTime;
        long threadId;

        ThreadTimeAccumulator(ThreadInfo info, long cpuTime) {
            this.blockedCount = info.getBlockedCount();
            this.blockedTime = info.getBlockedTime();
            this.waitedCount = info.getWaitedCount();
            this.waitedTime = info.getWaitedTime();
            this.cpuTime = cpuTime;
            this.threadId = info.getThreadId();
        }

        void applyDelta(ThreadTimeAccumulator previous) {
            if (previous != null) {
                this.blockedCount -= previous.blockedCount;
                this.blockedTime -= previous.blockedTime;
                this.waitedCount -= previous.waitedCount;
                this.waitedTime -= previous.waitedTime;
                this.cpuTime -= previous.cpuTime;
            }
        }

        static ToLongFunction<ThreadTimeAccumulator> valueGetterForReportType(ReportType type) {
            switch (type) {
                case CPU:
                    return o -> {
                        assert o.cpuTime >= -1 : "cpu time should not be negative, but was " + o.cpuTime + ", thread id: " + o.threadId;
                        return o.cpuTime;
                    };
                case WAIT:
                    return o -> {
                        assert o.waitedTime >= -1 : "waited time should not be negative, but was " + o.waitedTime + ", thread id: " + o.threadId;
                        return o.waitedTime;
                    };
                case BLOCK:
                    return o -> {
                        assert o.blockedTime >= -1 : "blocked time should not be negative, but was " + o.blockedTime + ", thread id: " + o.threadId;
                        return o.blockedTime;
                    };
            }
            throw new IllegalArgumentException("expected thread type to be either 'cpu', 'wait', or 'block', but was " + type);
        }
    }

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

    public HotThreads type(ReportType type) {
        this.type = type;
        return this;
    }

    public String detect() throws Exception {
        synchronized (mutex) {
            return innerDetect(ManagementFactory.getThreadMXBean(), Thread.currentThread().getId());
        }
    }

    static boolean isKnownJvmThread(ThreadInfo threadInfo) {
        return (knownJvmInternalThreads.stream().anyMatch(jvmThread ->
            threadInfo.getThreadName() != null && threadInfo.getThreadName().equals(jvmThread)));
    }

    static boolean isKnownElasticStackFrame(String className, String methodName) {
        return (knownElasticIdleFrames.stream().anyMatch(pair ->
            pair[0].equals(className) && pair[1].equals(methodName)));
    }

    static boolean isIdleThread(ThreadInfo threadInfo) {
        if (isKnownJvmThread(threadInfo)) {
            return true;
        }

        for (StackTraceElement frame : threadInfo.getStackTrace()) {
            if (isKnownElasticStackFrame(frame.getClassName(), frame.getMethodName())) {
                return true;
            }
        }

        return false;
    }

    Map<Long, ThreadTimeAccumulator> getAllValidThreadInfos(ThreadMXBean threadBean, long currentThreadId) {
        long[] threadIds = threadBean.getAllThreadIds();
        ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds);
        Map<Long, ThreadTimeAccumulator> result = new HashMap<>(threadIds.length);

        for (int i = 0; i < threadIds.length; i++) {
            if (threadInfos[i] == null || threadIds[i] == currentThreadId) {
                continue;
            }
            long cpuTime = threadBean.getThreadCpuTime(threadIds[i]);
            if (cpuTime == -1L) {
                continue;
            }
            result.put(threadIds[i], new ThreadTimeAccumulator(threadInfos[i], cpuTime));
        }

        return result;
    }

    ThreadInfo[][] captureThreadStacks(ThreadMXBean threadBean, List<ThreadTimeAccumulator> topThreads) throws InterruptedException {
        long[] topThreadIds = topThreads.stream().mapToLong(t -> t.threadId).toArray();

        ThreadInfo[][] result = new ThreadInfo[threadElementsSnapshotCount][];
        for (int j = 0; j < threadElementsSnapshotCount; j++) {
            // NOTE, javadoc of getThreadInfo says: If a thread of the given ID is not alive or does not exist,
            // null will be set in the corresponding element in the returned array. A thread is alive if it has
            // been started and has not yet died.
            result[j] = threadBean.getThreadInfo(topThreadIds, Integer.MAX_VALUE);
            Thread.sleep(threadElementsSnapshotDelay.millis());
        }

        return result;
    }

    String innerDetect(ThreadMXBean threadBean, long currentThreadId) throws InterruptedException {
        if (threadBean.isThreadCpuTimeSupported() == false) {
            throw new ElasticsearchException("thread CPU time is not supported on this JDK");
        }

        StringBuilder sb = new StringBuilder()
            .append("Hot threads at ")
            .append(DATE_TIME_FORMATTER.format(LocalDateTime.now(Clock.systemUTC())))
            .append(", interval=")
            .append(interval)
            .append(", busiestThreads=")
            .append(busiestThreads)
            .append(", ignoreIdleThreads=")
            .append(ignoreIdleThreads)
            .append(":\n");

        // Enabling thread contention monitoring is required for capturing JVM thread wait/blocked times
        if (threadBean.isThreadContentionMonitoringSupported()) {
            threadBean.setThreadContentionMonitoringEnabled(true);
        }

        try {
            // Capture before and after thread state with timings
            Map<Long, ThreadTimeAccumulator> previousThreadInfos = getAllValidThreadInfos(threadBean, currentThreadId);
            Thread.sleep(interval.millis());
            Map<Long, ThreadTimeAccumulator> latestThreadInfos = getAllValidThreadInfos(threadBean, currentThreadId);

            latestThreadInfos.forEach((threadId, accumulator) -> accumulator.applyDelta(previousThreadInfos.get(threadId)));

            // Sort by delta CPU time on thread.
            List<ThreadTimeAccumulator> topThreads = new ArrayList<>(latestThreadInfos.values());
            final ToLongFunction<ThreadTimeAccumulator> getter = ThreadTimeAccumulator.valueGetterForReportType(type);

            CollectionUtil.introSort(topThreads, Comparator.comparingLong(getter).reversed());
            topThreads = topThreads.subList(0, Math.min(busiestThreads, topThreads.size()));

            //TODO: Continue refactoring below.

            // analyse N stack traces for the top threads
            ThreadInfo[][] allInfos = captureThreadStacks(threadBean, topThreads);
            for (int t = 0; t < topThreads.size(); t++) {
                long time = getter.applyAsLong(topThreads.get(t));
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
                    percent, TimeValue.timeValueNanos(time), interval, type.getTypeValue(), threadName));
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
        } finally {
            if (threadBean.isThreadContentionMonitoringSupported()) {
                threadBean.setThreadContentionMonitoringEnabled(false);
            }
        }
    }

    int similarity(ThreadInfo threadInfo, ThreadInfo threadInfo0) {
        StackTraceElement[] s1 = threadInfo == null ? EMPTY_STACK : threadInfo.getStackTrace();
        StackTraceElement[] s2 = threadInfo0 == null ? EMPTY_STACK : threadInfo0.getStackTrace();
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
}
