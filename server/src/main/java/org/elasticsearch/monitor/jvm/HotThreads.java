/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

public class HotThreads {

    private static final Object mutex = new Object();

    private static final StackTraceElement[] EMPTY = new StackTraceElement[0];
    private static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("date_optional_time");
    private static final long INVALID_TIMING = -1L;

    private int busiestThreads = 3;
    private TimeValue interval = new TimeValue(500, TimeUnit.MILLISECONDS);
    private TimeValue threadElementsSnapshotDelay = new TimeValue(10, TimeUnit.MILLISECONDS);
    private int threadElementsSnapshotCount = 10;
    private ReportType type = ReportType.CPU;
    private SortOrder sortOrder = SortOrder.TOTAL;

    private boolean ignoreIdleThreads = true;

    private static final List<String[]> knownIdleStackFrames = Arrays.asList(
        new String[] { "java.util.concurrent.ThreadPoolExecutor", "getTask" },
        new String[] { "sun.nio.ch.SelectorImpl", "select" },
        new String[] { "org.elasticsearch.threadpool.ThreadPool$CachedTimeThread", "run" },
        new String[] { "org.elasticsearch.indices.ttl.IndicesTTLService$Notifier", "await" },
        new String[] { "java.util.concurrent.LinkedTransferQueue", "poll" },
        new String[] { "com.sun.jmx.remote.internal.ServerCommunicatorAdmin$Timeout", "run" }
    );

    // NOTE: these are JVM dependent and JVM version dependent
    private static final List<String> knownJDKInternalThreads = Arrays.asList(
        "Signal Dispatcher",
        "Finalizer",
        "Reference Handler",
        "Notification Thread",
        "Common-Cleaner",
        "process reaper",
        "DestroyJavaVM"
    );

    public enum ReportType {

        CPU("cpu"),
        WAIT("wait"),
        BLOCK("block"),
        MEM("mem");

        private final String type;

        ReportType(String type) {
            this.type = type;
        }

        public String getTypeValue() {
            return type;
        }

        // Custom enum from string because of legacy support. The standard Enum.valueOf is static
        // and cannot be overriden to show a nice error message in case the enum value isn't found
        public static ReportType of(String type) {
            for (var report : values()) {
                if (report.type.equals(type)) {
                    return report;
                }
            }
            throw new IllegalArgumentException("type not supported [" + type + "]");
        }
    }

    public enum SortOrder {

        TOTAL("total"),
        CPU("cpu");

        private final String order;

        SortOrder(String order) {
            this.order = order;
        }

        public String getOrderValue() {
            return order;
        }

        // Custom enum from string because of legacy support. The standard Enum.valueOf is static
        // and cannot be overriden to show a nice error message in case the enum value isn't found
        public static SortOrder of(String order) {
            for (var sortOrder : values()) {
                if (sortOrder.order.equals(order)) {
                    return sortOrder;
                }
            }
            throw new IllegalArgumentException("sort order not supported [" + order + "]");
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

    public HotThreads sortOrder(SortOrder order) {
        this.sortOrder = order;
        return this;
    }

    public String detect() throws Exception {
        synchronized (mutex) {
            return innerDetect(ManagementFactory.getThreadMXBean(), SunThreadInfo.INSTANCE, Thread.currentThread().getId(), (interval) -> {
                Thread.sleep(interval);
                return null;
            });
        }
    }

    static boolean isKnownJDKThread(ThreadInfo threadInfo) {
        return (knownJDKInternalThreads.stream()
            .anyMatch(jvmThread -> threadInfo.getThreadName() != null && threadInfo.getThreadName().equals(jvmThread)));
    }

    static boolean isKnownIdleStackFrame(String className, String methodName) {
        return (knownIdleStackFrames.stream().anyMatch(pair -> pair[0].equals(className) && pair[1].equals(methodName)));
    }

    static boolean isIdleThread(ThreadInfo threadInfo) {
        if (isKnownJDKThread(threadInfo)) {
            return true;
        }

        for (StackTraceElement frame : threadInfo.getStackTrace()) {
            if (isKnownIdleStackFrame(frame.getClassName(), frame.getMethodName())) {
                return true;
            }
        }

        return false;
    }

    Map<Long, ThreadTimeAccumulator> getAllValidThreadInfos(ThreadMXBean threadBean, SunThreadInfo sunThreadInfo, long currentThreadId) {
        long[] threadIds = threadBean.getAllThreadIds();
        ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds);
        Map<Long, ThreadTimeAccumulator> result = Maps.newMapWithExpectedSize(threadIds.length);

        for (int i = 0; i < threadIds.length; i++) {
            if (threadInfos[i] == null || threadIds[i] == currentThreadId) {
                continue;
            }
            long cpuTime = threadBean.getThreadCpuTime(threadIds[i]);
            if (cpuTime == INVALID_TIMING) {
                continue;
            }
            long allocatedBytes = type == ReportType.MEM ? sunThreadInfo.getThreadAllocatedBytes(threadIds[i]) : 0;
            result.put(threadIds[i], new ThreadTimeAccumulator(threadInfos[i], interval, cpuTime, allocatedBytes));
        }

        return result;
    }

    ThreadInfo[][] captureThreadStacks(ThreadMXBean threadBean, long[] threadIds) throws InterruptedException {
        ThreadInfo[][] result = new ThreadInfo[threadElementsSnapshotCount][];
        for (int j = 0; j < threadElementsSnapshotCount; j++) {
            // NOTE, javadoc of getThreadInfo says: If a thread of the given ID is not alive or does not exist,
            // null will be set in the corresponding element in the returned array. A thread is alive if it has
            // been started and has not yet died.
            result[j] = threadBean.getThreadInfo(threadIds, Integer.MAX_VALUE);
            Thread.sleep(threadElementsSnapshotDelay.millis());
        }

        return result;
    }

    private static boolean isThreadWaitBlockTimeMonitoringEnabled(ThreadMXBean threadBean) {
        if (threadBean.isThreadContentionMonitoringSupported()) {
            return threadBean.isThreadContentionMonitoringEnabled();
        }
        return false;
    }

    private double getTimeSharePercentage(long time) {
        return (((double) time) / interval.nanos()) * 100;
    }

    String innerDetect(ThreadMXBean threadBean, SunThreadInfo sunThreadInfo, long currentThreadId, SleepFunction<Long, Void> threadSleep)
        throws Exception {
        if (threadBean.isThreadCpuTimeSupported() == false) {
            throw new ElasticsearchException("thread CPU time is not supported on this JDK");
        }

        if (type == ReportType.MEM && sunThreadInfo.isThreadAllocatedMemorySupported() == false) {
            throw new ElasticsearchException("thread allocated memory is not supported on this JDK");
        }

        // Enabling thread contention monitoring is required for capturing JVM thread wait/blocked times. If we weren't
        // able to enable this functionality during bootstrap, we should not produce HotThreads reports.
        if (isThreadWaitBlockTimeMonitoringEnabled(threadBean) == false) {
            throw new ElasticsearchException("thread wait/blocked time accounting is not supported on this JDK");
        }

        StringBuilder sb = new StringBuilder().append("Hot threads at ")
            .append(DATE_TIME_FORMATTER.format(LocalDateTime.now(Clock.systemUTC())))
            .append(", interval=")
            .append(interval)
            .append(", busiestThreads=")
            .append(busiestThreads)
            .append(", ignoreIdleThreads=")
            .append(ignoreIdleThreads)
            .append(":\n");

        // Capture before and after thread state with timings
        Map<Long, ThreadTimeAccumulator> previousThreadInfos = getAllValidThreadInfos(threadBean, sunThreadInfo, currentThreadId);
        threadSleep.apply(interval.millis());
        Map<Long, ThreadTimeAccumulator> latestThreadInfos = getAllValidThreadInfos(threadBean, sunThreadInfo, currentThreadId);

        latestThreadInfos.forEach((threadId, accumulator) -> accumulator.subtractPrevious(previousThreadInfos.get(threadId)));

        // Sort by delta CPU time on thread.
        List<ThreadTimeAccumulator> topThreads = new ArrayList<>(latestThreadInfos.values());

        // Special comparator for CPU mode with TOTAL sort type only. Otherwise, we simply use the value.
        if (type == ReportType.CPU && sortOrder == SortOrder.TOTAL) {
            CollectionUtil.introSort(
                topThreads,
                Comparator.comparingLong(ThreadTimeAccumulator::getRunnableTime)
                    .thenComparingLong(ThreadTimeAccumulator::getCpuTime)
                    .reversed()
            );
        } else {
            CollectionUtil.introSort(topThreads, Comparator.comparingLong(ThreadTimeAccumulator.valueGetterForReportType(type)).reversed());
        }

        topThreads = topThreads.subList(0, Math.min(busiestThreads, topThreads.size()));
        long[] topThreadIds = topThreads.stream().mapToLong(t -> t.threadId).toArray();

        // analyse N stack traces for the top threads
        ThreadInfo[][] allInfos = captureThreadStacks(threadBean, topThreadIds);

        for (int t = 0; t < topThreads.size(); t++) {
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

            ThreadTimeAccumulator topThread = topThreads.get(t);

            switch (type) {
                case MEM -> sb.append(
                    String.format(
                        Locale.ROOT,
                        "%n%s memory allocated by thread '%s'%n",
                        new ByteSizeValue(topThread.getAllocatedBytes()),
                        threadName
                    )
                );
                case CPU -> {
                    double percentCpu = getTimeSharePercentage(topThread.getCpuTime());
                    double percentOther = getTimeSharePercentage(topThread.getOtherTime());
                    sb.append(
                        String.format(
                            Locale.ROOT,
                            "%n%4.1f%% [cpu=%1.1f%%, other=%1.1f%%] (%s out of %s) %s usage by thread '%s'%n",
                            percentOther + percentCpu,
                            percentCpu,
                            percentOther,
                            TimeValue.timeValueNanos(topThread.getCpuTime() + topThread.getOtherTime()),
                            interval,
                            type.getTypeValue(),
                            threadName
                        )
                    );
                }
                default -> {
                    long time = ThreadTimeAccumulator.valueGetterForReportType(type).applyAsLong(topThread);
                    double percent = getTimeSharePercentage(time);
                    sb.append(
                        String.format(
                            Locale.ROOT,
                            "%n%4.1f%% (%s out of %s) %s usage by thread '%s'%n",
                            percent,
                            TimeValue.timeValueNanos(time),
                            interval,
                            type.getTypeValue(),
                            threadName
                        )
                    );
                }
            }

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
                        for (StackTraceElement frame : show) {
                            sb.append(String.format(Locale.ROOT, "    %s%n", frame));
                        }
                    } else {
                        sb.append(
                            String.format(
                                Locale.ROOT,
                                "  %d/%d snapshots sharing following %d elements%n",
                                count,
                                threadElementsSnapshotCount,
                                maxSim
                            )
                        );
                        for (int l = show.length - maxSim; l < show.length; l++) {
                            sb.append(String.format(Locale.ROOT, "    %s%n", show[l]));
                        }
                    }
                }
            }
        }

        return sb.toString();
    }

    static int similarity(ThreadInfo threadInfo, ThreadInfo threadInfo0) {
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

    static class ThreadTimeAccumulator {
        private final long threadId;
        private final TimeValue interval;

        private long cpuTime;
        private long blockedTime;
        private long waitedTime;
        private long allocatedBytes;

        ThreadTimeAccumulator(ThreadInfo info, TimeValue interval, long cpuTime, long allocatedBytes) {
            this.blockedTime = millisecondsToNanos(info.getBlockedTime()); // Convert to nanos to standardize
            this.waitedTime = millisecondsToNanos(info.getWaitedTime()); // Convert to nanos to standardize
            this.cpuTime = cpuTime;
            this.allocatedBytes = allocatedBytes;
            this.threadId = info.getThreadId();
            this.interval = interval;
        }

        private static long millisecondsToNanos(long millis) {
            return millis * 1_000_000;
        }

        void subtractPrevious(ThreadTimeAccumulator previous) {
            // Previous can be null for threads that had started while we were sleeping.
            // In that case the absolute thread timings are the delta.
            if (previous != null) {
                if (previous.getThreadId() != getThreadId()) {
                    throw new IllegalStateException("Thread timing accumulation must be done on the same thread");
                }
                this.blockedTime -= previous.blockedTime;
                this.waitedTime -= previous.waitedTime;
                this.cpuTime -= previous.cpuTime;
                this.allocatedBytes -= previous.allocatedBytes;
            }
        }

        // A thread can migrate a CPU while executing, in which case, on certain processors, the
        // reported timings will be bogus (and likely negative). We cap the reported timings to >= 0 values only.
        public long getCpuTime() {
            return Math.max(cpuTime, 0);
        }

        public long getRunnableTime() {
            // If the thread didn't have any CPU movement, we can't really tell if it's
            // not running, or it has been asleep forever.
            if (getCpuTime() == 0) {
                return 0;
            }
            return Math.max(interval.nanos() - getWaitedTime() - getBlockedTime(), 0);
        }

        public long getOtherTime() {
            // If the thread didn't have any CPU movement, we can't really tell if it's
            // not running, or it has been asleep forever.
            if (getCpuTime() == 0) {
                return 0;
            }

            return Math.max(getRunnableTime() - getCpuTime(), 0);
        }

        public long getBlockedTime() {
            return Math.max(blockedTime, 0);
        }

        public long getWaitedTime() {
            return Math.max(waitedTime, 0);
        }

        public long getAllocatedBytes() {
            return Math.max(allocatedBytes, 0);
        }

        public long getThreadId() {
            return threadId;
        }

        static ToLongFunction<ThreadTimeAccumulator> valueGetterForReportType(ReportType type) {
            return switch (type) {
                case CPU -> ThreadTimeAccumulator::getCpuTime;
                case WAIT -> ThreadTimeAccumulator::getWaitedTime;
                case BLOCK -> ThreadTimeAccumulator::getBlockedTime;
                case MEM -> ThreadTimeAccumulator::getAllocatedBytes;
            };
        }
    }

    @FunctionalInterface
    public interface SleepFunction<T, R> {
        R apply(T t) throws InterruptedException;
    }

    public static void initializeRuntimeMonitoring() {
        // We'll let the JVM boot without this functionality, however certain APIs like HotThreads will not
        // function and report an error.
        if (ManagementFactory.getThreadMXBean().isThreadContentionMonitoringSupported() == false) {
            LogManager.getLogger(HotThreads.class).info("Thread wait/blocked time accounting not supported.");
        } else {
            try {
                ManagementFactory.getThreadMXBean().setThreadContentionMonitoringEnabled(true);
            } catch (UnsupportedOperationException monitoringUnavailable) {
                LogManager.getLogger(HotThreads.class).warn("Thread wait/blocked time accounting cannot be enabled.");
            }
        }
    }
}
