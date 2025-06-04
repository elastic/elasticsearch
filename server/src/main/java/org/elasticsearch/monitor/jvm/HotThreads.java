/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.jvm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.ChunkedLoggingStream;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
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

    /**
     * Capture and log the hot threads on the local node. Useful for capturing stack traces for unexpectedly-slow operations in production.
     * The resulting log message may be large, and contains significant whitespace, so it is compressed and base64-encoded using {@link
     * ChunkedLoggingStream}.
     *
     * @param logger        The logger to use for the logging
     * @param level         The log level to use for the logging.
     * @param prefix        The prefix to emit on each chunk of the logging.
     * @param referenceDocs A link to the docs describing how to decode the logging.
     */
    public static void logLocalHotThreads(Logger logger, Level level, String prefix, ReferenceDocs referenceDocs) {
        if (logger.isEnabled(level) == false) {
            return;
        }

        try (
            var stream = ChunkedLoggingStream.create(logger, level, prefix, referenceDocs);
            var writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8)
        ) {
            new HotThreads().busiestThreads(500).ignoreIdleThreads(false).detect(writer);
        } catch (Exception e) {
            logger.error(() -> org.elasticsearch.common.Strings.format("failed to write local hot threads with prefix [%s]", prefix), e);
        }
    }

    /**
     * Capture and log the current threads on the local node. Unlike hot threads this does not sample and captures current state only.
     * Useful for capturing stack traces for unexpectedly-slow operations in production. The resulting message might be large, so it is
     * split per thread and logged as multiple entries.
     *
     * @param logger        The logger to use for the logging
     * @param level         The log level to use for the logging.
     * @param prefix        The prefix to emit on each chunk of the logging.
     */
    public static void logLocalCurrentThreads(Logger logger, Level level, String prefix) {
        if (logger.isEnabled(level) == false) {
            return;
        }

        try (var writer = new StringWriter()) {
            new HotThreads().busiestThreads(500).threadElementsSnapshotCount(1).detect(writer, () -> {
                logger.log(level, "{}: {}", prefix, writer.toString());
                writer.getBuffer().setLength(0);
            });
        } catch (Exception e) {
            logger.error(
                () -> org.elasticsearch.common.Strings.format("failed to write local current threads with prefix [%s]", prefix),
                e
            );
        }
    }

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

    public void detect(Writer writer) throws Exception {
        detect(writer, () -> {});
    }

    public void detect(Writer writer, Runnable onNextThread) throws Exception {
        synchronized (mutex) {
            innerDetect(ManagementFactory.getThreadMXBean(), SunThreadInfo.INSTANCE, Thread.currentThread().getId(), writer, onNextThread);
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

        // NOTE, javadoc of getThreadInfo says: If a thread of the given ID is not alive or does not exist,
        // null will be set in the corresponding element in the returned array. A thread is alive if it has
        // been started and has not yet died.
        for (int j = 0; j < threadElementsSnapshotCount - 1; j++) {
            result[j] = threadBean.getThreadInfo(threadIds, Integer.MAX_VALUE);
            Thread.sleep(threadElementsSnapshotDelay.millis());
        }
        result[threadElementsSnapshotCount - 1] = threadBean.getThreadInfo(threadIds, Integer.MAX_VALUE);

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

    void innerDetect(ThreadMXBean threadBean, SunThreadInfo sunThreadInfo, long currentThreadId, Writer writer, Runnable onNextThread)
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

        writer.append("Hot threads at ")
            .append(DATE_TIME_FORMATTER.format(LocalDateTime.now(Clock.systemUTC())))
            .append(", interval=")
            .append(interval.toString())
            .append(", busiestThreads=")
            .append(Integer.toString(busiestThreads))
            .append(", ignoreIdleThreads=")
            .append(Boolean.toString(ignoreIdleThreads))
            .append(":\n");
        onNextThread.run();

        // Capture before and after thread state with timings
        Map<Long, ThreadTimeAccumulator> previousThreadInfos = getAllValidThreadInfos(threadBean, sunThreadInfo, currentThreadId);
        Thread.sleep(interval.millis());
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
                case MEM -> writer.append(
                    Strings.format(
                        "%n%s memory allocated by thread '%s'%n",
                        ByteSizeValue.ofBytes(topThread.getAllocatedBytes()),
                        threadName
                    )
                );
                case CPU -> {
                    double percentCpu = getTimeSharePercentage(topThread.getCpuTime());
                    double percentOther = Transports.isTransportThread(threadName) && topThread.getCpuTime() == 0L
                        ? 100.0
                        : getTimeSharePercentage(topThread.getOtherTime());
                    double percentTotal = (Transports.isTransportThread(threadName)) ? percentCpu : percentOther + percentCpu;
                    String otherLabel = (Transports.isTransportThread(threadName)) ? "idle" : "other";
                    writer.append(
                        Strings.format(
                            "%n%4.1f%% [cpu=%1.1f%%, %s=%1.1f%%] (%s out of %s) %s usage by thread '%s'%n",
                            percentTotal,
                            percentCpu,
                            otherLabel,
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
                    writer.append(
                        Strings.format(
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
                        writer.append(Strings.format("  unique snapshot%n"));
                        for (StackTraceElement frame : show) {
                            writer.append(Strings.format("    %s%n", frame));
                        }
                    } else {
                        writer.append(
                            Strings.format("  %d/%d snapshots sharing following %d elements%n", count, threadElementsSnapshotCount, maxSim)
                        );
                        for (int l = show.length - maxSim; l < show.length; l++) {
                            writer.append(Strings.format("    %s%n", show[l]));
                        }
                    }
                }
            }
            onNextThread.run();
        }
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
        private final String threadName;
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
            this.threadName = info.getThreadName();
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
            } else if (Transports.isTransportThread(threadName)) {
                return getCpuTime();
            }
            return Math.max(interval.nanos() - getWaitedTime() - getBlockedTime(), 0);
        }

        public long getOtherTime() {
            // If the thread didn't have any CPU movement, we can't really tell if it's
            // not running, or it has been asleep forever.
            if (getCpuTime() == 0) {
                return 0;
            }

            return Math.max(interval.nanos() - getWaitedTime() - getBlockedTime() - getCpuTime(), 0);
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

    public record RequestOptions(
        int threads,
        HotThreads.ReportType reportType,
        HotThreads.SortOrder sortOrder,
        TimeValue interval,
        int snapshots,
        boolean ignoreIdleThreads
    ) implements Writeable {

        public static RequestOptions readFrom(StreamInput in) throws IOException {
            var threads = in.readInt();
            var ignoreIdleThreads = in.readBoolean();
            var reportType = HotThreads.ReportType.of(in.readString());
            var interval = in.readTimeValue();
            var snapshots = in.readInt();
            var sortOrder = HotThreads.SortOrder.of(in.readString());
            return new RequestOptions(threads, reportType, sortOrder, interval, snapshots, ignoreIdleThreads);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(threads);
            out.writeBoolean(ignoreIdleThreads);
            out.writeString(reportType.getTypeValue());
            out.writeTimeValue(interval);
            out.writeInt(snapshots);
            out.writeString(sortOrder.getOrderValue());
        }

        public static final RequestOptions DEFAULT = new RequestOptions(
            3,
            ReportType.CPU,
            SortOrder.TOTAL,
            TimeValue.timeValueMillis(500),
            10,
            true
        );
    }
}
