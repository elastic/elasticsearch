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
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.security.AccessController;
import java.security.PrivilegedAction;
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

    private static final StackTraceElement[] EMPTY = new StackTraceElement[0];
    private static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("date_optional_time");
    private static final long INVALID_TIMING = -1L;

    private int busiestThreads = 3;
    private TimeValue interval = new TimeValue(500, TimeUnit.MILLISECONDS);
    private TimeValue threadElementsSnapshotDelay = new TimeValue(10, TimeUnit.MILLISECONDS);
    private int threadElementsSnapshotCount = 10;
    private ReportType type = ReportType.CPU;
    private boolean ignoreIdleThreads = true;

    private static final List<String[]> knownIdleStackFrames = Arrays.asList(
        new String[] {"java.util.concurrent.ThreadPoolExecutor", "getTask"},
        new String[] {"sun.nio.ch.SelectorImpl", "select"},
        new String[] {"org.elasticsearch.threadpool.ThreadPool$CachedTimeThread", "run"},
        new String[] {"org.elasticsearch.indices.ttl.IndicesTTLService$Notifier", "await"},
        new String[] {"java.util.concurrent.LinkedTransferQueue", "poll"},
        new String[] {"com.sun.jmx.remote.internal.ServerCommunicatorAdmin$Timeout", "run"}
    );

    // NOTE: these are JVM dependent and JVM version dependent
    private static final List<String> knownJDKInternalThreads = Arrays.asList(
        "Signal Dispatcher", "Finalizer", "Reference Handler", "Notification Thread", "Common-Cleaner", "process reaper"
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
        if ("cpu".equals(type) || "wait".equals(type) || "block".equals(type) || "mem".equals(type)) {
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

    static boolean isKnownJDKThread(ThreadInfo threadInfo) {
        return (knownJDKInternalThreads.stream().anyMatch(jvmThread ->
            threadInfo.getThreadName() != null && threadInfo.getThreadName().equals(jvmThread)));
    }

    static boolean isKnownIdleStackFrame(String className, String methodName) {
        return (knownIdleStackFrames.stream().anyMatch(pair ->
            pair[0].equals(className) && pair[1].equals(methodName)));
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

    private String innerDetect() throws Exception {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        if (threadBean.isThreadCpuTimeSupported() == false) {
            throw new ElasticsearchException("thread CPU time is not supported on this JDK");
        }

        for (int i = 0; i < threadIds.length; i++) {
            if (threadInfos[i] == null || threadIds[i] == currentThreadId) {
                continue;
            }
            long cpuTime = threadBean.getThreadCpuTime(threadIds[i]);
            if (cpuTime == INVALID_TIMING) {
                continue;
            }
            long threadAllocatedBytes = SunThreadInfo.getThreadAllocatedBytes(threadId);
            if (threadAllocatedBytes == -1) {
                continue;
            }
            ThreadInfo info = threadBean.getThreadInfo(threadId, 0);
            if (info == null) {
                continue;
            }
            threadInfos.put(threadId, new MyThreadInfo(cpu, threadAllocatedBytes, info));
        }
        Thread.sleep(interval.millis());
        for (long threadId : threadBean.getAllThreadIds()) {
            // ignore our own thread...
            if (Thread.currentThread().getId() == threadId) {
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
            long threadAllocatedBytes = SunThreadInfo.getThreadAllocatedBytes(threadId);
            if (threadAllocatedBytes == -1) {
                continue;
            }
            MyThreadInfo data = threadInfos.get(threadId);
            if (data != null) {
                data.setDelta(cpu, threadAllocatedBytes, info);
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
            getter = o -> o.cpuTime;
        } else if ("wait".equals(type)) {
            getter = o -> o.waitedTime;
        } else if ("block".equals(type)) {
            getter = o -> o.blockedTime;
        } else if ("mem".equals(type)) {
            getter = o -> o.threadAllocatedBytes;
        } else {
            throw new IllegalArgumentException("expected thread type to be either 'cpu', 'wait', or 'block', but was " + type);
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
        for (int t = 0; t < busiestThreads; t++) {
            long timeOrBytes = getter.applyAsLong(hotties.get(t));
            String threadName = null;
            for (ThreadInfo[] info : allInfos) {
                if (info != null && info[t] != null) {
                    if (ignoreIdleThreads && isIdleThread(info[t])) {
                        info[t] = null;
                        continue;
                    }
                }
            }
            if (threadName == null) {
                continue; // thread is not alive yet or died before the first snapshot - ignore it!
            }

            if ("mem".equals(type)) {
                double percent = 0;
                MemoryUsage memUsage = memoryMXBean.getHeapMemoryUsage();
                long heapMax = memUsage.getMax() < 0 ? 0 : memUsage.getMax();
                if (heapMax > 0){
                    percent = (((double) timeOrBytes) / heapMax) * 100;
                }
                sb.append(String.format(Locale.ROOT, "%n%4.1f%% (%s out of %s) %s usage by thread '%s'%n",
                    percent, timeOrBytes, heapMax, type, threadName));
            }else {
                double percent = (((double) timeOrBytes) / interval.nanos()) * 100;
                sb.append(String.format(Locale.ROOT, "%n%4.1f%% (%s out of %s) %s usage by thread '%s'%n",
                    percent, TimeValue.timeValueNanos(timeOrBytes), interval, type, threadName));
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
                            for (StackTraceElement frame : show) {
                                sb.append(String.format(Locale.ROOT, "    %s%n", frame));
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
            setThreadWaitBlockTimeMonitoringEnabled(threadBean, false);
        }
    }

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

    static class ThreadTimeAccumulator {
        private final long threadId;

        private long cpuTime;
        private long blockedTime;
        private long waitedTime;

    class MyThreadInfo {
        long cpuTime;
        long blockedCount;
        long blockedTime;
        long waitedCount;
        long waitedTime;
        boolean deltaDone;
        long threadAllocatedBytes;
        ThreadInfo info;

        MyThreadInfo(long cpuTime, long threadAllocatedBytes, ThreadInfo info) {
            blockedCount = info.getBlockedCount();
            blockedTime = info.getBlockedTime();
            waitedCount = info.getWaitedCount();
            waitedTime = info.getWaitedTime();
            this.cpuTime = cpuTime;
            this.info = info;
            this.threadAllocatedBytes= threadAllocatedBytes;
        }

        void setDelta(long cpuTime, long threadAllocatedBytes, ThreadInfo info) {
            if (deltaDone) throw new IllegalStateException("setDelta already called once");
            blockedCount = info.getBlockedCount() - blockedCount;
            blockedTime = info.getBlockedTime() - blockedTime;
            waitedCount = info.getWaitedCount() - waitedCount;
            waitedTime = info.getWaitedTime() - waitedTime;
            this.cpuTime = cpuTime - this.cpuTime;
            this.threadAllocatedBytes = threadAllocatedBytes - this.threadAllocatedBytes;
            deltaDone = true;
            this.info = info;
        }
    }
}
