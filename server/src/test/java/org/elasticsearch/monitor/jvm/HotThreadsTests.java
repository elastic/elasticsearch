/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Matchers;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HotThreadsTests extends ESTestCase {

    public void testSupportedThreadsReportType() {
        for (String type: new String[] {"unsupported", "", null, "CPU", "WAIT", "BLOCK" }) {
            expectThrows(IllegalArgumentException.class, () -> new HotThreads().type(HotThreads.ReportType.of(type)));
        }

        for (String type : new String[] { "cpu", "wait", "block" }) {
            try {
                new HotThreads().type(HotThreads.ReportType.of(type));
            } catch (IllegalArgumentException e) {
                fail(String.format(Locale.ROOT, "IllegalArgumentException called when creating HotThreads for supported type [%s]", type));
            }
        }
    }

    private void assertIdleThreadHelper(ThreadInfo threadInfo, List<StackTraceElement> stack) {
        when(threadInfo.getStackTrace()).thenReturn(stack.toArray(new StackTraceElement[0]));
        assertTrue(HotThreads.isIdleThread(threadInfo));
    }

    private List<StackTraceElement> makeThreadStackHelper(List<String[]> names) {
        return names.stream().map(e -> {
            // Cannot mock StackTraceElement because it's final
            return new StackTraceElement(e[0], e[1], "Some_File", 1);
        }).collect(Collectors.toList());
    }

    public void testIdleThreadsDetection() {
        for (String threadName : new String[] {
            "Signal Dispatcher", "Finalizer", "Reference Handler", "Notification Thread", "Common-Cleaner" }) {
            ThreadInfo mockedThreadInfo = mock(ThreadInfo.class);
            when(mockedThreadInfo.getThreadName()).thenReturn(threadName);
            assertTrue(HotThreads.isIdleThread(mockedThreadInfo));
            assertTrue(HotThreads.isKnownJvmThread(mockedThreadInfo));
        }

        for (String threadName : new String[] { "Text", "", null, "Finalizer".toLowerCase(Locale.ROOT) }) {
            ThreadInfo mockedThreadInfo = mock(ThreadInfo.class);
            when(mockedThreadInfo.getThreadName()).thenReturn(threadName);
            when(mockedThreadInfo.getStackTrace()).thenReturn(new StackTraceElement[0]);
            assertFalse(HotThreads.isIdleThread(mockedThreadInfo));
            assertFalse(HotThreads.isKnownJvmThread(mockedThreadInfo));
        }

        List<StackTraceElement> testJvmStack = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.test", "methodOne"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"},
                new String[]{"org.elasticsearch.monitor.test", "methodThree"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodFour"}
            ));

        for (StackTraceElement stackFrame : testJvmStack) {
            assertFalse(HotThreads.isKnownElasticStackFrame(stackFrame.getClassName(), stackFrame.getMethodName()));
        }

        ThreadInfo notIdleThread = mock(ThreadInfo.class);
        when(notIdleThread.getThreadName()).thenReturn("Not Idle Thread");
        when(notIdleThread.getStackTrace()).thenReturn(testJvmStack.toArray(new StackTraceElement[0]));

        assertFalse(HotThreads.isIdleThread(notIdleThread));

        List<StackTraceElement> idleThreadStackElements = makeThreadStackHelper(
            List.of(
                new String[]{"java.util.concurrent.ThreadPoolExecutor", "getTask"},
                new String[]{"sun.nio.ch.SelectorImpl", "select"},
                new String[]{"org.elasticsearch.threadpool.ThreadPool$CachedTimeThread", "run"},
                new String[]{"org.elasticsearch.indices.ttl.IndicesTTLService$Notifier", "await"},
                new String[]{"java.util.concurrent.LinkedTransferQueue", "poll"}
            ));

        for (StackTraceElement extraFrame : idleThreadStackElements) {
            ThreadInfo idleThread = mock(ThreadInfo.class);
            when(idleThread.getThreadName()).thenReturn("Idle Thread");
            when(idleThread.getStackTrace()).thenReturn(new StackTraceElement[] {extraFrame});
            assertTrue(HotThreads.isIdleThread(idleThread));
            assertTrue(HotThreads.isKnownElasticStackFrame(extraFrame.getClassName(), extraFrame.getMethodName()));

            List<StackTraceElement> topOfStack = new ArrayList<>(testJvmStack);
            topOfStack.add(0, extraFrame);
            assertIdleThreadHelper(idleThread, topOfStack);

            List<StackTraceElement> bottomOfStack = new ArrayList<>(testJvmStack);
            bottomOfStack.add(extraFrame);
            assertIdleThreadHelper(idleThread, bottomOfStack);

            if (testJvmStack.size() > 1) {
                List<StackTraceElement> middleOfStack = new ArrayList<>(testJvmStack);
                middleOfStack.add(between(Math.min(1, testJvmStack.size()), Math.max(0, testJvmStack.size() - 1)), extraFrame);
                assertIdleThreadHelper(idleThread, middleOfStack);
            }
        }
    }

    public void testSimilarity() {
        StackTraceElement[] stackOne = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.test", "methodOne"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"}
            )).toArray(new StackTraceElement[0]);

        StackTraceElement[] stackTwo = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.test1", "methodOne"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"}
            )).toArray(new StackTraceElement[0]);

        StackTraceElement[] stackThree = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"},
                new String[]{"org.elasticsearch.monitor.test", "methodOne"}
            )).toArray(new StackTraceElement[0]);

        StackTraceElement[] stackFour = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.testPrior", "methodOther"},
                new String[]{"org.elasticsearch.monitor.test", "methodOne"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"}
            )).toArray(new StackTraceElement[0]);

        // We can simplify this with records when the toolchain is upgraded
        class SimilarityTestCase {
            final StackTraceElement[] one;
            final StackTraceElement[] two;
            final int similarityScore;

            SimilarityTestCase(StackTraceElement[] one, StackTraceElement[] two, int similarityScore) {
                this.one = one;
                this.two = two;
                this.similarityScore = similarityScore;
            }
        }

        SimilarityTestCase[] testCases = new SimilarityTestCase[] {
            new SimilarityTestCase(stackOne, stackOne, 2),
            new SimilarityTestCase(stackOne, stackTwo, 1),
            new SimilarityTestCase(stackOne, stackThree, 0),
            new SimilarityTestCase(stackOne, stackFour, 2),
            new SimilarityTestCase(stackTwo, stackFour, 1),
            new SimilarityTestCase(stackOne, new StackTraceElement[0], 0),
        };

        for (SimilarityTestCase testCase : testCases) {
            ThreadInfo threadOne = mock(ThreadInfo.class);
            when(threadOne.getThreadName()).thenReturn("Thread One");
            when(threadOne.getStackTrace()).thenReturn(testCase.one);

            ThreadInfo threadTwo = mock(ThreadInfo.class);
            when(threadTwo.getThreadName()).thenReturn("Thread Two");
            when(threadTwo.getStackTrace()).thenReturn(testCase.two);

            assertEquals(testCase.similarityScore, HotThreads.similarity(threadOne, threadTwo));
        }

        ThreadInfo threadOne = mock(ThreadInfo.class);
        when(threadOne.getThreadName()).thenReturn("Thread One");
        when(threadOne.getStackTrace()).thenReturn(testCases[0].one);

        assertEquals(0, HotThreads.similarity(threadOne, null));

        assertEquals(0, HotThreads.getStackTrace(null).length);
        assertEquals(2, HotThreads.getStackTrace(threadOne).length);
    }

    // We call this helper for each different mode to reset the before and after timings.
    private List<ThreadInfo> makeThreadInfoMocksHelper(ThreadMXBean mockedMXBean, long[] threadIds) {
        List<ThreadInfo> allInfos = new ArrayList<>(threadIds.length);

        for (long threadId : threadIds) {
            // We first return 0 for all timings, then a true value to create the reporting deltas.
            when(mockedMXBean.getThreadCpuTime(threadId)).thenReturn(0L).thenReturn(threadId);
            ThreadInfo mockedThreadInfo = mock(ThreadInfo.class);
            when(mockedMXBean.getThreadInfo(eq(threadId), anyInt())).thenReturn(mockedThreadInfo);
            when(mockedThreadInfo.getThreadName()).thenReturn(String.format(Locale.ROOT, "Thread %d", threadId));

            // We create some variability for the blocked and waited times. Odd and even.
            when(mockedThreadInfo.getBlockedCount()).thenReturn(0L).thenReturn(threadId % 2);
            long blockedTime = ((threadId % 2) == 0) ? 0L : threadId;
            when(mockedThreadInfo.getBlockedTime()).thenReturn(0L).thenReturn(blockedTime);

            when(mockedThreadInfo.getWaitedCount()).thenReturn(0L).thenReturn((threadId + 1) % 2);
            long waitTime = (((threadId + 1) % 2) == 0) ? 0L : threadId;
            when(mockedThreadInfo.getWaitedTime()).thenReturn(0L).thenReturn(waitTime);

            when(mockedThreadInfo.getThreadId()).thenReturn(threadId);

            StackTraceElement[] stack = makeThreadStackHelper(
                List.of(
                    new String[]{"org.elasticsearch.monitor.test", String.format(Locale.ROOT, "method_%d", (threadId) % 2)},
                    new String[]{"org.elasticsearch.monitor.testOther", "methodFinal"}
                )).toArray(new StackTraceElement[0]);
            when(mockedThreadInfo.getStackTrace()).thenReturn(stack);

            allInfos.add(mockedThreadInfo);
        }

        when(mockedMXBean.getThreadInfo(Matchers.any(), anyInt())).thenReturn(allInfos.toArray(new ThreadInfo[0]));
        when(mockedMXBean.getThreadInfo(Matchers.any(long[].class))).thenReturn(allInfos.toArray(new ThreadInfo[0]));

        return allInfos;
    }

    public void testInnerDetect() throws Exception {
        ThreadMXBean mockedMXBean = mock(ThreadMXBean.class);
        when(mockedMXBean.isThreadCpuTimeSupported()).thenReturn(true);

        long[] threadIds = new long[] { 1, 2, 3, 4 }; // Adds up to 10, the intervalNanos for calculating time percentages
        long mockCurrentThreadId = 0L;
        when(mockedMXBean.getAllThreadIds()).thenReturn(threadIds);

        List<ThreadInfo> allInfos = makeThreadInfoMocksHelper(mockedMXBean, threadIds);
        List<ThreadInfo> cpuOrderedInfos = List.of(allInfos.get(3), allInfos.get(2), allInfos.get(1), allInfos.get(0));
        when(mockedMXBean.getThreadInfo(Matchers.any(), anyInt())).thenReturn(cpuOrderedInfos.toArray(new ThreadInfo[0]));

        HotThreads hotThreads = new HotThreads()
            .busiestThreads(4)
            .type(HotThreads.ReportType.CPU)
            .interval(TimeValue.timeValueNanos(10))
            .threadElementsSnapshotCount(11)
            .ignoreIdleThreads(false);

        String innerResult = hotThreads.innerDetect(mockedMXBean, mockCurrentThreadId);

        assertThat(innerResult, containsString("Hot threads at "));
        assertThat(innerResult, containsString("interval=10nanos, busiestThreads=4, ignoreIdleThreads=false:"));
        assertThat(innerResult, containsString("11/11 snapshots sharing following 2 elements"));
        assertEquals(5, innerResult.split(" 11/11 snapshots sharing following 2 elements").length);
        assertThat(innerResult, containsString("40.0% (4nanos out of 10nanos) cpu usage by thread 'Thread 4'"));
        assertThat(innerResult, containsString("30.0% (3nanos out of 10nanos) cpu usage by thread 'Thread 3'"));
        assertThat(innerResult, containsString("20.0% (2nanos out of 10nanos) cpu usage by thread 'Thread 2'"));
        assertThat(innerResult, containsString("10.0% (1nanos out of 10nanos) cpu usage by thread 'Thread 1'"));
        assertThat(innerResult, containsString("org.elasticsearch.monitor.test.method_0(Some_File:1)"));
        assertThat(innerResult, containsString("org.elasticsearch.monitor.test.method_1(Some_File:1)"));
        assertThat(innerResult, containsString("org.elasticsearch.monitor.testOther.methodFinal(Some_File:1)"));

        // Let's ask again without progressing the CPU thread counters, e.g. resetting the mocks
        innerResult = hotThreads.innerDetect(mockedMXBean, mockCurrentThreadId);

        assertThat(innerResult, containsString("0.0% (0s out of 10nanos) cpu usage by thread 'Thread 4'"));
        assertThat(innerResult, containsString("0.0% (0s out of 10nanos) cpu usage by thread 'Thread 3'"));
        assertThat(innerResult, containsString("0.0% (0s out of 10nanos) cpu usage by thread 'Thread 2'"));
        assertThat(innerResult, containsString("0.0% (0s out of 10nanos) cpu usage by thread 'Thread 1'"));

        HotThreads hotWaitingThreads = new HotThreads()
            .busiestThreads(4)
            .type(HotThreads.ReportType.WAIT)
            .interval(TimeValue.timeValueNanos(10))
            .threadElementsSnapshotCount(11)
            .ignoreIdleThreads(false);

        allInfos = makeThreadInfoMocksHelper(mockedMXBean, threadIds);
        List<ThreadInfo> waitOrderedInfos = List.of(allInfos.get(3), allInfos.get(1), allInfos.get(0), allInfos.get(2));
        when(mockedMXBean.getThreadInfo(Matchers.any(), anyInt())).thenReturn(waitOrderedInfos.toArray(new ThreadInfo[0]));

        String waitInnerResult = hotWaitingThreads.innerDetect(mockedMXBean, mockCurrentThreadId);

        assertThat(waitInnerResult, containsString("40.0% (4nanos out of 10nanos) wait usage by thread 'Thread 4'"));
        assertThat(waitInnerResult, containsString("20.0% (2nanos out of 10nanos) wait usage by thread 'Thread 2'"));
        assertThat(waitInnerResult, containsString("0.0% (0s out of 10nanos) wait usage by thread 'Thread 1'"));
        assertThat(waitInnerResult, containsString("0.0% (0s out of 10nanos) wait usage by thread 'Thread 3'"));

        HotThreads hotBlockedThreads = new HotThreads()
            .busiestThreads(4)
            .type(HotThreads.ReportType.BLOCK)
            .interval(TimeValue.timeValueNanos(10))
            .threadElementsSnapshotCount(11)
            .ignoreIdleThreads(false);

        allInfos = makeThreadInfoMocksHelper(mockedMXBean, threadIds);
        List<ThreadInfo> blockOrderedInfos = List.of(allInfos.get(2), allInfos.get(0), allInfos.get(1), allInfos.get(3));
        when(mockedMXBean.getThreadInfo(Matchers.any(), anyInt())).thenReturn(blockOrderedInfos.toArray(new ThreadInfo[0]));

        String blockInnerResult = hotBlockedThreads.innerDetect(mockedMXBean, mockCurrentThreadId);

        assertThat(blockInnerResult, containsString("30.0% (3nanos out of 10nanos) block usage by thread 'Thread 3'"));
        assertThat(blockInnerResult, containsString("10.0% (1nanos out of 10nanos) block usage by thread 'Thread 1'"));
        assertThat(blockInnerResult, containsString("0.0% (0s out of 10nanos) block usage by thread 'Thread 2'"));
        assertThat(blockInnerResult, containsString("0.0% (0s out of 10nanos) block usage by thread 'Thread 4'"));

        // Test with only one stack to trigger the different print in innerDetect

        allInfos = makeThreadInfoMocksHelper(mockedMXBean, threadIds);
        cpuOrderedInfos = List.of(allInfos.get(3), allInfos.get(2), allInfos.get(1), allInfos.get(0));
        when(mockedMXBean.getThreadInfo(Matchers.any(), anyInt())).thenReturn(cpuOrderedInfos.toArray(new ThreadInfo[0]));

        hotThreads = new HotThreads()
            .busiestThreads(4)
            .type(HotThreads.ReportType.CPU)
            .interval(TimeValue.timeValueNanos(10))
            .threadElementsSnapshotCount(1)
            .ignoreIdleThreads(false);

        String singleResult = hotThreads.innerDetect(mockedMXBean, mockCurrentThreadId);

        assertThat(singleResult, containsString("  unique snapshot"));
        assertEquals(5, singleResult.split(" unique snapshot").length);
        assertThat(singleResult, containsString("40.0% (4nanos out of 10nanos) cpu usage by thread 'Thread 4'"));
        assertThat(singleResult, containsString("30.0% (3nanos out of 10nanos) cpu usage by thread 'Thread 3'"));
        assertThat(singleResult, containsString("20.0% (2nanos out of 10nanos) cpu usage by thread 'Thread 2'"));
        assertThat(singleResult, containsString("10.0% (1nanos out of 10nanos) cpu usage by thread 'Thread 1'"));
        assertThat(innerResult, containsString("org.elasticsearch.monitor.test.method_0(Some_File:1)"));
        assertThat(innerResult, containsString("org.elasticsearch.monitor.test.method_1(Some_File:1)"));
        assertThat(innerResult, containsString("org.elasticsearch.monitor.testOther.methodFinal(Some_File:1)"));
    }

    public void testEnsureInnerDetectSkipsCurrentThread() throws Exception {
        ThreadMXBean mockedMXBean = mock(ThreadMXBean.class);
        when(mockedMXBean.isThreadCpuTimeSupported()).thenReturn(true);

        long mockCurrentThreadId = 5L;
        long[] threadIds = new long[] { mockCurrentThreadId }; // Matches half the intervalNanos for calculating time percentages

        when(mockedMXBean.getAllThreadIds()).thenReturn(threadIds);

        List<ThreadInfo> allInfos = makeThreadInfoMocksHelper(mockedMXBean, threadIds);
        when(mockedMXBean.getThreadInfo(Matchers.any(), anyInt())).thenReturn(allInfos.toArray(new ThreadInfo[0]));

        HotThreads hotThreads = new HotThreads()
            .busiestThreads(4)
            .type(HotThreads.ReportType.CPU)
            .interval(TimeValue.timeValueNanos(10))
            .threadElementsSnapshotCount(11)
            .ignoreIdleThreads(false);

        String innerResult = hotThreads.innerDetect(mockedMXBean, mockCurrentThreadId);

        assertEquals(1, innerResult.lines().count());
    }

    public void testReportTypeValueGetter() {
        ThreadInfo mockedThreadInfo = mock(ThreadInfo.class);

        when(mockedThreadInfo.getBlockedTime()).thenReturn(2L).thenReturn(0L);
        when(mockedThreadInfo.getWaitedTime()).thenReturn(3L).thenReturn(0L);

        HotThreads.ThreadTimeAccumulator info = new HotThreads.ThreadTimeAccumulator(mockedThreadInfo, 1L);

        assertEquals(1L, HotThreads.ThreadTimeAccumulator.valueGetterForReportType(HotThreads.ReportType.CPU).applyAsLong(info));
        assertEquals(3L, HotThreads.ThreadTimeAccumulator.valueGetterForReportType(HotThreads.ReportType.WAIT).applyAsLong(info));
        assertEquals(2L, HotThreads.ThreadTimeAccumulator.valueGetterForReportType(HotThreads.ReportType.BLOCK).applyAsLong(info));
    }

    public void testGetAllValidThreadInfos() {
        ThreadMXBean mockedMXBean = mock(ThreadMXBean.class);
        when(mockedMXBean.isThreadCpuTimeSupported()).thenReturn(true);

        long[] threadIds = new long[] { 1, 2, 3, 4 }; // Adds up to 10, the intervalNanos for calculating time percentages
        long mockCurrentThreadId = 0L;
        when(mockedMXBean.getAllThreadIds()).thenReturn(threadIds);

        HotThreads hotThreads = new HotThreads()
            .busiestThreads(4)
            .type(HotThreads.ReportType.CPU)
            .interval(TimeValue.timeValueNanos(10))
            .threadElementsSnapshotCount(11)
            .ignoreIdleThreads(false);

        // Test the case when all threads exist before and after sleep
        List<ThreadInfo> allInfos = makeThreadInfoMocksHelper(mockedMXBean, threadIds);

        Map<Long, HotThreads.ThreadTimeAccumulator> validInfos = hotThreads.getAllValidThreadInfos(mockedMXBean, mockCurrentThreadId);
        assertEquals(allInfos.size(), validInfos.size());

        for (long threadId : threadIds) {
            HotThreads.ThreadTimeAccumulator accumulator = validInfos.get(threadId);
            assertNotNull(accumulator);
            assertEquals(0, accumulator.cpuTime);
            assertEquals(0, accumulator.blockedTime);
            assertEquals(0, accumulator.waitedTime);
        }

        // Fake sleep, e.g don't sleep call the mock again

        Map<Long, HotThreads.ThreadTimeAccumulator> afterValidInfos = hotThreads.getAllValidThreadInfos(mockedMXBean, mockCurrentThreadId);
        assertEquals(allInfos.size(), afterValidInfos.size());
        for (long threadId : threadIds) {
            HotThreads.ThreadTimeAccumulator accumulator = afterValidInfos.get(threadId);
            assertNotNull(accumulator);
            boolean evenThreadId = ((threadId % 2) == 0);

            assertEquals(threadId, accumulator.cpuTime);
            assertEquals((evenThreadId) ? 0 : threadId, accumulator.blockedTime);
            assertEquals((evenThreadId) ? threadId : 0, accumulator.waitedTime);
        }

        // Test when a thread has terminated during sleep, we don't report that thread
        allInfos = makeThreadInfoMocksHelper(mockedMXBean, threadIds);

        validInfos = hotThreads.getAllValidThreadInfos(mockedMXBean, mockCurrentThreadId);
        assertEquals(allInfos.size(), validInfos.size());

        ThreadInfo removedInfo = allInfos.remove(0);
        long[] reducedThreadIds = new long[] { 2, 3, 4 };
        when(mockedMXBean.getAllThreadIds()).thenReturn(reducedThreadIds);
        when(mockedMXBean.getThreadInfo(Matchers.any(), anyInt())).thenReturn(allInfos.toArray(new ThreadInfo[0]));
        when(mockedMXBean.getThreadInfo(Matchers.any(long[].class))).thenReturn(allInfos.toArray(new ThreadInfo[0]));

        // Fake sleep

        afterValidInfos = hotThreads.getAllValidThreadInfos(mockedMXBean, mockCurrentThreadId);
        assertEquals(reducedThreadIds.length, afterValidInfos.size());

        for (long threadId : reducedThreadIds) {
            HotThreads.ThreadTimeAccumulator accumulator = afterValidInfos.get(threadId);
            assertNotNull(accumulator);
            boolean evenThreadId = ((threadId % 2) == 0);

            assertEquals(threadId, accumulator.cpuTime);
            assertEquals((evenThreadId) ? 0 : threadId, accumulator.blockedTime);
            assertEquals((evenThreadId) ? threadId : 0, accumulator.waitedTime);
        }

        // Test capturing timings for thread that launched while we were sleeping
        allInfos.add(0, removedInfo); // We called getInfo on this once, so second time should be with cpu/wait > 0
        when(mockedMXBean.getAllThreadIds()).thenReturn(threadIds);
        when(mockedMXBean.getThreadInfo(Matchers.any(), anyInt())).thenReturn(allInfos.toArray(new ThreadInfo[0]));
        when(mockedMXBean.getThreadInfo(Matchers.any(long[].class))).thenReturn(allInfos.toArray(new ThreadInfo[0]));

        // Fake sleep

        afterValidInfos = hotThreads.getAllValidThreadInfos(mockedMXBean, mockCurrentThreadId);
        assertEquals(threadIds.length, afterValidInfos.size());

        HotThreads.ThreadTimeAccumulator firstAccumulator = afterValidInfos.get(removedInfo.getThreadId());
        assertEquals(1, firstAccumulator.cpuTime);
        assertEquals(0, firstAccumulator.waitedTime);
        assertEquals(1, firstAccumulator.blockedTime);

        // Test skipping of current thread
        validInfos = hotThreads.getAllValidThreadInfos(mockedMXBean, threadIds[threadIds.length - 1]);
        assertEquals(threadIds.length - 1, validInfos.size());
        assertFalse(validInfos.containsKey(threadIds[threadIds.length - 1]));

        // Test skipping threads with CPU time of -1
        when(mockedMXBean.getThreadCpuTime(threadIds[0])).thenReturn(-1L);
        validInfos = hotThreads.getAllValidThreadInfos(mockedMXBean, mockCurrentThreadId);
        assertEquals(threadIds.length - 1, validInfos.size());
        assertFalse(validInfos.containsKey(threadIds[0]));

        // Test skipping null thread infos
        when(mockedMXBean.getThreadInfo(eq(threadIds[0]), anyInt())).thenReturn(null);
        validInfos = hotThreads.getAllValidThreadInfos(mockedMXBean, mockCurrentThreadId);
        assertEquals(threadIds.length - 1, validInfos.size());
        assertFalse(validInfos.containsKey(threadIds[0]));
    }

    public void testCaptureThreadStacks() throws InterruptedException {
        ThreadMXBean mockedMXBean = mock(ThreadMXBean.class);
        when(mockedMXBean.isThreadCpuTimeSupported()).thenReturn(true);

        long[] threadIds = new long[] { 1, 2, 3, 4 }; // Adds up to 10, the intervalNanos for calculating time percentages
        long mockCurrentThreadId = 0L;
        when(mockedMXBean.getAllThreadIds()).thenReturn(threadIds);

        HotThreads hotThreads = new HotThreads()
            .busiestThreads(4)
            .type(HotThreads.ReportType.CPU)
            .interval(TimeValue.timeValueNanos(1))
            .threadElementsSnapshotCount(3)
            .ignoreIdleThreads(false);

        // Setup the mocks
        List<ThreadInfo> allInfos = makeThreadInfoMocksHelper(mockedMXBean, threadIds);

        long[] topThreadIds = new long[] { threadIds[threadIds.length - 1], threadIds[threadIds.length - 2] };
        List<ThreadInfo> topThreads = List.of(
            allInfos.get(threadIds.length - 1),
            allInfos.get(threadIds.length - 2));

        when(mockedMXBean.getThreadInfo(Matchers.any(long[].class), anyInt())).thenReturn(topThreads.toArray(new ThreadInfo[0]));

        ThreadInfo[][] infosWithStacks = hotThreads.captureThreadStacks(mockedMXBean, topThreadIds);

        assertEquals(3, infosWithStacks.length);
        for (ThreadInfo[] infos : infosWithStacks) {
            assertEquals(2, infos.length);
            assertEquals(threadIds[threadIds.length - 1], infos[0].getThreadId());
            assertEquals(threadIds[threadIds.length - 2], infos[1].getThreadId());
        }
    }
}
