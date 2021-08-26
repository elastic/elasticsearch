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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HotThreadsTests extends ESTestCase {

    public void testSupportedThreadsReportType() {
        for (var type: new String[] {"unsupported", "", null, "CPU", "WAIT", "BLOCK" }) {
            expectThrows(IllegalArgumentException.class, () -> new HotThreads().type(HotThreads.ReportType.of(type)));
        }

        for (var type : new String[] { "cpu", "wait", "block" }) {
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
        for (var threadName : new String[] {
            "Signal Dispatcher", "Finalizer", "Reference Handler", "Notification Thread", "Common-Cleaner" }) {
            ThreadInfo mockedThreadInfo = mock(ThreadInfo.class);
            when(mockedThreadInfo.getThreadName()).thenReturn(threadName);
            assertTrue(HotThreads.isIdleThread(mockedThreadInfo));
        }

        for (var threadName : new String[] { "Text", "", null, "Finalizer".toLowerCase(Locale.ROOT) }) {
            ThreadInfo mockedThreadInfo = mock(ThreadInfo.class);
            when(mockedThreadInfo.getThreadName()).thenReturn(threadName);
            when(mockedThreadInfo.getStackTrace()).thenReturn(new StackTraceElement[0]);
            assertFalse(HotThreads.isIdleThread(mockedThreadInfo));
        }

        List<StackTraceElement> testJvmStack = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.test", "methodOne"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"},
                new String[]{"org.elasticsearch.monitor.test", "methodThree"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodFour"}
            ));

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

        for (var extraFrame : idleThreadStackElements) {
            ThreadInfo idleThread = mock(ThreadInfo.class);
            when(idleThread.getThreadName()).thenReturn("Idle Thread");
            when(idleThread.getStackTrace()).thenReturn(new StackTraceElement[] {extraFrame});
            assertTrue(HotThreads.isIdleThread(idleThread));

            var topOfStack = new ArrayList<>(testJvmStack);
            topOfStack.add(0, extraFrame);
            assertIdleThreadHelper(idleThread, topOfStack);

            var bottomOfStack = new ArrayList<>(testJvmStack);
            bottomOfStack.add(extraFrame);
            assertIdleThreadHelper(idleThread, bottomOfStack);

            if (testJvmStack.size() > 1) {
                var middleOfStack = new ArrayList<>(testJvmStack);
                middleOfStack.add(between(Math.min(1, testJvmStack.size()), Math.max(0, testJvmStack.size() - 1)), extraFrame);
                assertIdleThreadHelper(idleThread, middleOfStack);
            }
        }
    }

    public void testSimilarity() {
        var stackOne = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.test", "methodOne"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"}
            )).toArray(new StackTraceElement[0]);

        var stackTwo = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.test1", "methodOne"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"}
            )).toArray(new StackTraceElement[0]);

        var stackThree = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"},
                new String[]{"org.elasticsearch.monitor.test", "methodOne"}
            )).toArray(new StackTraceElement[0]);

        var stackFour = makeThreadStackHelper(
            List.of(
                new String[]{"org.elasticsearch.monitor.testPrior", "methodOther"},
                new String[]{"org.elasticsearch.monitor.test", "methodOne"},
                new String[]{"org.elasticsearch.monitor.testOther", "methodTwo"}
            )).toArray(new StackTraceElement[0]);

        HotThreads hotThreads = new HotThreads();

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

        var testCases = new SimilarityTestCase[] {
            new SimilarityTestCase(stackOne, stackOne, 2),
            new SimilarityTestCase(stackOne, stackTwo, 1),
            new SimilarityTestCase(stackOne, stackThree, 0),
            new SimilarityTestCase(stackOne, stackFour, 2),
            new SimilarityTestCase(stackTwo, stackFour, 1),
            new SimilarityTestCase(stackOne, new StackTraceElement[0], 0),
        };

        for (var testCase : testCases) {
            ThreadInfo threadOne = mock(ThreadInfo.class);
            when(threadOne.getThreadName()).thenReturn("Thread One");
            when(threadOne.getStackTrace()).thenReturn(testCase.one);

            ThreadInfo threadTwo = mock(ThreadInfo.class);
            when(threadTwo.getThreadName()).thenReturn("Thread Two");
            when(threadTwo.getStackTrace()).thenReturn(testCase.two);

            assertEquals(testCase.similarityScore, hotThreads.similarity(threadOne, threadTwo));
        }

        ThreadInfo threadOne = mock(ThreadInfo.class);
        when(threadOne.getThreadName()).thenReturn("Thread One");
        when(threadOne.getStackTrace()).thenReturn(testCases[0].one);

        assertEquals(0, hotThreads.similarity(threadOne, null));
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

            var stack = makeThreadStackHelper(
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

        var threadIds = new long[] { 1, 2, 3, 4 }; // Adds up to 10, the intervalNanos for calculating time percentages
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
    }

    public void testEnsureInnerDetectSkipsCurrentThread() throws Exception {
        ThreadMXBean mockedMXBean = mock(ThreadMXBean.class);
        when(mockedMXBean.isThreadCpuTimeSupported()).thenReturn(true);

        long mockCurrentThreadId = 5L;
        var threadIds = new long[] { mockCurrentThreadId }; // Matches half the intervalNanos for calculating time percentages

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
}
