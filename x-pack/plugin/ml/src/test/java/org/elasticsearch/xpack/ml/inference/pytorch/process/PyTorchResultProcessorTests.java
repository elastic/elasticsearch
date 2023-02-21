/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.pytorch.results.AckResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.ErrorResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.ThreadSettings;

import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor.REPORTING_PERIOD_MS;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PyTorchResultProcessorTests extends ESTestCase {

    public void testsThreadSettings() {
        var settingsHolder = new AtomicReference<ThreadSettings>();
        var processor = new PyTorchResultProcessor("deployment-foo", settingsHolder::set);

        var settings = new ThreadSettings(1, 1);
        processor.registerRequest("thread-setting", new AssertingResultListener(r -> assertEquals(settings, r.threadSettings())));

        processor.process(
            mockNativeProcess(List.of(new PyTorchResult("thread-setting", null, null, null, settings, null, null)).iterator())
        );

        assertEquals(settings, settingsHolder.get());
    }

    public void testResultsProcessing() {
        var inferenceResult = new PyTorchInferenceResult(null);
        var threadSettings = new ThreadSettings(1, 1);
        var ack = new AckResult(true);
        var errorResult = new ErrorResult("a bad thing has happened");

        var inferenceListener = new AssertingResultListener(r -> assertEquals(inferenceResult, r.inferenceResult()));
        var threadSettingsListener = new AssertingResultListener(r -> assertEquals(threadSettings, r.threadSettings()));
        var ackListener = new AssertingResultListener(r -> assertEquals(ack, r.ackResult()));
        var errorListener = new AssertingResultListener(r -> assertEquals(errorResult, r.errorResult()));

        var processor = new PyTorchResultProcessor("foo", s -> {});
        processor.registerRequest("a", inferenceListener);
        processor.registerRequest("b", threadSettingsListener);
        processor.registerRequest("c", ackListener);
        processor.registerRequest("d", errorListener);

        processor.process(
            mockNativeProcess(
                List.of(
                    new PyTorchResult("a", true, 1000L, inferenceResult, null, null, null),
                    new PyTorchResult("b", null, null, null, threadSettings, null, null),
                    new PyTorchResult("c", null, null, null, null, ack, null),
                    new PyTorchResult("d", null, null, null, null, null, errorResult)
                ).iterator()
            )
        );

        assertTrue(inferenceListener.hasResponse);
        assertTrue(threadSettingsListener.hasResponse);
        assertTrue(ackListener.hasResponse);
        assertTrue(errorListener.hasResponse);
    }

    public void testPendingRequest() {
        var processor = new PyTorchResultProcessor("foo", s -> {});

        var resultHolder = new AtomicReference<PyTorchInferenceResult>();
        processor.registerRequest("a", new AssertingResultListener(r -> resultHolder.set(r.inferenceResult())));

        // this listener should only be called when the processor shuts down
        var calledOnShutdown = new AssertingResultListener(
            r -> assertThat(r.errorResult().error(), containsString("inference canceled as process is stopping"))
        );
        processor.registerRequest("b", calledOnShutdown);

        var inferenceResult = new PyTorchInferenceResult(null);

        processor.process(mockNativeProcess(List.of(new PyTorchResult("a", false, 1000L, inferenceResult, null, null, null)).iterator()));
        assertSame(inferenceResult, resultHolder.get());
        assertTrue(calledOnShutdown.hasResponse);
    }

    public void testCancelPendingRequest() {
        var processor = new PyTorchResultProcessor("foo", s -> {});

        processor.registerRequest("a", new AssertingResultListener(r -> fail("listener a should not be called")));

        processor.ignoreResponseWithoutNotifying("a");

        var inferenceResult = new PyTorchInferenceResult(null);
        processor.process(mockNativeProcess(List.of(new PyTorchResult("a", false, 1000L, inferenceResult, null, null, null)).iterator()));
    }

    public void testPendingRequestAreCalledAtShutdown() {
        var processor = new PyTorchResultProcessor("foo", s -> {});

        var listeners = List.of(
            new AssertingResultListener(r -> assertEquals(r.errorResult().error(), "inference canceled as process is stopping")),
            new AssertingResultListener(r -> assertEquals(r.errorResult().error(), "inference canceled as process is stopping")),
            new AssertingResultListener(r -> assertEquals(r.errorResult().error(), "inference canceled as process is stopping")),
            new AssertingResultListener(r -> assertEquals(r.errorResult().error(), "inference canceled as process is stopping"))
        );

        int i = 0;
        for (var l : listeners) {
            processor.registerRequest(Integer.toString(i++), l);
        }

        processor.process(mockNativeProcess(Collections.emptyIterator()));

        for (var l : listeners) {
            assertTrue(l.hasResponse);
        }
    }

    private static class AssertingResultListener implements ActionListener<PyTorchResult> {
        boolean hasResponse;
        final Consumer<PyTorchResult> responseAsserter;

        AssertingResultListener(Consumer<PyTorchResult> responseAsserter) {
            this.responseAsserter = responseAsserter;
        }

        @Override
        public void onResponse(PyTorchResult pyTorchResult) {
            hasResponse = true;
            responseAsserter.accept(pyTorchResult);
        }

        @Override
        public void onFailure(Exception e) {
            fail(e.getMessage());
        }
    }

    private PyTorchResult wrapInferenceResult(String requestId, boolean isCacheHit, long timeMs) {
        return new PyTorchResult(requestId, isCacheHit, timeMs, new PyTorchInferenceResult(null), null, null, null);
    }

    public void testsStats() {
        var processor = new PyTorchResultProcessor("foo", s -> {});

        var pendingA = new AssertingResultListener(r -> {});
        var pendingB = new AssertingResultListener(r -> {});
        var pendingC = new AssertingResultListener(r -> {});

        processor.registerRequest("a", pendingA);
        processor.registerRequest("b", pendingB);
        processor.registerRequest("c", pendingC);

        var a = wrapInferenceResult("a", false, 1000L);
        var b = wrapInferenceResult("b", false, 900L);
        var c = wrapInferenceResult("c", true, 200L); // cache hit

        processor.processInferenceResult(a);
        var stats = processor.getResultStats();
        assertThat(stats.errorCount(), equalTo(0));
        assertThat(stats.cacheHitCount(), equalTo(0L));
        assertThat(stats.numberOfPendingResults(), equalTo(2));
        assertThat(stats.timingStats().getCount(), equalTo(1L));
        assertThat(stats.timingStats().getSum(), equalTo(1000L));
        assertThat(stats.timingStatsExcludingCacheHits().getCount(), equalTo(1L));
        assertThat(stats.timingStatsExcludingCacheHits().getSum(), equalTo(1000L));

        processor.processInferenceResult(b);
        stats = processor.getResultStats();
        assertThat(stats.errorCount(), equalTo(0));
        assertThat(stats.cacheHitCount(), equalTo(0L));
        assertThat(stats.numberOfPendingResults(), equalTo(1));
        assertThat(stats.timingStats().getCount(), equalTo(2L));
        assertThat(stats.timingStats().getSum(), equalTo(1900L));
        assertThat(stats.timingStatsExcludingCacheHits().getCount(), equalTo(2L));
        assertThat(stats.timingStatsExcludingCacheHits().getSum(), equalTo(1900L));

        processor.processInferenceResult(c);
        stats = processor.getResultStats();
        assertThat(stats.errorCount(), equalTo(0));
        assertThat(stats.cacheHitCount(), equalTo(1L));
        assertThat(stats.numberOfPendingResults(), equalTo(0));
        assertThat(stats.timingStats().getCount(), equalTo(3L));
        assertThat(stats.timingStats().getSum(), equalTo(2100L));
        assertThat(stats.timingStatsExcludingCacheHits().getCount(), equalTo(2L));
        assertThat(stats.timingStatsExcludingCacheHits().getSum(), equalTo(1900L));
    }

    public void testsTimeDependentStats() {

        long start = System.currentTimeMillis();
        // the first value is used in the ctor to set the start time.
        // each period has a number of timestamps for each inference results
        // and every call to getResultsStats()
        var resultTimestamps = new long[] {
            start,
            // 1st period: 2 results and 2 calls to getResultsStats() in different periods
            start + 1,
            start + 10,
            start + 20,
            start + 50,
            start + REPORTING_PERIOD_MS,
            // 2nd period: 1 result and 1 call to getResultsStats() in the next period
            start + REPORTING_PERIOD_MS + 20,
            start + (2L * REPORTING_PERIOD_MS) + 60,
            // call to getResultsStats()
            start + (3L * REPORTING_PERIOD_MS) + 1,
            // 4th after a missing period: 1 result and a getResultsStats() in the next period
            start + (3L * REPORTING_PERIOD_MS) + 20,
            start + (4L * REPORTING_PERIOD_MS) + 71,
            // 7th after 2 missing periods: 2 results and 2 calls to getResultsStats() in different periods
            start + (6L * REPORTING_PERIOD_MS) + 30,
            start + (6L * REPORTING_PERIOD_MS) + 50,
            start + (6L * REPORTING_PERIOD_MS) + 80,
            start + (7L * REPORTING_PERIOD_MS) + 81,
            // 8th 3 results and 1 call to getResultsStats() in the next period
            start + (7L * REPORTING_PERIOD_MS) + 40,
            start + (7L * REPORTING_PERIOD_MS) + 44,
            start + (7L * REPORTING_PERIOD_MS) + 55,
            start + (8L * REPORTING_PERIOD_MS) + 90 };

        var timeSupplier = new TimeSupplier(resultTimestamps);
        var processor = new PyTorchResultProcessor("foo", s -> {}, timeSupplier);

        // 1st period
        processor.processInferenceResult(wrapInferenceResult("foo", false, 200L));
        processor.processInferenceResult(wrapInferenceResult("foo", false, 200L));
        processor.processInferenceResult(wrapInferenceResult("foo", false, 200L));
        // first call has no results as is in the same period
        var stats = processor.getResultStats();
        assertThat(stats.recentStats().requestsProcessed(), equalTo(0L));
        assertThat(stats.recentStats().avgInferenceTime(), nullValue());
        // 2nd time in the next period
        stats = processor.getResultStats();
        assertNotNull(stats.recentStats());
        assertThat(stats.recentStats().requestsProcessed(), equalTo(3L));
        assertThat(stats.recentStats().avgInferenceTime(), closeTo(200.0, 0.00001));
        assertThat(stats.lastUsed(), equalTo(Instant.ofEpochMilli(resultTimestamps[3])));
        assertThat(stats.peakThroughput(), equalTo(3L));

        // 2nd period
        processor.processInferenceResult(wrapInferenceResult("foo", false, 100L));
        stats = processor.getResultStats();
        assertNotNull(stats.recentStats());
        assertThat(stats.recentStats().requestsProcessed(), equalTo(1L));
        assertThat(stats.recentStats().avgInferenceTime(), closeTo(100.0, 0.00001));
        assertThat(stats.lastUsed(), equalTo(Instant.ofEpochMilli(resultTimestamps[6])));
        assertThat(stats.peakThroughput(), equalTo(3L));

        stats = processor.getResultStats();
        assertThat(stats.recentStats().requestsProcessed(), equalTo(0L));

        // 4th period
        processor.processInferenceResult(wrapInferenceResult("foo", false, 300L));
        stats = processor.getResultStats();
        assertNotNull(stats.recentStats());
        assertThat(stats.recentStats().requestsProcessed(), equalTo(1L));
        assertThat(stats.recentStats().avgInferenceTime(), closeTo(300.0, 0.00001));
        assertThat(stats.lastUsed(), equalTo(Instant.ofEpochMilli(resultTimestamps[9])));

        // 7th period
        processor.processInferenceResult(wrapInferenceResult("foo", false, 410L));
        processor.processInferenceResult(wrapInferenceResult("foo", false, 390L));
        stats = processor.getResultStats();
        assertThat(stats.recentStats().requestsProcessed(), equalTo(0L));
        assertThat(stats.recentStats().avgInferenceTime(), nullValue());
        stats = processor.getResultStats(); // called in the next period
        assertNotNull(stats.recentStats());
        assertThat(stats.recentStats().requestsProcessed(), equalTo(2L));
        assertThat(stats.recentStats().avgInferenceTime(), closeTo(400.0, 0.00001));
        assertThat(stats.lastUsed(), equalTo(Instant.ofEpochMilli(resultTimestamps[12])));

        // 8th period
        processor.processInferenceResult(wrapInferenceResult("foo", false, 510L));
        processor.processInferenceResult(wrapInferenceResult("foo", false, 500L));
        processor.processInferenceResult(wrapInferenceResult("foo", false, 490L));
        stats = processor.getResultStats();
        assertNotNull(stats.recentStats());
        assertThat(stats.recentStats().requestsProcessed(), equalTo(3L));
        assertThat(stats.recentStats().avgInferenceTime(), closeTo(500.0, 0.00001));
        assertThat(stats.lastUsed(), equalTo(Instant.ofEpochMilli(resultTimestamps[17])));
        assertThat(stats.peakThroughput(), equalTo(3L));
    }

    private static class TimeSupplier implements LongSupplier {
        private int index;
        private final long[] times;

        TimeSupplier(long[] times) {
            this.times = times;
        }

        @Override
        public long getAsLong() {
            if (index == times.length) {
                fail("requested too many times");
            }
            return times[index++];
        }
    }

    private NativePyTorchProcess mockNativeProcess(Iterator<PyTorchResult> results) {
        var process = mock(NativePyTorchProcess.class);
        when(process.readResults()).thenReturn(results);
        return process;
    }
}
