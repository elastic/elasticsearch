/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
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
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PyTorchResultProcessorTests extends ESTestCase {

    public void testsThreadSettings() {
        var settingsHolder = new AtomicReference<ThreadSettings>();
        var processor = new PyTorchResultProcessor("foo", settingsHolder::set);

        var settings = new ThreadSettings(1, 1);
        processor.process(mockNativeProcess(List.of(new PyTorchResult(null, settings)).iterator()));

        assertEquals(settings, settingsHolder.get());
    }

    public void testPendingRequest() {
        var processor = new PyTorchResultProcessor("foo", s -> {});

        var resultHolder = new AtomicReference<PyTorchInferenceResult>();
        processor.registerRequest("a", new AssertingResultListener(resultHolder::set));

        // this listener should only be called when the processor shuts down
        var calledOnShutdown = new AssertingResultListener(
            r -> assertThat(r.getError(), containsString("inference canceled as process is stopping"))
        );
        processor.registerRequest("b", calledOnShutdown);

        var inferenceResult = new PyTorchInferenceResult("a", null, 1000L, null);

        processor.process(mockNativeProcess(List.of(new PyTorchResult(inferenceResult, null)).iterator()));
        assertSame(inferenceResult, resultHolder.get());
        assertTrue(calledOnShutdown.hasResponse);
    }

    public void testCancelPendingRequest() {
        var processor = new PyTorchResultProcessor("foo", s -> {});

        processor.registerRequest("a", new AssertingResultListener(r -> fail("listener a should not be called")));

        processor.ignoreResponseWithoutNotifying("a");

        var inferenceResult = new PyTorchInferenceResult("a", null, 1000L, null);
        processor.process(mockNativeProcess(List.of(new PyTorchResult(inferenceResult, null)).iterator()));
    }

    public void testPendingRequestAreCalledAtShutdown() {
        var processor = new PyTorchResultProcessor("foo", s -> {});

        var listeners = List.of(
            new AssertingResultListener(r -> assertEquals(r.getError(), "inference canceled as process is stopping")),
            new AssertingResultListener(r -> assertEquals(r.getError(), "inference canceled as process is stopping")),
            new AssertingResultListener(r -> assertEquals(r.getError(), "inference canceled as process is stopping")),
            new AssertingResultListener(r -> assertEquals(r.getError(), "inference canceled as process is stopping"))
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

    private static class AssertingResultListener implements ActionListener<PyTorchInferenceResult> {
        boolean hasResponse;
        final Consumer<PyTorchInferenceResult> responseAsserter;

        AssertingResultListener(Consumer<PyTorchInferenceResult> responseAsserter) {
            this.responseAsserter = responseAsserter;
        }

        @Override
        public void onResponse(PyTorchInferenceResult pyTorchInferenceResult) {
            hasResponse = true;
            responseAsserter.accept(pyTorchInferenceResult);
        }

        @Override
        public void onFailure(Exception e) {
            fail(e.getMessage());
        }
    }

    public void testsStats() {
        var processor = new PyTorchResultProcessor("foo", s -> {});

        var pendingA = new AssertingResultListener(r -> {});
        var pendingB = new AssertingResultListener(r -> {});
        var pendingC = new AssertingResultListener(r -> {});

        processor.registerRequest("a", pendingA);
        processor.registerRequest("b", pendingB);
        processor.registerRequest("c", pendingC);

        var a = new PyTorchInferenceResult("a", null, 1000L, null);
        var b = new PyTorchInferenceResult("b", null, 900L, null);
        var c = new PyTorchInferenceResult("c", null, 200L, null);

        processor.processInferenceResult(a);
        var stats = processor.getResultStats();
        assertThat(stats.errorCount(), comparesEqualTo(0));
        assertThat(stats.numberOfPendingResults(), comparesEqualTo(2));
        assertThat(stats.timingStats().getCount(), comparesEqualTo(1L));
        assertThat(stats.timingStats().getSum(), comparesEqualTo(1000L));

        processor.processInferenceResult(b);
        stats = processor.getResultStats();
        assertThat(stats.errorCount(), comparesEqualTo(0));
        assertThat(stats.numberOfPendingResults(), comparesEqualTo(1));
        assertThat(stats.timingStats().getCount(), comparesEqualTo(2L));
        assertThat(stats.timingStats().getSum(), comparesEqualTo(1900L));

        processor.processInferenceResult(c);
        stats = processor.getResultStats();
        assertThat(stats.errorCount(), comparesEqualTo(0));
        assertThat(stats.numberOfPendingResults(), comparesEqualTo(0));
        assertThat(stats.timingStats().getCount(), comparesEqualTo(3L));
        assertThat(stats.timingStats().getSum(), comparesEqualTo(2100L));
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

        var inferenceResults = List.of(
            // 1st period
            new PyTorchInferenceResult("foo", null, 200L, null),
            new PyTorchInferenceResult("foo", null, 200L, null),
            new PyTorchInferenceResult("foo", null, 200L, null),
            // 2nd
            new PyTorchInferenceResult("foo", null, 100L, null),
            // 4th
            new PyTorchInferenceResult("foo", null, 300L, null),
            // 7th
            new PyTorchInferenceResult("foo", null, 400L, null),
            new PyTorchInferenceResult("foo", null, 400L, null),
            // 8th
            new PyTorchInferenceResult("foo", null, 500L, null),
            new PyTorchInferenceResult("foo", null, 500L, null),
            new PyTorchInferenceResult("foo", null, 500L, null)
        );

        var timeSupplier = new TimeSupplier(resultTimestamps);
        var processor = new PyTorchResultProcessor("foo", s -> {}, timeSupplier);

        // 1st period
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 200L, null));
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 200L, null));
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 200L, null));
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
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 100L, null));
        stats = processor.getResultStats();
        assertNotNull(stats.recentStats());
        assertThat(stats.recentStats().requestsProcessed(), equalTo(1L));
        assertThat(stats.recentStats().avgInferenceTime(), closeTo(100.0, 0.00001));
        assertThat(stats.lastUsed(), equalTo(Instant.ofEpochMilli(resultTimestamps[6])));
        assertThat(stats.peakThroughput(), equalTo(3L));

        stats = processor.getResultStats();
        assertThat(stats.recentStats().requestsProcessed(), equalTo(0L));

        // 4th period
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 300L, null));
        stats = processor.getResultStats();
        assertNotNull(stats.recentStats());
        assertThat(stats.recentStats().requestsProcessed(), equalTo(1L));
        assertThat(stats.recentStats().avgInferenceTime(), closeTo(300.0, 0.00001));
        assertThat(stats.lastUsed(), equalTo(Instant.ofEpochMilli(resultTimestamps[9])));

        // 7th period
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 410L, null));
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 390L, null));
        stats = processor.getResultStats();
        assertThat(stats.recentStats().requestsProcessed(), equalTo(0L));
        assertThat(stats.recentStats().avgInferenceTime(), nullValue());
        stats = processor.getResultStats(); // called in the next period
        assertNotNull(stats.recentStats());
        assertThat(stats.recentStats().requestsProcessed(), equalTo(2L));
        assertThat(stats.recentStats().avgInferenceTime(), closeTo(400.0, 0.00001));
        assertThat(stats.lastUsed(), equalTo(Instant.ofEpochMilli(resultTimestamps[12])));

        // 8th period
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 510L, null));
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 500L, null));
        processor.processInferenceResult(new PyTorchInferenceResult("foo", null, 490L, null));
        stats = processor.getResultStats();
        assertNotNull(stats.recentStats());
        assertThat(stats.recentStats().requestsProcessed(), equalTo(3L));
        assertThat(stats.recentStats().avgInferenceTime(), closeTo(500.0, 0.00001));
        assertThat(stats.lastUsed(), equalTo(Instant.ofEpochMilli(resultTimestamps[17])));
        assertThat(stats.peakThroughput(), equalTo(3L));
    }

    private static class TimeSupplier implements LongSupplier {
        private int index;
        private long[] times;

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
