/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;

import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SwappableLogRecordExporterTests extends ESTestCase {

    public void testOperationsGoToCurrentDelegate() {
        RecordingExporter first = new RecordingExporter();
        RecordingExporter second = new RecordingExporter();
        try (SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(first)) {
            assertTrue(exporter.export(List.of()).isSuccess());
            assertTrue(exporter.flush().isSuccess());
            exporter.swap(second);
            assertThat(first.shutdowns.get(), equalTo(1));
            assertTrue(exporter.export(List.of()).isSuccess());
            assertTrue(exporter.export(List.of()).isSuccess());
            assertTrue(exporter.flush().isSuccess());

            assertThat(first.exports.get(), equalTo(1));
            assertThat(first.flushes.get(), equalTo(1));
            assertThat(second.exports.get(), equalTo(2));
            assertThat(second.flushes.get(), equalTo(1));
        }
        assertThat(second.shutdowns.get(), equalTo(1));
    }

    /** An export must already own a reference during preparation, before it can return an asynchronous result. */
    public void testSwapDuringExportPreparation() {
        CountDownLatch preparing = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        RecordingExporter old = new RecordingExporter() {
            @Override
            public CompletableResultCode export(Collection<LogRecordData> logs) {
                preparing.countDown();
                safeAwait(resume);
                return super.export(logs);
            }
        };
        RecordingExporter replacement = new RecordingExporter();
        try (SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(old)) {
            runInParallel(() -> assertTrue(exporter.export(List.of()).isSuccess()), () -> {
                safeAwait(preparing);
                try {
                    exporter.swap(replacement);
                    assertThat(old.shutdowns.get(), equalTo(0));
                    assertTrue(exporter.export(List.of()).isSuccess());
                } finally {
                    resume.countDown();
                }
            });
            assertThat(old.exports.get(), equalTo(1));
            assertThat(old.shutdowns.get(), equalTo(1));
        }
    }

    public void testRetirementWaitsForSuccessfulExport() {
        assertRetirementWaitsForCompletion(false, true);
    }

    public void testRetirementWaitsForFailedExport() {
        assertRetirementWaitsForCompletion(false, false);
    }

    public void testRetirementWaitsForFlush() {
        assertRetirementWaitsForCompletion(true, true);
    }

    private void assertRetirementWaitsForCompletion(boolean flush, boolean success) {
        RecordingExporter old = new RecordingExporter();
        CompletableResultCode pending = new CompletableResultCode();
        if (flush) {
            old.flushResult = pending;
        } else {
            old.exportResult = pending;
        }
        try (SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(old)) {
            assertSame(pending, flush ? exporter.flush() : exporter.export(List.of()));
            exporter.swap(new RecordingExporter());
            assertThat(old.shutdowns.get(), equalTo(0));
            if (success) {
                pending.succeed();
            } else {
                pending.fail();
            }
            assertThat(old.shutdowns.get(), equalTo(1));
        }
    }

    public void testRetirementWaitsForAllOperations() {
        RecordingExporter old = new RecordingExporter();
        old.exportResult = new CompletableResultCode();
        old.flushResult = new CompletableResultCode();
        try (SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(old)) {
            exporter.export(List.of());
            exporter.flush();
            exporter.swap(new RecordingExporter());
            old.exportResult.succeed();
            assertThat(old.shutdowns.get(), equalTo(0));
            old.flushResult.succeed();
            assertThat(old.shutdowns.get(), equalTo(1));
        }
    }

    public void testExportExceptionReleasesReference() {
        assertExceptionReleasesReference(false);
    }

    public void testFlushExceptionReleasesReference() {
        assertExceptionReleasesReference(true);
    }

    public void testNullOperationResultReleasesReference() {
        for (boolean flush : List.of(false, true)) {
            RecordingExporter old = new RecordingExporter();
            old.exportResult = null;
            old.flushResult = null;
            try (SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(old)) {
                expectThrows(NullPointerException.class, () -> {
                    if (flush) {
                        exporter.flush();
                    } else {
                        exporter.export(List.of());
                    }
                });
                exporter.swap(new RecordingExporter());
                assertThat(old.shutdowns.get(), equalTo(1));
            }
        }
    }

    private void assertExceptionReleasesReference(boolean flush) {
        RuntimeException failure = new IllegalStateException("operation failed");
        RecordingExporter old = new RecordingExporter() {
            @Override
            public CompletableResultCode export(Collection<LogRecordData> logs) {
                throw failure;
            }

            @Override
            public CompletableResultCode flush() {
                throw failure;
            }
        };
        try (SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(old)) {
            assertSame(failure, expectThrows(IllegalStateException.class, () -> {
                if (flush) {
                    exporter.flush();
                } else {
                    exporter.export(List.of());
                }
            }));
            exporter.swap(new RecordingExporter());
            assertThat(old.shutdowns.get(), equalTo(1));
        }
    }

    public void testShutdownClosesCurrentAndRetiredDelegates() {
        RecordingExporter old = new RecordingExporter();
        old.exportResult = new CompletableResultCode();
        old.shutdownResult = new CompletableResultCode();
        RecordingExporter current = new RecordingExporter();
        current.exportResult = new CompletableResultCode();
        current.shutdownResult = new CompletableResultCode();
        SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(old);
        exporter.export(List.of());
        exporter.swap(current);
        exporter.export(List.of());

        CompletableResultCode shutdown = exporter.shutdown();
        assertSame(shutdown, exporter.shutdown());
        assertFalse(shutdown.isDone());
        assertThat(old.shutdowns.get(), equalTo(1));
        assertThat(current.shutdowns.get(), equalTo(1));
        for (CompletableResultCode result : List.of(exporter.export(List.of()), exporter.flush())) {
            assertTrue(result.isDone());
            assertFalse(result.isSuccess());
            assertThat(result.getFailureThrowable(), instanceOf(IllegalStateException.class));
            assertThat(result.getFailureThrowable().getMessage(), equalTo("log exporter is shut down"));
        }
        assertThat(current.exports.get(), equalTo(1));
        assertThat(current.flushes.get(), equalTo(0));

        current.shutdownResult.succeed();
        assertFalse(shutdown.isDone());
        old.shutdownResult.succeed();
        assertTrue(shutdown.isSuccess());
        // Completion after forced teardown releases references without another shutdown call.
        old.exportResult.fail();
        current.exportResult.fail();
        assertThat(old.shutdowns.get(), equalTo(1));
        assertThat(current.shutdowns.get(), equalTo(1));
    }

    public void testShutdownWaitsForRetirementAlreadyInProgress() {
        RecordingExporter old = new RecordingExporter();
        old.shutdownResult = new CompletableResultCode();
        SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(old);
        exporter.swap(new RecordingExporter());
        assertThat(old.shutdowns.get(), equalTo(1));

        CompletableResultCode shutdown = exporter.shutdown();
        assertFalse(shutdown.isDone());
        RuntimeException failure = new IllegalStateException("shutdown failed");
        old.shutdownResult.failExceptionally(failure);
        assertTrue(shutdown.isDone());
        assertFalse(shutdown.isSuccess());
        assertSame(failure, shutdown.getFailureThrowable());
        assertThat(old.shutdowns.get(), equalTo(1));
    }

    public void testShutdownExceptionDoesNotPreventOtherDelegatesClosing() {
        RuntimeException failure = new IllegalStateException("shutdown threw");
        RecordingExporter old = new RecordingExporter() {
            @Override
            public CompletableResultCode shutdown() {
                super.shutdown();
                throw failure;
            }
        };
        old.exportResult = new CompletableResultCode();
        RecordingExporter current = new RecordingExporter();
        SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(old);
        exporter.export(List.of());
        exporter.swap(current);

        CompletableResultCode shutdown = exporter.shutdown();
        assertTrue(shutdown.isDone());
        assertSame(failure, shutdown.getFailureThrowable());
        assertThat(current.shutdowns.get(), equalTo(1));
        old.exportResult.fail();
        assertThat(old.shutdowns.get(), equalTo(1));
    }

    public void testMultipleSwapsWithConcurrentCompletionAndShutdown() {
        RecordingExporter first = new RecordingExporter();
        first.exportResult = new CompletableResultCode();
        first.shutdownResult = new CompletableResultCode();
        RecordingExporter second = new RecordingExporter();
        second.exportResult = new CompletableResultCode();
        second.shutdownResult = new CompletableResultCode();
        RecordingExporter third = new RecordingExporter();
        third.shutdownResult = new CompletableResultCode();
        SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(first);
        exporter.export(List.of());
        exporter.swap(second);
        exporter.export(List.of());
        exporter.swap(third);

        startInParallel(first.exportResult::succeed, second.exportResult::fail, exporter::shutdown, exporter::shutdown);
        CompletableResultCode shutdown = exporter.shutdown();
        assertFalse(shutdown.isDone());
        for (RecordingExporter delegate : List.of(first, second, third)) {
            assertThat(delegate.shutdowns.get(), equalTo(1));
            delegate.shutdownResult.succeed();
        }
        assertTrue(shutdown.isSuccess());
    }

    public void testSwapAfterShutdownClosesRejectedDelegate() {
        RecordingExporter current = new RecordingExporter();
        RecordingExporter rejected = new RecordingExporter();
        SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(current);
        assertTrue(exporter.shutdown().isSuccess());
        expectThrows(IllegalStateException.class, () -> exporter.swap(rejected));
        assertThat(current.shutdowns.get(), equalTo(1));
        assertThat(rejected.shutdowns.get(), equalTo(1));
    }

    public void testSwapDuringShutdownClosesRejectedDelegate() {
        RecordingExporter current = new RecordingExporter();
        current.shutdownResult = new CompletableResultCode();
        RecordingExporter rejected = new RecordingExporter();
        SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(current);

        CompletableResultCode shutdown = exporter.shutdown();
        assertFalse(shutdown.isDone());
        IllegalStateException failure = expectThrows(IllegalStateException.class, () -> exporter.swap(rejected));
        assertThat(failure.getMessage(), equalTo("log exporter is shut down"));
        assertThat(current.shutdowns.get(), equalTo(1));
        assertThat(rejected.shutdowns.get(), equalTo(1));
        assertFalse(shutdown.isDone());

        current.shutdownResult.succeed();
        assertTrue(shutdown.isSuccess());
    }

    public void testSwapConcurrentWithShutdownDoesNotLeakDelegate() {
        RecordingExporter current = new RecordingExporter();
        RecordingExporter replacement = new RecordingExporter();
        SwappableLogRecordExporter exporter = new SwappableLogRecordExporter(current);
        startInParallel(exporter::shutdown, () -> {
            try {
                exporter.swap(replacement);
            } catch (IllegalStateException e) {
                assertThat(e.getMessage(), equalTo("log exporter is shut down"));
            }
        });
        assertTrue(exporter.shutdown().isSuccess());
        assertThat(current.shutdowns.get(), equalTo(1));
        assertThat(replacement.shutdowns.get(), equalTo(1));
    }

    /**
     * Controls operation and shutdown completion independently to exercise races without a network sender. Like the
     * real gRPC exporter, it rejects exports after shutdown, including ones delayed during preparation by a subclass.
     */
    private static class RecordingExporter implements LogRecordExporter {
        final AtomicInteger exports = new AtomicInteger();
        final AtomicInteger flushes = new AtomicInteger();
        final AtomicInteger shutdowns = new AtomicInteger();
        CompletableResultCode exportResult = CompletableResultCode.ofSuccess();
        CompletableResultCode flushResult = CompletableResultCode.ofSuccess();
        CompletableResultCode shutdownResult = CompletableResultCode.ofSuccess();

        @Override
        public CompletableResultCode export(Collection<LogRecordData> logs) {
            exports.incrementAndGet();
            return shutdowns.get() == 0 ? exportResult : CompletableResultCode.ofFailure();
        }

        @Override
        public CompletableResultCode flush() {
            flushes.incrementAndGet();
            return shutdowns.get() == 0 ? flushResult : CompletableResultCode.ofFailure();
        }

        @Override
        public CompletableResultCode shutdown() {
            shutdowns.incrementAndGet();
            return shutdownResult;
        }
    }
}
