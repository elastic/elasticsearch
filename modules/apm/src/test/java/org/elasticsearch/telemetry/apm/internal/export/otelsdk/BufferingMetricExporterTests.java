/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.contrib.disk.buffering.storage.SignalStorage;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.apm.RecordingOtelMeterProvider;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.telemetry.InstrumentType.LONG_COUNTER;
import static org.elasticsearch.telemetry.InstrumentType.LONG_GAUGE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class BufferingMetricExporterTests extends ESTestCase {

    private RecordingOtelMeterProvider meterProvider;
    private Path bufferDir;
    private FakeMetricExporter delegate;
    private BufferingMetricExporter exporter;

    @Before
    public void setupCommon() throws Exception {
        meterProvider = new RecordingOtelMeterProvider();
        bufferDir = createTempDir("telemetry-buffer");
        delegate = new FakeMetricExporter();
    }

    @After
    public void shutdownExporter() {
        if (exporter != null) {
            exporter.shutdown();
        }
    }

    private void build(Settings overrides) {
        Settings merged = Settings.builder()
            .put("telemetry.metrics.buffer.disk_size", "10mb")
            .put("telemetry.metrics.buffer.ttl", "5m")
            .put(overrides)
            .build();
        exporter = new BufferingMetricExporter(delegate, merged, bufferDir, () -> meterProvider, 100);
    }

    private CompletableResultCode exportAndWait(String name) {
        return exporter.export(List.of(metric(name))).join(1, TimeUnit.SECONDS);
    }

    public void testFailBufferRecoverAndDrain() throws Exception {
        build(Settings.EMPTY);
        delegate.setShouldFail(true);
        assertTrue("buffered batch must report success", exportAndWait("buf").isSuccess());
        assertBusy(() -> assertThat(countBufferFiles(), greaterThanOrEqualTo(1)));
        assertThat(counter("writes"), hasSize(1));
        assertBusy(() -> {
            List<Measurement> files = gauge("files");
            assertThat(files, not(empty()));
            assertThat(files.getLast().getLong(), greaterThanOrEqualTo(1L));
            List<Measurement> bytes = gauge("bytes");
            assertThat(bytes, not(empty()));
            assertThat(bytes.getLast().getLong(), greaterThanOrEqualTo(1L));
        });

        delegate.setShouldFail(false);
        safeSleep(200); // let the write window (100ms) expire so the reader can force-close and promote the file
        exportAndWait("trigger");

        assertBusy(() -> assertThat(countBufferFiles(), equalTo(0)));
        assertThat(delegate.exportedNames(), hasItems("buf", "trigger"));
        assertBusy(() -> assertThat(counter("replays"), hasSize(1)));
    }

    public void testFailedReplayIsRetried() throws Exception {
        build(Settings.EMPTY);
        delegate.setShouldFail(true);
        exportAndWait("first");
        exportAndWait("second");
        safeSleep(200); // let the write window (100ms) expire so the reader can promote the file
        delegate.clearExported();
        exporter.flush().join(10, TimeUnit.SECONDS);
        assertThat("the drain stops at the failed replay of the first batch", delegate.exportedNames(), contains("first"));

        delegate.setShouldFail(false);
        delegate.clearExported();
        assertBusy(() -> {
            exporter.flush().join(10, TimeUnit.SECONDS);
            assertThat(delegate.exportedNames(), hasItems("first", "second"));
        });
    }

    public void testDiskCapRotatesOldestToMakeRoom() throws Exception {
        build(Settings.builder().put("telemetry.metrics.buffer.disk_size", "1kb").build());

        delegate.setShouldFail(true);
        assertTrue(exportAndWait("first").isSuccess());
        safeSleep(200); // let the write window (100ms) expire so the second batch goes to a new file
        assertTrue(exportAndWait("second").isSuccess());
        assertThat(counter("writes"), hasSize(2));
        assertThat(counter("drops_full"), hasSize(1));
        assertThat(countBufferFiles(), equalTo(1));
    }

    public void testTtlExpiredFilesAreNotReplayed() throws Exception {
        // TTL must be greater than the write window (100ms) so the file is promoted before it expires for reads
        build(Settings.builder().put("telemetry.metrics.buffer.ttl", "300ms").build());

        delegate.setShouldFail(true);
        exportAndWait("expires");
        assertBusy(() -> assertThat(countBufferFiles(), greaterThanOrEqualTo(1)));

        safeSleep(500);

        delegate.clearExported();
        delegate.setShouldFail(false);
        exportAndWait("trigger");
        assertThat(counter("replays"), empty());
        assertThat(delegate.exportedNames(), not(hasItem("expires")));
    }

    public void testShutdownLeavesFilesIfDelegateBroken() throws Exception {
        build(Settings.EMPTY);
        delegate.setShouldFail(true);
        exportAndWait("m1");
        assertBusy(() -> assertThat(countBufferFiles(), greaterThanOrEqualTo(1)));

        exporter.shutdown();
        exporter = null;

        assertThat("buffered file must remain on disk for next startup", countBufferFiles(), greaterThanOrEqualTo(1));
    }

    public void testRestartDrainsRecoveredBufferFiles() throws Exception {
        build(Settings.EMPTY);
        delegate.setShouldFail(true);
        exportAndWait("recovered");
        assertBusy(() -> assertThat(countBufferFiles(), greaterThanOrEqualTo(1)));
        exporter.shutdown();

        // Recreate everything as if there was a restart
        delegate = new FakeMetricExporter();
        meterProvider = new RecordingOtelMeterProvider();
        build(Settings.EMPTY);

        // let the write window (100ms) expire
        safeSleep(200);
        exportAndWait("trigger");

        assertBusy(() -> assertThat(countBufferFiles(), equalTo(0)));
        assertThat(delegate.exportedNames(), hasItems("recovered", "trigger"));
    }

    public void testGaugesRecoverAfterNoopMeterProvider() throws Exception {
        var meterRef = new AtomicReference<>(MeterProvider.noop());
        Settings merged = Settings.builder()
            .put("telemetry.metrics.buffer.disk_size", "10mb")
            .put("telemetry.metrics.buffer.ttl", "5m")
            .build();
        exporter = new BufferingMetricExporter(delegate, merged, bufferDir, meterRef::get, 100);
        meterRef.set(meterProvider);

        delegate.setShouldFail(true);
        assertTrue(exportAndWait("buf").isSuccess());
        assertBusy(() -> assertThat(countBufferFiles(), greaterThanOrEqualTo(1)));
        assertThat(counter("writes"), hasSize(1));
        assertBusy(() -> {
            assertThat(gauge("files"), not(empty()));
            assertThat(gauge("files").getLast().getLong(), greaterThanOrEqualTo(1L));
            assertThat(gauge("bytes"), not(empty()));
            assertThat(gauge("bytes").getLast().getLong(), greaterThanOrEqualTo(1L));
        });
    }

    public void testShutdownDuringDrainBetweenHasNextAndNext() throws Exception {
        // Shutdown closes the storage while a drain is in progress. When that happens between hasNext() and next(), the real
        // disk-buffering iterator reports hasNext() == true and then returns null from next(). The storage is wrapped to pause the
        // drain at exactly that point.
        AtomicBoolean exportedNullBatch = new AtomicBoolean();
        var recordingDelegate = new FakeMetricExporter() {
            @Override
            public CompletableResultCode export(Collection<MetricData> metrics) {
                if (metrics == null) {
                    exportedNullBatch.set(true);
                    return CompletableResultCode.ofSuccess();
                }
                return super.export(metrics);
            }
        };
        var pausingStorage = new AtomicReference<PausingStorage>();
        Settings settings = Settings.builder()
            .put("telemetry.metrics.buffer.disk_size", "10mb")
            .put("telemetry.metrics.buffer.ttl", "5m")
            .build();
        exporter = new BufferingMetricExporter(recordingDelegate, settings, bufferDir, () -> meterProvider, 100, realStorage -> {
            pausingStorage.set(new PausingStorage(realStorage));
            return pausingStorage.get();
        });

        // buffer a batch on disk, and let it become readable
        recordingDelegate.setShouldFail(true);
        assertTrue(exporter.export(List.of(metric("buffered"))).join(1, TimeUnit.SECONDS).isSuccess());
        assertBusy(() -> {
            assertThat(gauge("files"), not(empty()));
            assertThat(gauge("files").getLast().getLong(), greaterThanOrEqualTo(1L));
        });
        safeSleep(200); // let the write window (100ms) expire so the reader can promote the file
        recordingDelegate.setShouldFail(false);

        // start a drain, and shut the exporter down once the drain has seen that there is a batch to replay
        CompletableResultCode flushed = exporter.flush();
        assertTrue("the drain must reach the storage iterator", pausingStorage.get().hasNextReturned.await(10, TimeUnit.SECONDS));
        exporter.shutdown();
        exporter = null; // don't shut it down again in @After
        pausingStorage.get().storageClosed.countDown();
        flushed.join(10, TimeUnit.SECONDS);

        assertFalse("a null batch must not be exported", exportedNullBatch.get());
    }

    private List<Measurement> counter(String suffix) {
        return meterProvider.meter().getRecorder().getMeasurements(LONG_COUNTER, "es.apm.metrics.disk_buffer." + suffix);
    }

    private List<Measurement> gauge(String suffix) {
        return meterProvider.meter().getRecorder().getMeasurements(LONG_GAUGE, "es.apm.metrics.disk_buffer." + suffix);
    }

    private int countBufferFiles() throws Exception {
        if (Files.isDirectory(bufferDir) == false) {
            return 0;
        }
        int n = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(bufferDir)) {
            for (Path ignored : stream) {
                n++;
            }
        }
        return n;
    }

    /**
     * Wraps a real storage so a test can close it between {@code hasNext()} and {@code next()} of the iterator used to drain it.
     */
    private static class PausingStorage implements SignalStorage.Metric {
        private final SignalStorage.Metric storage;
        final CountDownLatch hasNextReturned = new CountDownLatch(1);
        final CountDownLatch storageClosed = new CountDownLatch(1);

        PausingStorage(SignalStorage.Metric storage) {
            this.storage = storage;
        }

        @Override
        public CompletableResultCode write(Collection<MetricData> items) {
            return storage.write(items);
        }

        @Override
        public CompletableResultCode clear() {
            return storage.clear();
        }

        @Override
        public void close() throws IOException {
            storage.close();
        }

        @Override
        public Iterator<Collection<MetricData>> iterator() {
            Iterator<Collection<MetricData>> iterator = storage.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    boolean hasNext = iterator.hasNext();
                    hasNextReturned.countDown();
                    return hasNext;
                }

                @Override
                public Collection<MetricData> next() {
                    try {
                        assertTrue("the storage must be closed", storageClosed.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    return iterator.next();
                }

                @Override
                public void remove() {
                    iterator.remove();
                }
            };
        }
    }

    private static MetricData metric(String name) {
        return ImmutableMetricData.createLongGauge(
            Resource.getDefault(),
            InstrumentationScopeInfo.create("test"),
            name,
            "test metric",
            "1",
            ImmutableGaugeData.create(List.of(ImmutableLongPointData.create(0L, 0L, Attributes.empty(), 1L)))
        );
    }

}
