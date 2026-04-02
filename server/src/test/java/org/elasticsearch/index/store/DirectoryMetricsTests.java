/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class DirectoryMetricsTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(DirectoryMetrics.PluggableMetrics.class, StoreMetrics.NAME, StoreMetrics::new))
    );

    private StreamInput wrapInput(BytesStreamOutput out) throws IOException {
        return new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
    }

    public void testSerializationRoundTrip() throws IOException {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add("store", new StoreMetrics(42));
        DirectoryMetrics original = builder.build();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        DirectoryMetrics deserialized = new DirectoryMetrics(wrapInput(out));

        StoreMetrics originalStore = original.metrics("store").cast(StoreMetrics.class);
        StoreMetrics deserializedStore = deserialized.metrics("store").cast(StoreMetrics.class);
        assertEquals(originalStore.getBytesRead(), deserializedStore.getBytesRead());
    }

    public void testSerializationEmpty() throws IOException {
        DirectoryMetrics original = DirectoryMetrics.EMPTY;

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        DirectoryMetrics deserialized = new DirectoryMetrics(wrapInput(out));

        assertNull(deserialized.metrics("store"));
    }

    public void testMerge() {
        DirectoryMetrics.Builder builder1 = new DirectoryMetrics.Builder();
        builder1.add("store", new StoreMetrics(100));
        DirectoryMetrics metrics1 = builder1.build();

        DirectoryMetrics.Builder builder2 = new DirectoryMetrics.Builder();
        builder2.add("store", new StoreMetrics(200));
        DirectoryMetrics metrics2 = builder2.build();

        DirectoryMetrics merged = metrics1.merge(metrics2);
        StoreMetrics mergedStore = merged.metrics("store").cast(StoreMetrics.class);
        assertEquals(300, mergedStore.getBytesRead());
    }

    public void testMergeDisjointKeys() {
        DirectoryMetrics.Builder builder1 = new DirectoryMetrics.Builder();
        builder1.add("store", new StoreMetrics(100));
        DirectoryMetrics metrics1 = builder1.build();

        DirectoryMetrics merged = metrics1.merge(DirectoryMetrics.EMPTY);
        StoreMetrics mergedStore = merged.metrics("store").cast(StoreMetrics.class);
        assertEquals(100, mergedStore.getBytesRead());
    }

    public void testMergeWithEmpty() {
        DirectoryMetrics merged = DirectoryMetrics.EMPTY.merge(DirectoryMetrics.EMPTY);
        assertNull(merged.metrics("store"));
    }

    public void testCaptureResolvedOnSameThread() {
        StoreMetrics storeMetrics = new StoreMetrics();
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(StoreMetrics.NAME, storeMetrics);
        Supplier<DirectoryMetrics> delta = builder.build().delta();

        var capture = new IndicesService.DirectoryMetricsCapture(delta, storeMetrics);

        storeMetrics.addBytesRead(100);

        DirectoryMetrics resolved = resolveCapture(capture, 50);
        StoreMetrics result = resolved.metrics(StoreMetrics.NAME).cast(StoreMetrics.class);
        assertEquals(150, result.getBytesRead());
    }

    public void testCaptureResolvedOnDifferentThread() throws Exception {
        StoreMetrics storeMetrics = new StoreMetrics();
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(StoreMetrics.NAME, storeMetrics);
        Supplier<DirectoryMetrics> delta = builder.build().delta();

        var capture = new IndicesService.DirectoryMetricsCapture(delta, storeMetrics);

        storeMetrics.addBytesRead(200);

        AtomicReference<DirectoryMetrics> resolvedRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Thread resolver = new Thread(() -> {
            resolvedRef.set(resolveCapture(capture, 75));
            latch.countDown();
        });
        resolver.start();
        latch.await();

        DirectoryMetrics resolved = resolvedRef.get();
        assertNotNull(resolved);
        StoreMetrics result = resolved.metrics(StoreMetrics.NAME).cast(StoreMetrics.class);
        assertEquals(275, result.getBytesRead());
    }

    public void testCaptureWithZeroWorkerBytes() {
        StoreMetrics storeMetrics = new StoreMetrics();
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(StoreMetrics.NAME, storeMetrics);
        Supplier<DirectoryMetrics> delta = builder.build().delta();

        var capture = new IndicesService.DirectoryMetricsCapture(delta, storeMetrics);

        storeMetrics.addBytesRead(42);

        DirectoryMetrics resolved = resolveCapture(capture, 0);
        StoreMetrics result = resolved.metrics(StoreMetrics.NAME).cast(StoreMetrics.class);
        assertEquals(42, result.getBytesRead());
    }

    private static DirectoryMetrics resolveCapture(IndicesService.DirectoryMetricsCapture capture, long workerBytesRead) {
        capture.callingThreadStoreMetrics().addBytesRead(workerBytesRead);
        return capture.delta().get();
    }
}
