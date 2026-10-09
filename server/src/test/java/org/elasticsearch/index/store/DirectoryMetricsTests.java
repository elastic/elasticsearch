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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

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

        storeMetrics.addBytesRead(100);

        DirectoryMetrics resolved = resolveDelta(delta, storeMetrics, 50);
        StoreMetrics result = resolved.metrics(StoreMetrics.NAME).cast(StoreMetrics.class);
        assertEquals(150, result.getBytesRead());
    }

    public void testCaptureResolvedOnDifferentThread() throws Exception {
        StoreMetrics storeMetrics = new StoreMetrics();
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(StoreMetrics.NAME, storeMetrics);
        Supplier<DirectoryMetrics> delta = builder.build().delta();

        storeMetrics.addBytesRead(200);

        AtomicReference<DirectoryMetrics> resolvedRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Thread resolver = new Thread(() -> {
            resolvedRef.set(resolveDelta(delta, storeMetrics, 75));
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

        storeMetrics.addBytesRead(42);

        DirectoryMetrics resolved = resolveDelta(delta, storeMetrics, 0);
        StoreMetrics result = resolved.metrics(StoreMetrics.NAME).cast(StoreMetrics.class);
        assertEquals(42, result.getBytesRead());
    }

    public void testSerializationWithSameNamedWriteableThrowsException() throws IOException {
        expectThrows(
            IllegalArgumentException.class,
            () -> new NamedWriteableRegistry(
                List.of(
                    new NamedWriteableRegistry.Entry(DirectoryMetrics.PluggableMetrics.class, Counter.NAME, Counter::new),
                    new NamedWriteableRegistry.Entry(DirectoryMetrics.PluggableMetrics.class, Counter2.NAME, Counter2::new)
                )
            )
        );
    }

    public void testEntriesLastWriteWinsForSameKey() {
        String key = "key";

        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add("metric_a", new MetricWithEntry("metric_a", key, "value_a"));
        builder.add("metric_b", new MetricWithEntry("metric_b", key, "value_b"));
        DirectoryMetrics metrics = builder.build();

        Map<String, String> entries = metrics.entries();
        assertThat(entries.size(), equalTo(1));
        assertThat(entries, hasKey(key));
        String value = entries.get(key);
        assertThat(value, anyOf(equalTo("value_a"), equalTo("value_b")));
    }

    public static class Counter implements DirectoryMetrics.PluggableMetrics<Counter> {
        public static final String NAME = "counter";
        private int count;

        public Counter() {}

        public Counter(int count) {
            this.count = count;
        }

        public Counter(StreamInput in) throws IOException {
            this.count = in.readVInt();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(count);
        }

        public void increment() {
            count++;
        }

        public long getCount() {
            return count;
        }

        @Override
        public Supplier<Counter> delta() {
            Counter snapshot = copy();
            return () -> new Counter(count - snapshot.count);
        }

        @Override
        public Counter copy() {
            return new Counter(count);
        }

        @Override
        public Counter merge(Counter other) {
            return new Counter(count + other.count);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("count", count);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Counter that = (Counter) o;
            return count == that.count;
        }

        @Override
        public int hashCode() {
            return Objects.hash(count);
        }
    }

    public static final class Counter2 extends Counter {
        public Counter2(int count) {
            super(count);
        }

        public Counter2(StreamInput in) throws IOException {
            super(in);
        }
    }

    private static class MetricWithEntry extends Counter {
        private final String name;
        private final String entryKey;
        private final String entryValue;

        MetricWithEntry(String name, String entryKey, String entryValue) {
            super(0);
            this.name = name;
            this.entryKey = entryKey;
            this.entryValue = entryValue;
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public Map<String, String> entries() {
            return Map.of(entryKey, entryValue);
        }
    }

    public static <M extends DirectoryMetrics.PluggableMetrics<M>> DirectoryMetrics.Capture metricsCapture(
        String name,
        Supplier<M> instanceSupplier
    ) {
        return () -> {
            DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
            builder.add(name, instanceSupplier.get());
            return builder.build().delta();
        };
    }

    public static long storeBytesRead(DirectoryMetrics metrics) {
        var metric = metrics.metrics(StoreMetrics.NAME);
        return metric == null ? 0L : metric.cast(StoreMetrics.class).getBytesRead();
    }

    private static DirectoryMetrics resolveDelta(Supplier<DirectoryMetrics> delta, StoreMetrics storeMetrics, long workerBytesRead) {
        storeMetrics.addBytesRead(workerBytesRead);
        return delta.get();
    }
}
