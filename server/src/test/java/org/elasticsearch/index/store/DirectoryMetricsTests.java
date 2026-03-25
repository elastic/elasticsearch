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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

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
}
