/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;

public class BlobStoreCacheDirectoryMetricsTests extends AbstractWireSerializingTestCase<BlobStoreCacheDirectoryMetrics> {

    @Override
    protected Writeable.Reader<BlobStoreCacheDirectoryMetrics> instanceReader() {
        return BlobStoreCacheDirectoryMetrics::new;
    }

    @Override
    protected BlobStoreCacheDirectoryMetrics createTestInstance() {
        BlobStoreCacheDirectoryMetrics metrics = new BlobStoreCacheDirectoryMetrics();
        if (randomBoolean()) {
            metrics.add(randomNonNegativeLong(), randomNonNegativeLong());
        }
        if (randomBoolean()) {
            metrics.add(randomNonNegativeLong(), randomNonNegativeLong());
        }
        return metrics;
    }

    @Override
    protected BlobStoreCacheDirectoryMetrics mutateInstance(BlobStoreCacheDirectoryMetrics instance) {
        BlobStoreCacheDirectoryMetrics mutated = instance.copy();
        mutated.add(randomNonNegativeLong(), randomNonNegativeLong());
        return mutated;
    }

    public void testEntriesEmptyWhenNoCacheActivity() {
        assertThat(new BlobStoreCacheDirectoryMetrics().entries(), anEmptyMap());
    }

    public void testEntriesAfterAdd() {
        BlobStoreCacheDirectoryMetrics metrics = new BlobStoreCacheDirectoryMetrics();
        metrics.add(100, 200);
        metrics.add(50, 300);

        Map<String, String> entries = metrics.entries();
        assertThat(entries, hasEntry(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER, "150"));
    }

    public void testMerge() {
        BlobStoreCacheDirectoryMetrics a = new BlobStoreCacheDirectoryMetrics();
        a.add(100, 200);
        BlobStoreCacheDirectoryMetrics b = new BlobStoreCacheDirectoryMetrics();
        b.add(50, 300);

        BlobStoreCacheDirectoryMetrics merged = a.merge(b);
        assertEquals(150, merged.getWaitTime());
        assertEquals(2, merged.getWaits());
        assertEquals(500, merged.getWaitBytes());
    }

    public void testMergeEntries() {
        BlobStoreCacheDirectoryMetrics a = new BlobStoreCacheDirectoryMetrics();
        a.add(100, 200);
        BlobStoreCacheDirectoryMetrics b = new BlobStoreCacheDirectoryMetrics();
        b.add(50, 300);

        Map<String, String> entries = a.merge(b).entries();
        assertThat(entries, hasEntry(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER, "150"));
    }

    public void testCopy() {
        BlobStoreCacheDirectoryMetrics original = new BlobStoreCacheDirectoryMetrics();
        original.add(42, 84);
        BlobStoreCacheDirectoryMetrics copy = original.copy();
        assertEquals(original.getWaitTime(), copy.getWaitTime());
        assertEquals(original.getWaits(), copy.getWaits());
        assertEquals(original.getWaitBytes(), copy.getWaitBytes());
    }

    public void testDelta() {
        BlobStoreCacheDirectoryMetrics metrics = new BlobStoreCacheDirectoryMetrics();
        var delta = metrics.delta();
        metrics.add(50, 100);
        BlobStoreCacheDirectoryMetrics result = delta.get();
        assertEquals(50, result.getWaitTime());
        assertEquals(1, result.getWaits());
        assertEquals(100, result.getWaitBytes());
        assertThat(result.entries(), hasEntry(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER, "50"));
    }
}
