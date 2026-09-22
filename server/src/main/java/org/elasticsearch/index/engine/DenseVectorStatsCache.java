/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.DenseVectorStats;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Caches the per-field vector counts of a segment, keyed on its core cache key so entries survive a refresh and are
 * dropped when the segment closes. Reading a count opens the field's vector values, which for a sparse field builds an
 * {@link IndexedDISI} and prefetches; off-heap sizes come from field metadata, so they are neither cached nor skipped.
 */
final class DenseVectorStatsCache {

    private final Map<IndexReader.CacheKey, Map<String, Long>> perSegment = ConcurrentCollections.newConcurrentMap();

    /**
     * Returns what {@code leafReader} contributes to the dense vector stats for the given fields. When
     * {@code includeCounts} is false the vector values are never opened, and the returned count is zero.
     */
    DenseVectorStats get(LeafReader leafReader, Iterable<String> fieldNames, boolean includeCounts) throws IOException {
        final Map<String, Long> cachedCounts = includeCounts ? cacheFor(leafReader) : null;
        long count = 0;
        final Map<String, Map<String, Long>> offHeapStats = new HashMap<>();
        for (String fieldName : fieldNames) {
            final FieldInfo info = leafReader.getFieldInfos().fieldInfo(fieldName);
            if (info == null || info.getVectorDimension() <= 0) {
                continue;
            }
            if (includeCounts) {
                Long fieldCount = cachedCounts == null ? null : cachedCounts.get(fieldName);
                if (fieldCount == null) {
                    fieldCount = countVectors(leafReader, info);
                    if (cachedCounts != null) {
                        cachedCounts.put(fieldName, fieldCount);
                    }
                }
                count += fieldCount;
            }
            offHeapStats.put(fieldName, offHeapByteSize(leafReader, info));
        }
        return new DenseVectorStats(count, Collections.unmodifiableMap(offHeapStats));
    }

    /**
     * Returns the per-field map for this segment, or {@code null} if it cannot be cached, in which case the caller
     * recomputes.
     */
    private Map<String, Long> cacheFor(LeafReader leafReader) {
        final IndexReader.CacheHelper cacheHelper = leafReader.getCoreCacheHelper();
        if (cacheHelper == null) {
            return null;
        }
        final IndexReader.CacheKey cacheKey = cacheHelper.getKey();
        final Map<String, Long> existing = perSegment.get(cacheKey);
        if (existing != null) {
            return existing;
        }
        final Map<String, Long> created = ConcurrentCollections.newConcurrentMap();
        final Map<String, Long> witness = perSegment.putIfAbsent(cacheKey, created);
        if (witness != null) {
            return witness;
        }
        try {
            cacheHelper.addClosedListener(perSegment::remove);
        } catch (AlreadyClosedException e) {
            // no listener will fire to drop this entry, so drop it now
            perSegment.remove(cacheKey, created);
            return null;
        }
        return created;
    }

    private static long countVectors(LeafReader leafReader, FieldInfo info) throws IOException {
        return switch (info.getVectorEncoding()) {
            case FLOAT32 -> {
                FloatVectorValues values = leafReader.getFloatVectorValues(info.name);
                yield values != null ? values.size() : 0;
            }
            case BYTE -> {
                ByteVectorValues values = leafReader.getByteVectorValues(info.name);
                yield values != null ? values.size() : 0;
            }
        };
    }

    private static Map<String, Long> offHeapByteSize(LeafReader leafReader, FieldInfo info) throws IOException {
        final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
        KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
            vectorsReader = fieldsReader.getFieldReader(info.name);
        }
        return vectorsReader.getOffHeapByteSize(info);
    }

    // visible for testing
    int cachedSegments() {
        return perSegment.size();
    }
}
