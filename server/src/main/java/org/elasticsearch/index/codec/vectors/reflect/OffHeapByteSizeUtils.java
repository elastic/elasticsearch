/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.reflect;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader;
import org.apache.lucene.index.FieldInfo;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Static utility methods to help retrieve desired off-heap vector index size.
 * Remove once KnnVectorsReaders::getOffHeapByteSize is available.
 */
public class OffHeapByteSizeUtils {

    private OffHeapByteSizeUtils() {} // no instances

    public static Map<String, Long> getOffHeapByteSize(KnnVectorsReader reader, FieldInfo fieldInfo) {
        reader = AssertingKnnVectorsReaderReflect.unwrapAssertingReader(reader);
        switch (reader) {
            case OffHeapStats offHeapStats -> {
                return offHeapStats.getOffHeapByteSize(fieldInfo);
            }
            case Lucene99HnswVectorsReader hnswVectorsReader -> {
                var graph = OffHeapReflectionUtils.getOffHeapByteSizeL99HNSW(hnswVectorsReader, fieldInfo);
                var flat = getOffHeapByteSize(OffHeapReflectionUtils.getFlatVectorsReaderL99HNSW(hnswVectorsReader), fieldInfo);
                return mergeOffHeapByteSizeMaps(graph, flat);
            }
            case Lucene99ScalarQuantizedVectorsReader scalarQuantizedVectorsReader -> {
                var quant = OffHeapReflectionUtils.getOffHeapByteSizeSQ(scalarQuantizedVectorsReader, fieldInfo);
                var raw = getOffHeapByteSize(OffHeapReflectionUtils.getFlatVectorsReaderSQ(scalarQuantizedVectorsReader), fieldInfo);
                return mergeOffHeapByteSizeMaps(quant, raw);
            }
            case Lucene99FlatVectorsReader flatVectorsReader -> {
                return OffHeapReflectionUtils.getOffHeapByteSizeF99FLT(flatVectorsReader, fieldInfo);
            }
            case null, default -> throw new AssertionError("unexpected reader:" + reader);
        }
    }

    /**
     * Merges the Maps returned by getOffHeapByteSize(FieldInfo).
     *
     * <p>This method is a convenience for aggregating the desired off-heap memory requirements for
     * several fields. The keys in the returned map are a union of the keys in the given maps. Entries
     * with the same key are summed.
     */
    public static Map<String, Long> mergeOffHeapByteSizeMaps(Map<String, Long> map1, Map<String, Long> map2) {
        return Stream.of(map1, map2)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
    }
}
