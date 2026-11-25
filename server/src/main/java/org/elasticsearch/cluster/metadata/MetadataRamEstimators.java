/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Shared helpers for {@link Accountable#ramBytesUsed()} estimates on cluster metadata types.
 */
final class MetadataRamEstimators {

    private MetadataRamEstimators() {}

    /**
     * Sizes a map whose values are {@link Accountable}. {@link ImmutableOpenMap} instances are sized via their own
     * {@link ImmutableOpenMap#ramBytesUsed()} (open-hash keys/values arrays). Other map implementations fall back to
     * summing each entry's shallow size, key string, and value {@link Accountable#ramBytesUsed()}.
     */
    static <T extends Accountable> long ramBytesUsedByAccountableMap(Map<String, T> map) {
        if (map instanceof ImmutableOpenMap<String, T> immutableOpenMap) {
            return immutableOpenMap.ramBytesUsed();
        }
        long size = RamUsageEstimator.shallowSizeOf(map);
        long entryShallowSize = -1L;
        for (Map.Entry<String, T> entry : map.entrySet()) {
            if (entryShallowSize == -1L) {
                entryShallowSize = RamUsageEstimator.shallowSizeOf(entry);
            }
            size += entryShallowSize + RamUsageEstimator.sizeOf(entry.getKey()) + entry.getValue().ramBytesUsed();
        }
        return RamUsageEstimator.alignObjectSize(size);
    }

    /**
     * Sizes a project index map like {@link ImmutableOpenMap#ramBytesUsed(java.util.function.ToLongFunction)}, but counts
     * each shared {@link MappingMetadata} instance only once across indices.
     */
    static long ramBytesUsedByIndexMetadataMap(ImmutableOpenMap<String, IndexMetadata> indices) {
        Set<MappingMetadata> countedMappings = Collections.newSetFromMap(new IdentityHashMap<>());
        return indices.ramBytesUsed(indexMetadata -> {
            long bytes = indexMetadata.ramBytesUsed();
            MappingMetadata mapping = indexMetadata.mapping();
            if (mapping != null && countedMappings.add(mapping) == false) {
                bytes -= mapping.ramBytesUsed();
            }
            return bytes;
        });
    }
}
