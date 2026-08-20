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
     * Sizes a map whose values are {@link Accountable}, summing the map's shallow size, each entry's shallow size and key string, and each
     * value's recursive {@link Accountable#ramBytesUsed()}. Use this when you want compile-time enforcement that the map's values are
     * {@link Accountable}.
     */
    static <T extends Accountable> long ramBytesUsedByAccountableMap(Map<String, T> map) {
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
     * Sizes a project or cluster index map like {@link #ramBytesUsedByAccountableMap}, but counts each shared
     * {@link MappingMetadata} instance only once across indices.
     */
    static long ramBytesUsedByIndexMetadataMap(Map<String, IndexMetadata> indices) {
        long size = RamUsageEstimator.shallowSizeOf(indices);
        long entryShallowSize = -1L;
        Set<MappingMetadata> countedMappings = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Map.Entry<String, IndexMetadata> entry : indices.entrySet()) {
            if (entryShallowSize == -1L) {
                entryShallowSize = RamUsageEstimator.shallowSizeOf(entry);
            }
            size += entryShallowSize + RamUsageEstimator.sizeOf(entry.getKey()) + entry.getValue().ramBytesUsed();
            MappingMetadata mapping = entry.getValue().mapping();
            if (mapping != null && countedMappings.add(mapping) == false) {
                size -= mapping.ramBytesUsed();
            }
        }
        return RamUsageEstimator.alignObjectSize(size);
    }
}
