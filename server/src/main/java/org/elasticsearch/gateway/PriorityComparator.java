/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.Index;

import java.util.Comparator;
import java.util.function.Function;

/// A helper class for prioritizing [IndexMetadata] or [ShardRouting] instances.
public final class PriorityComparator {

    private PriorityComparator() {} // static utility class, should not be instantiated

    /// Returns a comparator for [IndexMetadata] instances that orders instances as follows:
    ///
    /// 1. First, system indices are ordered before non-system indices.
    /// 2. Then indices are ordered by their priority (`index.priority`), in descending order.
    /// 3. Then newer indices are ordered before older indices, based on their creation date (`index.creation_date`). This benefits
    /// time-series indices, where newer indices are considered more urgent
    /// 4. Lastly the index names are compared, which is useful when a date is baked into the index name, e.g. `logstash-2015.05.03`.
    public static Comparator<IndexMetadata> getIndexMetadataComparator() {
        return PriorityComparator::compareIndexMetadata;
    }

    /// Returns a comparator for [ShardRouting] instances that orders instances using the same rules as [#getIndexMetadataComparator()],
    /// using the given [RoutingAllocation] to provide the [IndexMetadata].
    public static Comparator<ShardRouting> getAllocationComparator(RoutingAllocation allocation) {
        return getShardRoutingComparator(index -> allocation.metadata().indexMetadata(index));
    }

    // visible for testing
    /// Returns a comparator for [ShardRouting] instances that orders instances using the same rules as [#getIndexMetadataComparator()],
    /// using the given [Function] to provide the [IndexMetadata].
    static Comparator<ShardRouting> getShardRoutingComparator(Function<Index, IndexMetadata> metadataMapper) {
        return (shard1, shard2) -> compareShardRoutings(metadataMapper, shard1, shard2);
    }

    private static int compareIndexMetadata(IndexMetadata metadata1, IndexMetadata metadata2) {
        int cmp = Boolean.compare(metadata2.isSystem(), metadata1.isSystem());
        if (cmp == 0) {
            cmp = Long.compare(metadata2.priority(), metadata1.priority());
            if (cmp == 0) {
                cmp = Long.compare(metadata2.getCreationDate(), metadata1.getCreationDate());
                if (cmp == 0) {
                    cmp = metadata2.getIndex().getName().compareTo(metadata1.getIndex().getName());
                }
            }
        }
        return cmp;
    }

    private static int compareShardRoutings(Function<Index, IndexMetadata> metadataMapper, ShardRouting shard1, ShardRouting shard2) {
        final Index o1Index = shard1.index();
        final Index o2Index = shard2.index();
        if (o1Index.equals(o2Index)) {
            return 0;
        }
        return compareIndexMetadata(metadataMapper.apply(o1Index), metadataMapper.apply(o2Index));
    }
}
