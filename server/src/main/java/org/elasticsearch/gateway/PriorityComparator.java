/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.Index;

import java.util.Comparator;

/**
 * A comparator that compares {@link ShardRouting} instances based on various properties. Instances
 * are ordered as follows.
 * <ol>
 *     <li>First, system indices are ordered before non-system indices</li>
 *     <li>Then indices are ordered by their priority, in descending order (index.priority)</li>
 *     <li>Then newer indices are ordered before older indices, based on their creation date. This benefits
 *         time-series indices, where newer indices are considered more urgent (index.creation_date)</li>
 *     <li>Lastly the index names are compared, which is useful when a date is baked into the index
 *         name, e.g. <code>logstash-2015.05.03</code></li>
 * </ol>
 */
public abstract class PriorityComparator implements Comparator<ShardRouting> {

    @Override
    public final int compare(ShardRouting o1, ShardRouting o2) {
        final Index o1Index = o1.index();
        final Index o2Index = o2.index();
        int cmp = 0;
        if (o1Index.equals(o2Index) == false) {
            final IndexMetadata metadata01 = getMetadata(o1Index);
            final IndexMetadata metadata02 = getMetadata(o2Index);
            cmp = Boolean.compare(metadata02.isSystem(), metadata01.isSystem());

            if (cmp == 0) {
                cmp = Long.compare(metadata02.priority(), metadata01.priority());

                if (cmp == 0) {
                    cmp = Long.compare(metadata02.getCreationDate(), metadata01.getCreationDate());
                    if (cmp == 0) {
                        cmp = o2Index.getName().compareTo(o1Index.getName());
                    }
                }
            }
        }
        return cmp;
    }

    protected abstract IndexMetadata getMetadata(Index index);

    /**
     * Returns a PriorityComparator that uses the RoutingAllocation index metadata to access the index setting per index.
     */
    public static PriorityComparator getAllocationComparator(final RoutingAllocation allocation) {
        return new PriorityComparator() {
            @Override
            protected IndexMetadata getMetadata(Index index) {
                return allocation.metadata().getIndexSafe(index);
            }
        };
    }
}
