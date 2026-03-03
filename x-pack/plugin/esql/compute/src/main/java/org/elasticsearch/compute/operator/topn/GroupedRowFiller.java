/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.List;

/**
 * Fills {@link GroupedRow}s from page data for grouped top-N. Handles both sort-key encoding
 * and value extraction. The group ID is set directly by the caller from the BlockHash callback.
 */
final class GroupedRowFiller {
    private final ValueExtractor[] valueExtractors;
    private final KeyExtractor[] sortKeyExtractors;

    private int keyPreAllocSize = 0;
    private int valuePreAllocSize = 0;

    GroupedRowFiller(
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<TopNOperator.SortOrder> sortOrders,
        boolean[] channelInKey,
        Page page
    ) {
        valueExtractors = new ValueExtractor[page.getBlockCount()];
        for (int b = 0; b < valueExtractors.length; b++) {
            valueExtractors[b] = ValueExtractor.extractorFor(
                elementTypes.get(b),
                encoders.get(b).toUnsortable(),
                channelInKey[b],
                page.getBlock(b)
            );
        }
        sortKeyExtractors = new KeyExtractor[sortOrders.size()];
        for (int k = 0; k < sortKeyExtractors.length; k++) {
            TopNOperator.SortOrder so = sortOrders.get(k);
            sortKeyExtractors[k] = KeyExtractor.extractorFor(
                elementTypes.get(so.channel()),
                encoders.get(so.channel()),
                so.asc(),
                so.nul(),
                so.nonNul(),
                page.getBlock(so.channel())
            );
        }
    }

    int preAllocatedKeysSize() {
        return keyPreAllocSize;
    }

    int preAllocatedValueSize() {
        return valuePreAllocSize;
    }

    void writeSortKey(int position, GroupedRow row) {
        for (KeyExtractor keyExtractor : sortKeyExtractors) {
            keyExtractor.writeKey(row.keys(), position);
        }
        keyPreAllocSize = newPreAllocSize(row.keys(), keyPreAllocSize);
    }

    void writeValues(int position, GroupedRow row) {
        for (ValueExtractor e : valueExtractors) {
            var refCounted = e.getRefCountedForShard(position);
            if (refCounted != null) {
                row.setShardRefCounted(refCounted);
            }
            e.writeValue(row.values(), position);
        }
        valuePreAllocSize = newPreAllocSize(row.values(), valuePreAllocSize);
    }

    /**
     * Pre-allocation size heuristic: use the larger of the current builder length and half
     * the previous pre-alloc size, so the size decays after a single unusually large row.
     */
    private static int newPreAllocSize(BreakingBytesRefBuilder builder, int sparePreAllocSize) {
        return Math.max(builder.length(), sparePreAllocSize / 2);
    }
}
