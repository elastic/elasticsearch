/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.List;

final class UngroupedRowFiller implements RowFiller {
    private final ValueExtractor[] valueExtractors;
    private final KeyFactory[] keyFactories;

    private int keyPreAllocSize = 0;
    private int valuePreAllocSize = 0;

    record KeyFactory(KeyExtractor extractor, boolean ascending) {}

    UngroupedRowFiller(List<ElementType> elementTypes, List<TopNEncoder> encoders, List<TopNOperator.SortOrder> sortOrders, Page page) {
        valueExtractors = new ValueExtractor[page.getBlockCount()];
        for (int b = 0; b < valueExtractors.length; b++) {
            valueExtractors[b] = ValueExtractor.extractorFor(
                elementTypes.get(b),
                encoders.get(b).toUnsortable(),
                TopNOperator.channelInKey(sortOrders, b),
                page.getBlock(b)
            );
        }
        keyFactories = new KeyFactory[sortOrders.size()];
        for (int k = 0; k < keyFactories.length; k++) {
            TopNOperator.SortOrder so = sortOrders.get(k);
            KeyExtractor extractor = KeyExtractor.extractorFor(
                elementTypes.get(so.channel()),
                encoders.get(so.channel()).toSortable(),
                so.asc(),
                so.nul(),
                so.nonNul(),
                page.getBlock(so.channel())
            );
            keyFactories[k] = new KeyFactory(extractor, so.asc());
        }
    }

    int preAllocatedKeysSize() {
        return keyPreAllocSize;
    }

    int preAlocatedValueSize() {
        return valuePreAllocSize;
    }

    @Override
    public void writeKey(int position, Row row) {
        int orderByCompositeKeyCurrentPosition = 0;
        for (int i = 0; i < keyFactories.length; i++) {
            int valueAsBytesSize = keyFactories[i].extractor().writeKey(row.keys(), position);
            if (valueAsBytesSize < 0) {
                throw new IllegalStateException("empty keys to allowed. " + valueAsBytesSize + " must be > 0");
            }
            orderByCompositeKeyCurrentPosition += valueAsBytesSize;
            row.bytesOrder().endOffsets[i] = orderByCompositeKeyCurrentPosition - 1;
        }
        keyPreAllocSize = RowFiller.newPreAllocSize(row.keys(), keyPreAllocSize);
    }

    @Override
    public void writeValues(int position, Row destination) {
        for (ValueExtractor e : valueExtractors) {
            var refCounted = e.getRefCountedForShard(position);
            if (refCounted != null) {
                destination.setShardRefCounted(refCounted);
            }
            e.writeValue(destination.values(), position);
        }
        valuePreAllocSize = RowFiller.newPreAllocSize(destination.values(), valuePreAllocSize);
    }
}
