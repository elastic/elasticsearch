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

final class GroupedRowFiller implements RowFiller {
    private final UngroupedRowFiller ungroupedRowFiller;
    private final ValueExtractor[] valueExtractors;

    private int keyPreAllocSize = 0;
    private int valuePreAllocSize = 0;
    private int groupKeyPreAllocSize = 0;

    GroupedRowFiller(
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<TopNOperator.SortOrder> sortOrders,
        List<Integer> groupChannels,
        Page page
    ) {
        this.ungroupedRowFiller = new UngroupedRowFiller(elementTypes, encoders, sortOrders, page);
        this.valueExtractors = new ValueExtractor[groupChannels.size()];
        for (int k = 0; k < valueExtractors.length; k++) {
            int channel = groupChannels.get(k);
            valueExtractors[k] = ValueExtractor.extractorFor(
                elementTypes.get(channel),
                encoders.get(channel).toUnsortable(),
                false,
                page.getBlock(channel)
            );
        }
    }

    int preAllocatedGroupKeySize() {
        return groupKeyPreAllocSize;
    }

    int preAllocatedValueSize() {
        return valuePreAllocSize;
    }

    int preAllocatedKeysSize() {
        return keyPreAllocSize;
    }

    @Override
    public void writeKey(int i, Row row) {
        ungroupedRowFiller.writeKey(i, row);
        GroupedRow groupedRow = (GroupedRow) row;
        for (ValueExtractor extractor : valueExtractors) {
            extractor.writeValue(groupedRow.groupKey(), i);
        }
        keyPreAllocSize = RowFiller.newPreAllocSize(row.keys(), keyPreAllocSize);
        groupKeyPreAllocSize = RowFiller.newPreAllocSize(groupedRow.groupKey(), groupKeyPreAllocSize);
    }

    @Override
    public void writeValues(int i, Row row) {
        ungroupedRowFiller.writeValues(i, row);
        valuePreAllocSize = RowFiller.newPreAllocSize(row.values(), keyPreAllocSize);
    }
}
