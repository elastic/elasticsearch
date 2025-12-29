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
    private final KeyExtractor[] groupKeyExtractors;

    GroupedRowFiller(List<ElementType> elementTypes, List<TopNEncoder> encoders, List<TopNOperator.SortOrder> sortOrders, List<Integer> groupChannels, Page page) {
        this.ungroupedRowFiller = new UngroupedRowFiller(elementTypes, encoders, sortOrders, page);
        this.groupKeyExtractors = new KeyExtractor[groupChannels.size()];
        for (int k = 0; k < groupKeyExtractors.length; k++) {
            int channel = groupChannels.get(k);
            groupKeyExtractors[k] = KeyExtractor.extractorFor(
                elementTypes.get(channel),
                encoders.get(channel).toSortable(),
                true,
                (byte) 0,
                (byte) 1,
                page.getBlock(channel)
            );
        }
    }

    @Override
    public void writeKey(int i, Row row) {
        ungroupedRowFiller.writeKey(i, row);
        for (KeyExtractor extractor : groupKeyExtractors) {
            extractor.writeKey(((GroupedRow) row).groupKey(), i);
        }
    }

    @Override
    public void writeValues(int i, Row row) {
        ungroupedRowFiller.writeValues(i, row);
    }
}
