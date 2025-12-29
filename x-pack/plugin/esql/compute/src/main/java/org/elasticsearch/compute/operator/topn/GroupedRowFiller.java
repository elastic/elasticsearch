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
    private final KeyExtractor groupKeyExtractor;

    GroupedRowFiller(List<ElementType> elementTypes, List<TopNEncoder> encoders, List<TopNOperator.SortOrder> sortOrders, Page page) {
        this.ungroupedRowFiller = new UngroupedRowFiller(elementTypes, encoders, sortOrders, page);
        this.groupKeyExtractor = KeyExtractor.extractorFor(
            elementTypes.get(0),
            encoders.get(0).toSortable(),
            true,
            (byte) 0,
            (byte) 1,
            page.getBlock(0)
        );
    }

    @Override
    public void writeKey(int i, Row row) {
        ungroupedRowFiller.writeKey(i, row);
        groupKeyExtractor.writeKey(((GroupedRow) row).groupKey(), i);
    }

    @Override
    public void writeValues(int i, Row row) {
        ungroupedRowFiller.writeValues(i, row);
    }
}
