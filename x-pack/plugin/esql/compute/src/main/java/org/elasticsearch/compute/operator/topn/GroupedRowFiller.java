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

    GroupedRowFiller(List<ElementType> elementTypes, List<TopNEncoder> encoders, List<TopNOperator.SortOrder> sortOrders, Page page) {
        this.ungroupedRowFiller = new UngroupedRowFiller(elementTypes, encoders, sortOrders, page);
    }

    @Override
    public void writeKey(int i, Row row) {
        ungroupedRowFiller.writeKey(i, row);
        ((GroupedRow) row).writeGroupKey(i, row);
    }

    @Override
    public void writeValues(int i, Row row) {
        ungroupedRowFiller.writeValues(i, row);
    }
}
