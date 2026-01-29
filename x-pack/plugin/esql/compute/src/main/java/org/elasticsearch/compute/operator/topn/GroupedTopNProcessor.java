/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.List;

class GroupedTopNProcessor implements TopNProcessor {
    private final int[] groupChannels;

    GroupedTopNProcessor(int[] groupChannels) {
        this.groupChannels = groupChannels;
    }

    @Override
    public RowFiller rowFiller(
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<TopNOperator.SortOrder> sortOrders,
        Page page
    ) {
        return new GroupedRowFiller(elementTypes, encoders, sortOrders, groupChannels, page);
    }

    @Override
    public Row row(CircuitBreaker breaker, List<TopNOperator.SortOrder> sortOrders, RowFiller filler) {
        GroupedRowFiller groupedFiller = (GroupedRowFiller) filler;
        return new GroupedRow(
            new UngroupedRow(breaker, sortOrders, groupedFiller.preAllocatedKeysSize(), groupedFiller.preAllocatedValueSize()),
            groupedFiller.preAllocatedGroupKeySize()
        );
    }

    @Override
    public TopNQueue queue(CircuitBreaker breaker, int topCount) {
        return new GroupedQueue(breaker, topCount);
    }
}
