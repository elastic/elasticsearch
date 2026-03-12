/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

public class LongTopNSetTests extends TopNSetTestCase<LongTopNSet, Long> {

    @Override
    protected LongTopNSet build(BigArrays bigArrays, SortOrder sortOrder, int limit) {
        return new LongTopNSet(bigArrays, sortOrder, limit);
    }

    @Override
    protected Long randomValue() {
        return randomLong();
    }

    @Override
    protected List<Long> threeSortedValues() {
        return List.of(Long.MIN_VALUE, randomLong(), Long.MAX_VALUE);
    }

    @Override
    protected void collect(LongTopNSet sort, Long value) {
        sort.collect(value);
    }

    @Override
    protected void reduceLimitByOne(LongTopNSet sort) {
        sort.reduceLimitByOne();
    }

    @Override
    protected Long getWorstValue(LongTopNSet sort) {
        return sort.getWorstValue();
    }

    @Override
    protected int getCount(LongTopNSet sort) {
        return sort.getCount();
    }

}
