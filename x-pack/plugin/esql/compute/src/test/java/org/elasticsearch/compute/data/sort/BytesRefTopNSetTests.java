/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

public class BytesRefTopNSetTests extends TopNSetTestCase<BytesRefTopNSet, BytesRef> {

    @Override
    protected BytesRefTopNSet build(BigArrays bigArrays, SortOrder sortOrder, int limit) {
        return new BytesRefTopNSet(bigArrays, sortOrder, limit);
    }

    @Override
    protected BytesRef randomValue() {
        return new BytesRef(randomAlphaOfLengthBetween(1, 16));
    }

    @Override
    protected List<BytesRef> threeSortedValues() {
        return List.of(new BytesRef("aaa"), new BytesRef("mmm"), new BytesRef("zzz"));
    }

    @Override
    protected void collect(BytesRefTopNSet sort, BytesRef value) {
        sort.collect(value);
    }

    @Override
    protected void reduceLimitByOne(BytesRefTopNSet sort) {
        sort.reduceLimitByOne();
    }

    @Override
    protected BytesRef getWorstValue(BytesRefTopNSet sort) {
        return sort.getWorstValue();
    }

    @Override
    protected int getCount(BytesRefTopNSet sort) {
        return sort.getCount();
    }
}
