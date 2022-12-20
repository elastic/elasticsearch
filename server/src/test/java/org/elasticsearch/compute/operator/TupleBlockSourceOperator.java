/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;

import java.util.BitSet;
import java.util.List;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given tuple values. This operator produces pages
 * with two Blocks. The returned pages preserve the order of values as given in the in initial list.
 */
public class TupleBlockSourceOperator extends AbstractBlockSourceOperator {

    private static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final List<Tuple<Long, Long>> values;

    public TupleBlockSourceOperator(Stream<Tuple<Long, Long>> values) {
        this(values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public TupleBlockSourceOperator(Stream<Tuple<Long, Long>> values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values.toList();
    }

    public TupleBlockSourceOperator(List<Tuple<Long, Long>> values) {
        this(values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public TupleBlockSourceOperator(List<Tuple<Long, Long>> values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values;
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        final long[] block1 = new long[length];
        final BitSet nulls1 = new BitSet(length);
        final long[] block2 = new long[length];
        final BitSet nulls2 = new BitSet(length);
        for (int i = 0; i < length; i++) {
            Tuple<Long, Long> item = values.get(positionOffset + i);
            if (item.v1() == null) {
                nulls1.set(i);
            } else {
                block1[i] = item.v1();
            }
            if (item.v2() == null) {
                nulls2.set(i);
            } else {
                block2[i] = item.v2();
            }
        }
        currentPosition += length;
        return new Page(new LongArrayBlock(block1, length, nulls1), new LongArrayBlock(block2, length, nulls2));
    }

    @Override
    protected int remaining() {
        return values.size() - currentPosition;
    }
}
