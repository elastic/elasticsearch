/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given tuple values. This operator produces pages
 * with two Blocks. The returned pages preserve the order of values as given in the in initial list.
 */
public class LongBytesRefTupleBlockSourceOperator extends AbstractBlockSourceOperator {

    private static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final List<Tuple<Long, BytesRef>> values;

    public LongBytesRefTupleBlockSourceOperator(BlockFactory blockFactory, Stream<Tuple<Long, BytesRef>> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public LongBytesRefTupleBlockSourceOperator(BlockFactory blockFactory, Stream<Tuple<Long, BytesRef>> values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        this.values = values.toList();
    }

    public LongBytesRefTupleBlockSourceOperator(BlockFactory blockFactory, List<Tuple<Long, BytesRef>> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public LongBytesRefTupleBlockSourceOperator(BlockFactory blockFactory, List<Tuple<Long, BytesRef>> values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        this.values = values;
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        var blockBuilder1 = blockFactory.newLongBlockBuilder(length);
        var blockBuilder2 = blockFactory.newBytesRefBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            Tuple<Long, BytesRef> item = values.get(positionOffset + i);
            if (item.v1() == null) {
                blockBuilder1.appendNull();
            } else {
                blockBuilder1.appendLong(item.v1());
            }
            if (item.v2() == null) {
                blockBuilder2.appendNull();
            } else {
                blockBuilder2.appendBytesRef(item.v2());
            }
        }
        currentPosition += length;
        return new Page(blockBuilder1.build(), blockBuilder2.build());
    }

    @Override
    protected int remaining() {
        return values.size() - currentPosition;
    }
}
