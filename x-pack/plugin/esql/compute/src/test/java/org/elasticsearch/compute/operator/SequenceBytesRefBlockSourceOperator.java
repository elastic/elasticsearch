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

import java.util.List;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given double values. This operator produces pages
 * containing a single Block. The Block contains the double values from the given list, in order.
 */
public class SequenceBytesRefBlockSourceOperator extends AbstractBlockSourceOperator {

    static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final BytesRef[] values;

    public SequenceBytesRefBlockSourceOperator(BlockFactory blockFactory, Stream<BytesRef> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceBytesRefBlockSourceOperator(BlockFactory blockFactory, Stream<BytesRef> values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        this.values = values.toArray(BytesRef[]::new);
    }

    public SequenceBytesRefBlockSourceOperator(BlockFactory blockFactory, List<BytesRef> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceBytesRefBlockSourceOperator(BlockFactory blockFactory, List<BytesRef> values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        this.values = values.toArray(BytesRef[]::new);
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        try (var builder = blockFactory.newBytesRefVectorBuilder(length)) {
            for (int i = 0; i < length; i++) {
                builder.appendBytesRef(values[positionOffset + i]);
            }
            currentPosition += length;
            return new Page(builder.build().asBlock());
        }
    }

    protected int remaining() {
        return values.length - currentPosition;
    }
}
