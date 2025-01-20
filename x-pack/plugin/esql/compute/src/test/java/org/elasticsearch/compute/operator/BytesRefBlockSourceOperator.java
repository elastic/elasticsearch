/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;

import java.util.List;

/**
 * A source operator whose output is the given BytesRef values. This operator produces pages
 * containing a single Block. The Block contains the BytesRef values from the given list, in order.
 */
public class BytesRefBlockSourceOperator extends AbstractBlockSourceOperator {

    static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final BytesRef[] values;

    public BytesRefBlockSourceOperator(BlockFactory blockFactory, List<BytesRef> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public BytesRefBlockSourceOperator(BlockFactory blockFactory, List<BytesRef> values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        this.values = values.toArray(new BytesRef[0]);
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        BytesRefVector.Builder builder = blockFactory.newBytesRefVectorBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.appendBytesRef(values[positionOffset + i]);
        }
        currentPosition += length;
        return new Page(builder.build().asBlock());
    }

    protected int remaining() {
        return values.length - currentPosition;
    }
}
