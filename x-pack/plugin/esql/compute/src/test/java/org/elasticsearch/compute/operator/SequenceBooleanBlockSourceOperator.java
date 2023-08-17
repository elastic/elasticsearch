/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;

import java.util.List;

/**
 * A source operator whose output is the given boolean values. This operator produces pages
 * containing a single Block. The Block contains the boolean values from the given list, in order.
 */
public class SequenceBooleanBlockSourceOperator extends AbstractBlockSourceOperator {

    static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final boolean[] values;

    public SequenceBooleanBlockSourceOperator(List<Boolean> values) {
        this(values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceBooleanBlockSourceOperator(List<Boolean> values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = new boolean[values.size()];
        for (int i = 0; i < values.size(); i++) {
            this.values[i] = values.get(i);
        }
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        BooleanVector.Builder builder = BooleanVector.newVectorBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.appendBoolean(values[positionOffset + i]);
        }
        currentPosition += length;
        return new Page(builder.build().asBlock());
    }

    protected int remaining() {
        return values.length - currentPosition;
    }
}
