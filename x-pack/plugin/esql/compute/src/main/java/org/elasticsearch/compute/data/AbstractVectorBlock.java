/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * A Block view of a Vector.
 */
abstract class AbstractVectorBlock extends AbstractBlock {

    AbstractVectorBlock(int positionCount) {
        super(positionCount);
    }

    @Override
    public int getFirstValueIndex(int position) {
        return position;
    }

    public int getValueCount(int position) {
        return 1;
    }

    @Override
    public boolean isNull(int position) {
        return false;
    }

    @Override
    public int nullValuesCount() {
        return 0;
    }

    @Override
    public boolean mayHaveNulls() {
        return false;
    }

    @Override
    public boolean areAllValuesNull() {
        return false;
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return false;
    }

    @Override
    public final MvOrdering mvOrdering() {
        return MvOrdering.UNORDERED;
    }

    @Override
    public final Block expand() {
        return this;
    }
}
