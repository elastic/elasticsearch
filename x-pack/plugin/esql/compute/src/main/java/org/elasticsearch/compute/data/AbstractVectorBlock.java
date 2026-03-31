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
abstract class AbstractVectorBlock extends AbstractNonThreadSafeRefCounted implements Block {

    @Override
    public final int getFirstValueIndex(int position) {
        return position;
    }

    @Override
    public final int getTotalValueCount() {
        return getPositionCount();
    }

    public final int getValueCount(int position) {
        return 1;
    }

    @Override
    public final boolean isNull(int position) {
        return false;
    }

    @Override
    public final boolean mayHaveNulls() {
        return false;
    }

    @Override
    public final boolean areAllValuesNull() {
        return false;
    }

    @Override
    public final boolean mayHaveMultivaluedFields() {
        return false;
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return false;
    }

    @Override
    public final MvOrdering mvOrdering() {
        return MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING;
    }
}
