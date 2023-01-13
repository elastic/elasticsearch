/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.aggregation.AggregatorState;

public class AggregatorStateBlock<T extends AggregatorState<T>> extends AbstractVectorBlock {

    private final AggregatorStateVector<T> vector;

    AggregatorStateBlock(AggregatorStateVector<T> vector, int positionCount) {
        super(positionCount);
        this.vector = vector;
    }

    public AggregatorStateVector<T> asVector() {
        return vector;
    }

    @Override
    public Object getObject(int valueIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ElementType elementType() {
        return ElementType.UNKNOWN;
    }

    @Override
    public AggregatorStateBlock<T> filter(int... positions) {
        throw new UnsupportedOperationException();
    }
}
