/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for {@link DoubleVectorVector}s that grows as needed.
 */
final class DoubleVectorVectorBuilder extends AbstractVectorBuilder implements DoubleVectorVector.Builder {

    private final List<double[]> values;

    private int dims;

    DoubleVectorVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        values = new ArrayList<>(initialSize);
    }

    @Override
    public DoubleVectorVectorBuilder appendDoubles(double... values) {
        this.values.add(valueCount, values);
        this.valueCount++;
        this.dims = values.length;
        return this;
    }

    @Override
    protected int elementSize() {
        return this.values.size() * this.dims * Double.BYTES;
    }

    @Override
    protected int valuesLength() {
        return values.size();
    }

    @Override
    protected void growValuesArray(int newSize) {
        // do nothing
    }

    @Override
    public DoubleVectorVector build() {
        finish();
        List<DoubleVector> list = new ArrayList<>();
        for (double[] da : values) {
            DoubleVectorBuilder build = new DoubleVectorBuilder(dims, blockFactory);
            for (double d : da) {
                build.appendDouble(d);
            }
            list.add(build.build());
        }
        DoubleVector[] vectors = list.toArray(new DoubleVector[0]);
        DoubleVectorVector vector = new DoubleVectorVector(vectors, blockFactory);
        built();
        return vector;
    }
}
