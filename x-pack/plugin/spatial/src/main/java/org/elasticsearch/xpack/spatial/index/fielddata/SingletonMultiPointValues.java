/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;

final class SingletonMultiPointValues extends MultiPointValues {

    private final PointValues in;

    SingletonMultiPointValues(PointValues in) {
        this.in = in;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        return in.advanceExact(doc);
    }

    @Override
    public int docValueCount() {
        return 1;
    }

    @Override
    public CartesianPoint nextValue() {
        return in.pointValue();
    }

    PointValues getPointValues() {
        return in;
    }
}
