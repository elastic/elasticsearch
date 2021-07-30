/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.geo.GeoPoint;

import java.io.IOException;

final class SingletonMultiGeoPointValues extends MultiGeoPointValues {

    private final GeoPointValues in;

    SingletonMultiGeoPointValues(GeoPointValues in) {
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
    public GeoPoint nextValue() {
        return in.geoPointValue();
    }

    GeoPointValues getGeoPointValues() {
        return in;
    }
}
