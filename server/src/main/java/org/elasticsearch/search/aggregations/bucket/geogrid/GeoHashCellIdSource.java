/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

/**
 * Class to help convert {@link MultiGeoPointValues} to Geohash {@link CellValues}
 */
public class GeoHashCellIdSource extends CellIdSource {

    public GeoHashCellIdSource(ValuesSource.GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox) {
        super(valuesSource, precision, geoBoundingBox);
    }

    @Override
    protected CellValues unboundedCellValues(MultiGeoPointValues values) {
        return new CellValues(values, precision()) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                values[valuesIdx] = Geohash.longEncode(target.getLon(), target.getLat(), precision);
                return valuesIdx + 1;
            }
        };
    }

    @Override
    protected CellValues boundedCellValues(MultiGeoPointValues values, GeoBoundingBox boundingBox) {
        final GeoHashBoundedPredicate predicate = new GeoHashBoundedPredicate(precision(), boundingBox);
        return new CellValues(values, precision()) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                final String hash = Geohash.stringEncode(target.getLon(), target.getLat(), precision);
                if (validPoint(target.getLon(), target.getLat()) || predicate.validHash(hash)) {
                    values[valuesIdx] = Geohash.longEncode(hash);
                    return valuesIdx + 1;
                }
                return valuesIdx;
            }
        };
    }
}
