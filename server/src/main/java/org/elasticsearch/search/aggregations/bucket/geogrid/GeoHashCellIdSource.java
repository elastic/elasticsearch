/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.util.function.LongConsumer;

/**
 * {@link CellIdSource} implementation for Geohash aggregation
 */
public class GeoHashCellIdSource extends CellIdSource {

    public GeoHashCellIdSource(
        ValuesSource.GeoPoint valuesSource,
        int precision,
        GeoBoundingBox geoBoundingBox,
        LongConsumer circuitBreakerConsumer
    ) {
        super(valuesSource, precision, geoBoundingBox, circuitBreakerConsumer);
    }

    @Override
    protected NumericDocValues unboundedCellSingleValue(GeoPointValues values) {
        return new CellSingleValue(values, precision()) {
            @Override
            protected boolean advance(org.elasticsearch.common.geo.GeoPoint target) {
                value = Geohash.longEncode(target.getLon(), target.getLat(), precision);
                return true;
            }
        };
    }

    @Override
    protected NumericDocValues boundedCellSingleValue(GeoPointValues values, GeoBoundingBox boundingBox) {
        final GeoHashBoundedPredicate predicate = new GeoHashBoundedPredicate(precision(), boundingBox);
        return new CellSingleValue(values, precision()) {
            @Override
            protected boolean advance(org.elasticsearch.common.geo.GeoPoint target) {
                final String hash = Geohash.stringEncode(target.getLon(), target.getLat(), precision);
                if (pointInBounds(target.getLon(), target.getLat()) || predicate.validHash(hash)) {
                    value = Geohash.longEncode(hash);
                    return true;
                }
                return false;
            }
        };
    }

    @Override
    protected SortedNumericDocValues unboundedCellMultiValues(MultiGeoPointValues values) {
        return new CellMultiValues(values, precision(), circuitBreakerConsumer) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                values[valuesIdx] = Geohash.longEncode(target.getLon(), target.getLat(), precision);
                return valuesIdx + 1;
            }
        };
    }

    @Override
    protected SortedNumericDocValues boundedCellMultiValues(MultiGeoPointValues values, GeoBoundingBox boundingBox) {
        final GeoHashBoundedPredicate predicate = new GeoHashBoundedPredicate(precision(), boundingBox);
        return new CellMultiValues(values, precision(), circuitBreakerConsumer) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                final String hash = Geohash.stringEncode(target.getLon(), target.getLat(), precision);
                if (pointInBounds(target.getLon(), target.getLat()) || predicate.validHash(hash)) {
                    values[valuesIdx] = Geohash.longEncode(hash);
                    return valuesIdx + 1;
                }
                return valuesIdx;
            }
        };
    }
}
