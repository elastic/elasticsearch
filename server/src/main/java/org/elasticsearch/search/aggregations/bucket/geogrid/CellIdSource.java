/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

/**
 * Wrapper class to help convert {@link MultiGeoPointValues}
 * to numeric long values for bucketing.
 */
public class CellIdSource extends ValuesSource.Numeric {
    private final ValuesSource.GeoPoint valuesSource;
    private final int precision;
    private final GeoPointLongEncoder encoder;
    private final GeoBoundingBox geoBoundingBox;

    public CellIdSource(GeoPoint valuesSource,int precision, GeoBoundingBox geoBoundingBox, GeoPointLongEncoder encoder) {
        this.valuesSource = valuesSource;
        //different GeoPoints could map to the same or different hashing cells.
        this.precision = precision;
        this.geoBoundingBox = geoBoundingBox;
        this.encoder = encoder;
    }

    public int precision() {
        return precision;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext ctx) {
        if (geoBoundingBox.isUnbounded()) {
            return new UnboundedCellValues(valuesSource.geoPointValues(ctx), precision, encoder);
        }
        return new BoundedCellValues(valuesSource.geoPointValues(ctx), precision, encoder, geoBoundingBox);
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    /**
     * The encoder to use to convert a geopoint's (lon, lat, precision) into
     * a long-encoded bucket key for aggregating.
     */
    @FunctionalInterface
    public interface GeoPointLongEncoder {
        long encode(double lon, double lat, int precision);
    }

}
