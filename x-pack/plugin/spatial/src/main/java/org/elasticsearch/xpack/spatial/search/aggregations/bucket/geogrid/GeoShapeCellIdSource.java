/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiGeoShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSource;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.util.function.LongConsumer;

public class GeoShapeCellIdSource  extends ValuesSource.Numeric {
    private final GeoShapeValuesSource valuesSource;
    private final int precision;
    private final GeoGridTiler encoder;
    private LongConsumer circuitBreakerConsumer;

    public GeoShapeCellIdSource(GeoShapeValuesSource valuesSource, int precision, GeoGridTiler encoder) {
        this.valuesSource = valuesSource;
        this.precision = precision;
        this.encoder = encoder;
        this.circuitBreakerConsumer = (l) -> {};
    }

    /**
     * This setter exists since the aggregator's circuit-breaking accounting needs to be
     * accessible from within the values-source. Problem is that this values-source needs to
     * be created and passed to the aggregator before we have access to this functionality.
     */
    public void setCircuitBreakerConsumer(LongConsumer circuitBreakerConsumer) {
        this.circuitBreakerConsumer = circuitBreakerConsumer;
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
        MultiGeoShapeValues geoValues = valuesSource.geoShapeValues(ctx);
        if (precision == 0) {
            // special case, precision 0 is the whole world
            return new AllCellValues(geoValues, encoder, circuitBreakerConsumer);
        }
        ValuesSourceType vs = geoValues.valuesSourceType();
        if (GeoShapeValuesSourceType.instance() == vs) {
            // docValues are geo shapes
            return new GeoShapeCellValues(geoValues, precision, encoder, circuitBreakerConsumer);
        } else {
            throw new IllegalArgumentException("unsupported geo type");
        }
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }
}
