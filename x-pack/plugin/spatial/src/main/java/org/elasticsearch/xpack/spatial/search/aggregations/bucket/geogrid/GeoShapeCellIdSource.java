/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSource;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.util.function.LongConsumer;

public class GeoShapeCellIdSource  extends ValuesSource.Numeric {
    private final GeoShapeValuesSource valuesSource;
    private final GeoGridTiler encoder;
    private LongConsumer circuitBreakerConsumer;

    public GeoShapeCellIdSource(GeoShapeValuesSource valuesSource, GeoGridTiler encoder) {
        this.valuesSource = valuesSource;
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

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext ctx) {
        GeoShapeValues geoValues = valuesSource.geoShapeValues(ctx);
        ValuesSourceType vs = geoValues.valuesSourceType();
        if (GeoShapeValuesSourceType.instance() == vs) {
            // docValues are geo shapes
            return new GeoShapeCellValues(geoValues, encoder, circuitBreakerConsumer);
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
