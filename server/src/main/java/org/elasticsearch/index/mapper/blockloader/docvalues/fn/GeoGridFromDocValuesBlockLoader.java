/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.NumericDvSingletonOrSorted;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

/**
 * Loads {@code geo_point} doc values as geo-grid cell ids ({@code long}s), fusing
 * {@code ST_GEOHASH}, {@code ST_GEOTILE} or {@code ST_GEOHEX} into the load. Each encoded point
 * is decoded to latitude and longitude and handed to the {@link BlockLoaderFunctionConfig.GeoGridEncoder}
 * from the config, so the point itself is never materialised as a block. Multi-valued points produce
 * one cell id per point, in doc values order and without de-duplication, which matches the output of the
 * equivalent ES|QL evaluator.
 */
public class GeoGridFromDocValuesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;
    private final BlockLoaderFunctionConfig.GeoGrid config;

    public GeoGridFromDocValuesBlockLoader(String fieldName, BlockLoaderFunctionConfig.GeoGrid config) {
        this.fieldName = fieldName;
        this.config = config;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.longs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        NumericDvSingletonOrSorted dv = NumericDvSingletonOrSorted.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.COLUMN_READER;
        }
        if (dv.singleton() != null) {
            return new Singleton(dv.singleton(), config.encoder());
        }
        return new Sorted(dv.sorted(), config.encoder());
    }

    @Override
    public String toString() {
        return "GeoGridFromDocValues[" + fieldName + ", " + config.function() + ", " + config.precision() + "]";
    }

    static long cellId(long encodedPoint, BlockLoaderFunctionConfig.GeoGridEncoder encoder) {
        // Same layout as GeoPoint#resetFromEncoded: latitude in the high 32 bits, longitude in the low 32 bits.
        double latitude = GeoEncodingUtils.decodeLatitude((int) (encodedPoint >>> 32));
        double longitude = GeoEncodingUtils.decodeLongitude((int) encodedPoint);
        return encoder.encode(longitude, latitude);
    }

    private static class Singleton extends BlockDocValuesReader {
        private final TrackingNumericDocValues numericDocValues;
        private final BlockLoaderFunctionConfig.GeoGridEncoder encoder;

        Singleton(TrackingNumericDocValues numericDocValues, BlockLoaderFunctionConfig.GeoGridEncoder encoder) {
            super(null);
            this.numericDocValues = numericDocValues;
            this.encoder = encoder;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            NumericDocValues docValues = numericDocValues.docValues();
            try (LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    if (docValues.advanceExact(docs.get(i))) {
                        builder.appendLong(cellId(docValues.longValue(), encoder));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "GeoGridFromDocValues.Singleton";
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }

    private static class Sorted extends BlockDocValuesReader {
        private final TrackingSortedNumericDocValues numericDocValues;
        private final BlockLoaderFunctionConfig.GeoGridEncoder encoder;

        Sorted(TrackingSortedNumericDocValues numericDocValues, BlockLoaderFunctionConfig.GeoGridEncoder encoder) {
            super(null);
            this.numericDocValues = numericDocValues;
            this.encoder = encoder;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            SortedNumericDocValues docValues = numericDocValues.docValues();
            // Cell ids of sorted points are neither sorted nor unique, so use the unconstrained builder.
            try (LongBuilder builder = factory.longs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    if (docValues.advanceExact(docs.get(i)) == false) {
                        builder.appendNull();
                        continue;
                    }
                    int count = docValues.docValueCount();
                    if (count == 1) {
                        builder.appendLong(cellId(docValues.nextValue(), encoder));
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendLong(cellId(docValues.nextValue(), encoder));
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "GeoGridFromDocValues.Sorted";
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }
}
