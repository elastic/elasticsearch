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
import org.apache.lucene.util.ArrayUtil;
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
 * equivalent ES|QL evaluator. For bounded grids the encoder returns a negative id for a point outside the
 * bounds; such points are dropped, and a document left with no cell loads as {@code null}, again as the
 * evaluator does.
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
        // Encoders may carry scratch state, so every reader gets its own
        BlockLoaderFunctionConfig.GeoGridEncoder encoder = config.encoders().get();
        if (dv.singleton() != null) {
            return new Singleton(dv.singleton(), encoder);
        }
        return new Sorted(dv.sorted(), encoder);
    }

    @Override
    public String toString() {
        return "GeoGridFromDocValues["
            + fieldName
            + ", "
            + config.function()
            + ", "
            + config.precision()
            + (config.bounds() == null ? "" : ", bounded")
            + "]";
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
                    long cellId = docValues.advanceExact(docs.get(i)) ? cellId(docValues.longValue(), encoder) : -1;
                    if (cellId < 0) {
                        builder.appendNull();
                    } else {
                        builder.appendLong(cellId);
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
        /** Cells of the current document that lie inside the bounds; sized on demand. */
        private long[] cells = new long[8];

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
                        long cellId = cellId(docValues.nextValue(), encoder);
                        if (cellId < 0) {
                            builder.appendNull();
                        } else {
                            builder.appendLong(cellId);
                        }
                        continue;
                    }
                    if (cells.length < count) {
                        cells = new long[ArrayUtil.oversize(count, Long.BYTES)];
                    }
                    int kept = 0;
                    for (int v = 0; v < count; v++) {
                        long cellId = cellId(docValues.nextValue(), encoder);
                        if (cellId >= 0) {
                            cells[kept++] = cellId;
                        }
                    }
                    if (kept == 0) {
                        builder.appendNull();
                    } else if (kept == 1) {
                        builder.appendLong(cells[0]);
                    } else {
                        builder.beginPositionEntry();
                        for (int v = 0; v < kept; v++) {
                            builder.appendLong(cells[v]);
                        }
                        builder.endPositionEntry();
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
            return "GeoGridFromDocValues.Sorted";
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }
}
