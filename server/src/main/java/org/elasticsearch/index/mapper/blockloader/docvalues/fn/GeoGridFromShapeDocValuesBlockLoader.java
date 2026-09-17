/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.util.List;

/**
 * Loads {@code geo_shape} doc values as the geo-grid cells ({@code long}s) intersecting each shape, fusing
 * {@code ST_GEOHASH}, {@code ST_GEOTILE} or {@code ST_GEOHEX} into the load. The binary doc value holds the indexed
 * triangle tree of the document's shapes and is handed to the {@link BlockLoaderFunctionConfig.GeoGridShapeTiler}
 * from the config, so the shape is never read from {@code _source} nor materialised as a block. A document whose
 * shapes intersect no cell, for instance because all lie outside the bounds, loads as {@code null}.
 */
public class GeoGridFromShapeDocValuesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private static final long ESTIMATED_SIZE = ByteSizeValue.ofKb(2).getBytes();

    private final String fieldName;
    private final BlockLoaderFunctionConfig.GeoGrid config;
    @Nullable
    private final Warnings warnings;

    public GeoGridFromShapeDocValuesBlockLoader(String fieldName, BlockLoaderFunctionConfig.GeoGrid config, @Nullable Warnings warnings) {
        this.fieldName = fieldName;
        this.config = config;
        this.warnings = warnings;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.longs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(ESTIMATED_SIZE, "load blocks");
        BinaryDocValues binaryDocValues = context.reader().getBinaryDocValues(fieldName);
        if (binaryDocValues == null) {
            breaker.addWithoutBreaking(-ESTIMATED_SIZE);
            return ConstantNull.COLUMN_READER;
        }
        // Tilers may carry scratch state, so every reader gets its own
        return new Reader(breaker, binaryDocValues, config.shapeTilers().create(warnings));
    }

    @Override
    public String toString() {
        return "GeoGridFromShapeDocValues["
            + fieldName
            + ", "
            + config.function()
            + ", "
            + config.precision()
            + (config.bounds() == null ? "" : ", bounded")
            + "]";
    }

    private static class Reader implements BlockLoader.ColumnAtATimeReader {
        private final CircuitBreaker breaker;
        private final BinaryDocValues binaryDocValues;
        private final BlockLoaderFunctionConfig.GeoGridShapeTiler tiler;

        Reader(CircuitBreaker breaker, BinaryDocValues binaryDocValues, BlockLoaderFunctionConfig.GeoGridShapeTiler tiler) {
            this.breaker = breaker;
            this.binaryDocValues = binaryDocValues;
            this.tiler = tiler;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            // Cells are in tiler order, neither sorted nor unique across documents, so use the unconstrained builder
            try (LongBuilder builder = factory.longs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        private void read(int doc, LongBuilder builder) throws IOException {
            if (binaryDocValues.advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            List<Long> cells = tiler.cells(binaryDocValues.binaryValue());
            if (cells.isEmpty()) {
                builder.appendNull();
            } else if (cells.size() == 1) {
                builder.appendLong(cells.get(0));
            } else {
                builder.beginPositionEntry();
                for (long cell : cells) {
                    builder.appendLong(cell);
                }
                builder.endPositionEntry();
            }
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return true;
        }

        @Override
        public String toString() {
            return "GeoGridFromShapeDocValues";
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-ESTIMATED_SIZE);
        }
    }
}
