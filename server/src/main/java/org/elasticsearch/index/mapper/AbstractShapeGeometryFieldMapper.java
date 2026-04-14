/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.Extent;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * Base class for shape field mappers
 */
public abstract class AbstractShapeGeometryFieldMapper<T> extends AbstractGeometryFieldMapper<T> {
    /**
     * Circuit breaker space reserved for each reader. Measured at 1.2kb in a heap dump, 2kb is
     * our overestimate.
     */
    private static final long BLOCK_LOADER_ESTIMATED_SIZE = ByteSizeValue.ofKb(2).getBytes();

    @Override
    protected boolean supportsParsingObject() {
        // ShapeGeometryFieldMapper supports parsing Well-Known Text (WKT) and GeoJSON.
        // WKT are of type String and GeoJSON for all shapes are of type Array.
        return false;
    }

    public static Parameter<Explicit<Boolean>> coerceParam(Function<FieldMapper, Explicit<Boolean>> initializer, boolean coerceByDefault) {
        return Parameter.explicitBoolParam("coerce", true, initializer, coerceByDefault);
    }

    private static final Explicit<Orientation> IMPLICIT_RIGHT = new Explicit<>(Orientation.RIGHT, false);

    public static Parameter<Explicit<Orientation>> orientationParam(Function<FieldMapper, Explicit<Orientation>> initializer) {
        return new Parameter<>(
            "orientation",
            true,
            () -> IMPLICIT_RIGHT,
            (n, c, o) -> new Explicit<>(Orientation.fromString(o.toString()), true),
            initializer,
            (b, f, v) -> b.field(f, v.value()),
            v -> v.value().toString()
        );
    }

    public abstract static class AbstractShapeGeometryFieldType<T> extends AbstractGeometryFieldType<T> {

        private final Orientation orientation;

        protected AbstractShapeGeometryFieldType(
            String name,
            IndexType indexType,
            boolean isStored,
            Parser<T> parser,
            Orientation orientation,
            Map<String, String> meta
        ) {
            super(name, indexType, isStored, parser, null, meta);
            this.orientation = orientation;
        }

        public Orientation orientation() {
            return this.orientation;
        }

        @Override
        protected Object nullValueAsSource(T nullValue) {
            // we don't support null value for shapes
            return nullValue;
        }

        protected static class BoundsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
            private final String fieldName;

            protected BoundsBlockLoader(String fieldName) {
                this.fieldName = fieldName;
            }

            @Override
            public BlockLoader.ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
                breaker.addEstimateBytesAndMaybeBreak(BLOCK_LOADER_ESTIMATED_SIZE, "load blocks");
                BinaryDocValues binaryDocValues = context.reader().getBinaryDocValues(fieldName);
                if (binaryDocValues == null) {
                    breaker.addWithoutBreaking(-BLOCK_LOADER_ESTIMATED_SIZE);
                    return ConstantNull.COLUMN_READER;
                }
                return new BoundsReader(breaker, binaryDocValues);
            }

            @Override
            public BlockLoader.Builder builder(BlockLoader.BlockFactory factory, int expectedCount) {
                return factory.ints(expectedCount);
            }
        }

        private static class BoundsReader implements BlockLoader.ColumnAtATimeReader {
            private final GeometryDocValueReader reader = new GeometryDocValueReader();
            private final CircuitBreaker breaker;
            private final BinaryDocValues binaryDocValues;

            private BoundsReader(CircuitBreaker breaker, BinaryDocValues binaryDocValues) {
                this.breaker = breaker;
                this.binaryDocValues = binaryDocValues;
            }

            @Override
            public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
                throws IOException {
                try (var builder = factory.ints(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        read(docs.get(i), builder);
                    }
                    return builder.build();
                }
            }

            private void read(int doc, BlockLoader.IntBuilder builder) throws IOException {
                if (binaryDocValues.advanceExact(doc) == false) {
                    builder.appendNull();
                    return;
                }
                reader.reset(binaryDocValues.binaryValue());
                writeExtent(builder, reader.getExtent());
            }

            @Override
            public boolean canReuse(int startingDocID) {
                return true;
            }

            private void writeExtent(BlockLoader.IntBuilder builder, Extent extent) {
                // We store the 6 values as a single multi-valued field, in the same order as the fields in the Extent class
                builder.beginPositionEntry();
                builder.appendInt(extent.top);
                builder.appendInt(extent.bottom);
                builder.appendInt(extent.negLeft);
                builder.appendInt(extent.negRight);
                builder.appendInt(extent.posLeft);
                builder.appendInt(extent.posRight);
                builder.endPositionEntry();
            }

            @Override
            public void close() {
                breaker.addWithoutBreaking(-BLOCK_LOADER_ESTIMATED_SIZE);
            }
        }

        /**
         * Block loader for extracting centroid data from shape doc values.
         * Outputs 4 doubles per document: [decoded X, decoded Y, weight, shapeTypeOrdinal]
         */
        protected static class CentroidBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
            private final String fieldName;
            private final CoordinateEncoder encoder;

            protected CentroidBlockLoader(String fieldName, CoordinateEncoder encoder) {
                this.fieldName = fieldName;
                this.encoder = encoder;
            }

            @Override
            public BlockLoader.ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
                breaker.addEstimateBytesAndMaybeBreak(BLOCK_LOADER_ESTIMATED_SIZE, "load blocks");
                BinaryDocValues binaryDocValues = context.reader().getBinaryDocValues(fieldName);
                if (binaryDocValues == null) {
                    breaker.addWithoutBreaking(-BLOCK_LOADER_ESTIMATED_SIZE);
                    return ConstantNull.COLUMN_READER;
                }
                return new CentroidReader(breaker, binaryDocValues, encoder);
            }

            @Override
            public BlockLoader.Builder builder(BlockLoader.BlockFactory factory, int expectedCount) {
                return factory.doubles(expectedCount);
            }
        }

        private static class CentroidReader implements BlockLoader.ColumnAtATimeReader {
            private final GeometryDocValueReader reader = new GeometryDocValueReader();
            private final CircuitBreaker breaker;
            private final BinaryDocValues binaryDocValues;
            private final CoordinateEncoder encoder;

            private CentroidReader(CircuitBreaker breaker, BinaryDocValues binaryDocValues, CoordinateEncoder encoder) {
                this.breaker = breaker;
                this.binaryDocValues = binaryDocValues;
                this.encoder = encoder;
            }

            @Override
            public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
                throws IOException {
                try (var builder = factory.doubles(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        read(docs.get(i), builder);
                    }
                    return builder.build();
                }
            }

            private void read(int doc, BlockLoader.DoubleBuilder builder) throws IOException {
                if (binaryDocValues.advanceExact(doc) == false) {
                    builder.appendNull();
                    return;
                }
                reader.reset(binaryDocValues.binaryValue());
                writeCentroid(builder);
            }

            @Override
            public boolean canReuse(int startingDocID) {
                return true;
            }

            private void writeCentroid(BlockLoader.DoubleBuilder builder) throws IOException {
                // We store 4 values as a single multi-valued field: [x, y, weight, shapeTypeOrdinal]
                builder.beginPositionEntry();
                builder.appendDouble(encoder.decodeX(reader.getCentroidX()));
                builder.appendDouble(encoder.decodeY(reader.getCentroidY()));
                builder.appendDouble(reader.getSumCentroidWeight());
                builder.appendDouble(reader.getDimensionalShapeType().ordinal());
                builder.endPositionEntry();
            }

            @Override
            public void close() {
                breaker.addWithoutBreaking(-BLOCK_LOADER_ESTIMATED_SIZE);
            }
        }

        /**
         * Block loader for extracting both bounds and centroid data from shape doc values.
         * The default implementation outputs 10 doubles per document for geo shapes:
         * [top, bottom, negLeft, negRight, posLeft, posRight, centroidX, centroidY, weight, shapeTypeOrdinal]
         * Subclasses can override writeBounds() for different bounds formats (e.g., cartesian uses 4 values).
         */
        protected static class BoundsAndCentroidBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
            private final String fieldName;
            private final CoordinateEncoder encoder;

            protected BoundsAndCentroidBlockLoader(String fieldName, CoordinateEncoder encoder) {
                this.fieldName = fieldName;
                this.encoder = encoder;
            }

            @Override
            public BlockLoader.ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
                breaker.addEstimateBytesAndMaybeBreak(BLOCK_LOADER_ESTIMATED_SIZE, "load blocks");
                BinaryDocValues binaryDocValues = context.reader().getBinaryDocValues(fieldName);
                if (binaryDocValues == null) {
                    breaker.addWithoutBreaking(-BLOCK_LOADER_ESTIMATED_SIZE);
                    return ConstantNull.COLUMN_READER;
                }
                return new BoundsAndCentroidReader(breaker, binaryDocValues, encoder, this);
            }

            @Override
            public BlockLoader.Builder builder(BlockLoader.BlockFactory factory, int expectedCount) {
                return factory.doubles(expectedCount);
            }

            /**
             * Writes the bounds portion of the combined output.
             * Default implementation writes 6 values for geo shapes: [top, bottom, negLeft, negRight, posLeft, posRight].
             * Subclasses can override for different formats (e.g., cartesian uses 4 values).
             */
            protected void writeBounds(BlockLoader.DoubleBuilder builder, Extent extent) {
                builder.appendDouble(extent.top);
                builder.appendDouble(extent.bottom);
                builder.appendDouble(extent.negLeft);
                builder.appendDouble(extent.negRight);
                builder.appendDouble(extent.posLeft);
                builder.appendDouble(extent.posRight);
            }
        }

        private static class BoundsAndCentroidReader implements BlockLoader.ColumnAtATimeReader {
            private final GeometryDocValueReader reader = new GeometryDocValueReader();
            private final CircuitBreaker breaker;
            private final BinaryDocValues binaryDocValues;
            private final CoordinateEncoder encoder;
            private final BoundsAndCentroidBlockLoader loader;

            private BoundsAndCentroidReader(
                CircuitBreaker breaker,
                BinaryDocValues binaryDocValues,
                CoordinateEncoder encoder,
                BoundsAndCentroidBlockLoader loader
            ) {
                this.breaker = breaker;
                this.binaryDocValues = binaryDocValues;
                this.encoder = encoder;
                this.loader = loader;
            }

            @Override
            public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
                throws IOException {
                try (var builder = factory.doubles(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        read(docs.get(i), builder);
                    }
                    return builder.build();
                }
            }

            private void read(int doc, BlockLoader.DoubleBuilder builder) throws IOException {
                if (binaryDocValues.advanceExact(doc) == false) {
                    builder.appendNull();
                    return;
                }
                reader.reset(binaryDocValues.binaryValue());
                writeBoundsAndCentroid(builder, reader.getExtent());
            }

            @Override
            public boolean canReuse(int startingDocID) {
                return true;
            }

            private void writeBoundsAndCentroid(BlockLoader.DoubleBuilder builder, Extent extent) throws IOException {
                builder.beginPositionEntry();
                // Write bounds using the loader's implementation (may vary by shape type)
                loader.writeBounds(builder, extent);
                // Centroid (4 values) - decoded coordinates, weight, and type ordinal
                builder.appendDouble(encoder.decodeX(reader.getCentroidX()));
                builder.appendDouble(encoder.decodeY(reader.getCentroidY()));
                builder.appendDouble(reader.getSumCentroidWeight());
                builder.appendDouble(reader.getDimensionalShapeType().ordinal());
                builder.endPositionEntry();
            }

            @Override
            public void close() {
                breaker.addWithoutBreaking(-BLOCK_LOADER_ESTIMATED_SIZE);
            }
        }
    }

    protected Explicit<Boolean> coerce;
    protected Explicit<Orientation> orientation;

    protected AbstractShapeGeometryFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Boolean> coerce,
        Explicit<Boolean> ignoreZValue,
        Explicit<Orientation> orientation,
        Parser<T> parser
    ) {
        super(simpleName, mappedFieldType, builderParams, ignoreMalformed, ignoreZValue, parser);
        this.coerce = coerce;
        this.orientation = orientation;
    }

    public boolean coerce() {
        return coerce.value();
    }

    public Orientation orientation() {
        return orientation.value();
    }
}
