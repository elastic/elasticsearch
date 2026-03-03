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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.Extent;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;

import java.io.IOException;
import java.nio.ByteOrder;
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
        private final IndexVersion indexCreatedVersion;

        protected AbstractShapeGeometryFieldType(
            String name,
            IndexType indexType,
            boolean isStored,
            Parser<T> parser,
            Orientation orientation,
            IndexVersion indexCreatedVersion,
            Map<String, String> meta
        ) {
            super(name, indexType, isStored, parser, null, meta);
            this.orientation = orientation;
            this.indexCreatedVersion = indexCreatedVersion;
        }

        public Orientation orientation() {
            return this.orientation;
        }

        /**
         * Returns true if the doc-values for this field use the V2 format that supports
         * geometry reconstruction via {@link GeometryDocValueReader#getGeometry}. Indices
         * created before {@link IndexVersions#GEOMETRY_DOC_VALUES_V2} use the legacy format,
         * which only supports reconstruction of point geometries.
         */
        @Override
        public boolean supportsGeometryDocValueReconstruction() {
            return indexCreatedVersion.onOrAfter(IndexVersions.GEOMETRY_DOC_VALUES_V2);
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
                        read(binaryDocValues, docs.get(i), builder);
                    }
                    return builder.build();
                }
            }

            private void read(BinaryDocValues binaryDocValues, int doc, org.elasticsearch.index.mapper.BlockLoader.IntBuilder builder)
                throws IOException {
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
         * Block loader that reconstructs geometry from binary doc-values and returns WKB-encoded bytes.
         * Used when the optimizer determines that reading from doc-values is sufficient (e.g. when the
         * field is consumed by ST_SIMPLIFY rather than returned directly).
         */
        protected static class GeometryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
            private final String fieldName;
            private final CoordinateEncoder encoder;

            protected GeometryBlockLoader(String fieldName, CoordinateEncoder encoder) {
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
                return new GeometryReader(breaker, binaryDocValues, encoder);
            }

            @Override
            public BlockLoader.Builder builder(BlockLoader.BlockFactory factory, int expectedCount) {
                return factory.bytesRefs(expectedCount);
            }
        }

        private static class GeometryReader implements BlockLoader.ColumnAtATimeReader {
            private final GeometryDocValueReader reader = new GeometryDocValueReader();
            private final CircuitBreaker breaker;
            private final BinaryDocValues binaryDocValues;
            private final CoordinateEncoder encoder;

            private GeometryReader(CircuitBreaker breaker, BinaryDocValues binaryDocValues, CoordinateEncoder encoder) {
                this.breaker = breaker;
                this.binaryDocValues = binaryDocValues;
                this.encoder = encoder;
            }

            @Override
            public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
                throws IOException {
                try (var builder = factory.bytesRefs(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        read(binaryDocValues, docs.get(i), builder);
                    }
                    return builder.build();
                }
            }

            private void read(BinaryDocValues binaryDocValues, int doc, BlockLoader.BytesRefBuilder builder) throws IOException {
                if (binaryDocValues.advanceExact(doc) == false) {
                    builder.appendNull();
                    return;
                }
                reader.reset(binaryDocValues.binaryValue());
                Geometry geometry = reader.getGeometry(encoder);
                if (geometry == null) {
                    builder.appendNull();
                    return;
                }
                byte[] wkb = WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN);
                builder.appendBytesRef(new BytesRef(wkb));
            }

            @Override
            public boolean canReuse(int startingDocID) {
                return true;
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
