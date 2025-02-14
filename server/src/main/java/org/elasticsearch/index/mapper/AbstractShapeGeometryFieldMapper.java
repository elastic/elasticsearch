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
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.lucene.spatial.Extent;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * Base class for shape field mappers
 */
public abstract class AbstractShapeGeometryFieldMapper<T> extends AbstractGeometryFieldMapper<T> {
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
            boolean isSearchable,
            boolean isStored,
            boolean hasDocValues,
            Parser<T> parser,
            Orientation orientation,
            Map<String, String> meta
        ) {
            super(name, isSearchable, isStored, hasDocValues, parser, null, meta);
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

            protected void writeExtent(BlockLoader.IntBuilder builder, Extent extent) {
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
            public BlockLoader.AllReader reader(LeafReaderContext context) throws IOException {
                return new BlockLoader.AllReader() {
                    @Override
                    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs) throws IOException {
                        var binaryDocValues = context.reader().getBinaryDocValues(fieldName);
                        var reader = new GeometryDocValueReader();
                        try (var builder = factory.ints(docs.count())) {
                            for (int i = 0; i < docs.count(); i++) {
                                read(binaryDocValues, docs.get(i), reader, builder);
                            }
                            return builder.build();
                        }
                    }

                    @Override
                    public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
                        var binaryDocValues = context.reader().getBinaryDocValues(fieldName);
                        var reader = new GeometryDocValueReader();
                        read(binaryDocValues, docId, reader, (IntBuilder) builder);
                    }

                    private void read(BinaryDocValues binaryDocValues, int doc, GeometryDocValueReader reader, IntBuilder builder)
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
                };
            }

            @Override
            public BlockLoader.Builder builder(BlockLoader.BlockFactory factory, int expectedCount) {
                return factory.ints(expectedCount);
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
