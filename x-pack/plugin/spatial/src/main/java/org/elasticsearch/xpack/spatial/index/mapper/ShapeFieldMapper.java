/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.spatial.BinaryShapeDocValuesField;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.XYQueriesUtils;
import org.elasticsearch.script.field.AbstractScriptFieldFactory;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.Field;
import org.elasticsearch.xpack.spatial.common.CartesianBoundingBox;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.AbstractAtomicCartesianShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.CartesianShapeIndexFieldData;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * FieldMapper for indexing cartesian {@link XYShape}s.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [1050.0, -1000.0], [1051.0, -1000.0], [1051.0, -1001.0], [1050.0, -1001.0], [1050.0, -1000.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((1050.0 -1000.0, 1051.0 -1000.0, 1051.0 -1001.0, 1050.0 -1001.0, 1050.0 -1000.0))
 */
public class ShapeFieldMapper extends AbstractShapeGeometryFieldMapper<Geometry> {
    public static final String CONTENT_TYPE = "shape";

    private static Builder builder(FieldMapper in) {
        return ((ShapeFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
        final Parameter<Boolean> hasDocValues;

        private final IndexVersion version;
        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<Explicit<Boolean>> coerce;
        final Parameter<Explicit<Orientation>> orientation = orientationParam(m -> builder(m).orientation.get());

        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, IndexVersion version, boolean ignoreMalformedByDefault, boolean coerceByDefault) {
            super(name);
            this.version = version;
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.coerce = coerceParam(m -> builder(m).coerce.get(), coerceByDefault);
            this.hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), IndexVersions.V_8_4_0.onOrBefore(version));
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { indexed, hasDocValues, ignoreMalformed, ignoreZValue, coerce, orientation, meta };
        }

        @Override
        public ShapeFieldMapper build(MapperBuilderContext context) {
            if (multiFieldsBuilder.hasMultiFields()) {
                DEPRECATION_LOGGER.warn(
                    DeprecationCategory.MAPPINGS,
                    "shape_multifields",
                    "Adding multifields to [shape] mappers has no effect and will be forbidden in future"
                );
            }
            GeometryParser geometryParser = new GeometryParser(
                orientation.get().value().getAsBoolean(),
                coerce.get().value(),
                ignoreZValue.get().value()
            );
            Parser<Geometry> parser = new ShapeParser(geometryParser);
            ShapeFieldType ft = new ShapeFieldType(
                context.buildFullName(leafName()),
                indexed.get(),
                hasDocValues.get(),
                orientation.get().value(),
                parser,
                meta.get()
            );
            return new ShapeFieldMapper(leafName(), ft, multiFieldsBuilder.build(this, context), copyTo, parser, this);
        }
    }

    public static TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(
            n,
            c.indexVersionCreated(),
            IGNORE_MALFORMED_SETTING.get(c.getSettings()),
            COERCE_SETTING.get(c.getSettings())
        )
    );

    public static final class ShapeFieldType extends AbstractShapeGeometryFieldType<Geometry> implements ShapeQueryable {

        public ShapeFieldType(
            String name,
            boolean indexed,
            boolean hasDocValues,
            Orientation orientation,
            Parser<Geometry> parser,
            Map<String, String> meta
        ) {
            super(name, indexed, false, hasDocValues, parser, orientation, meta);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return (a, b) -> new CartesianShapeIndexFieldData(
                name(),
                CartesianShapeValuesSourceType.instance(),
                ShapeFieldMapper.CartesianShapeDocValuesField::new
            );
        }

        @Override
        public Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0);
            if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(IndexVersions.V_7_5_0)) {
                throw new QueryShardException(
                    context,
                    ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "]."
                );
            }
            try {
                return XYQueriesUtils.toXYShapeQuery(shape, fieldName, relation, isIndexed(), hasDocValues());
            } catch (IllegalArgumentException e) {
                throw new QueryShardException(context, "Exception creating query on Field [" + fieldName + "] " + e.getMessage(), e);
            }
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected Function<List<Geometry>, List<Object>> getFormatter(String format) {
            return GeometryFormatterFactory.getFormatter(format, Function.identity());
        }
    }

    private final Builder builder;
    private final CartesianShapeIndexer indexer;

    public ShapeFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Parser<Geometry> parser,
        Builder builder
    ) {
        super(
            simpleName,
            mappedFieldType,
            builder.ignoreMalformed.get(),
            builder.coerce.get(),
            builder.ignoreZValue.get(),
            builder.orientation.get(),
            multiFields,
            copyTo,
            parser
        );
        this.builder = builder;
        this.indexer = new CartesianShapeIndexer(mappedFieldType.name());
    }

    @Override
    protected void index(DocumentParserContext context, Geometry geometry) {
        // TODO: Make common with the index method GeoShapeWithDocValuesFieldMapper
        if (geometry == null) {
            return;
        }
        List<IndexableField> fields = indexer.indexShape(geometry);
        if (fieldType().isIndexed()) {
            context.doc().addAll(fields);
        }
        if (fieldType().hasDocValues()) {
            String name = fieldType().name();
            BinaryShapeDocValuesField docValuesField = (BinaryShapeDocValuesField) context.doc().getByKey(name);
            if (docValuesField == null) {
                docValuesField = new BinaryShapeDocValuesField(name, CoordinateEncoder.CARTESIAN);
                context.doc().addWithKey(name, docValuesField);
            }
            docValuesField.add(fields, geometry);
        } else if (fieldType().isIndexed()) {
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(
            leafName(),
            builder.version,
            builder.ignoreMalformed.getDefaultValue().value(),
            builder.coerce.getDefaultValue().value()
        ).init(this);
    }

    @Override
    public ShapeFieldType fieldType() {
        return (ShapeFieldType) super.fieldType();
    }

    public static class CartesianShapeDocValuesField extends AbstractScriptFieldFactory<CartesianShapeValues.CartesianShapeValue>
        implements
            Field<CartesianShapeValues.CartesianShapeValue>,
            DocValuesScriptFieldFactory,
            ScriptDocValues.GeometrySupplier<CartesianPoint, CartesianShapeValues.CartesianShapeValue> {

        private final CartesianShapeValues in;
        protected final String name;

        private CartesianShapeValues.CartesianShapeValue value;

        // maintain bwc by making bounding box and centroid available to CartesianShapeValues (ScriptDocValues)
        private final CartesianPoint centroid = new CartesianPoint();
        private final CartesianBoundingBox boundingBox = new CartesianBoundingBox(new CartesianPoint(), new CartesianPoint());
        private ScriptDocValues<CartesianShapeValues.CartesianShapeValue> cartesianShapeScriptValues;

        public CartesianShapeDocValuesField(CartesianShapeValues in, String name) {
            this.in = in;
            this.name = name;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                value = in.value();
                centroid.reset(value.getX(), value.getY());
                boundingBox.topLeft().reset(value.boundingBox().minX(), value.boundingBox().maxY());
                boundingBox.bottomRight().reset(value.boundingBox().maxX(), value.boundingBox().minY());
            } else {
                value = null;
            }
        }

        @Override
        public ScriptDocValues<CartesianShapeValues.CartesianShapeValue> toScriptDocValues() {
            if (cartesianShapeScriptValues == null) {
                cartesianShapeScriptValues = new AbstractAtomicCartesianShapeFieldData.CartesianShapeScriptValues(this);
            }

            return cartesianShapeScriptValues;
        }

        @Override
        public CartesianShapeValues.CartesianShapeValue getInternal(int index) {
            if (index != 0) {
                throw new UnsupportedOperationException();
            }

            return value;
        }

        // maintain bwc by making centroid available to CartesianShapeValues (ScriptDocValues)
        @Override
        public CartesianPoint getInternalCentroid() {
            return centroid;
        }

        // maintain bwc by making centroid available to CartesianShapeValues (ScriptDocValues)
        @Override
        public CartesianBoundingBox getInternalBoundingBox() {
            return boundingBox;
        }

        @Override
        public CartesianPoint getInternalLabelPosition() {
            try {
                return new CartesianPoint(value.labelPosition());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to parse geo shape label position: " + e.getMessage(), e);
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isEmpty() {
            return value == null;
        }

        @Override
        public int size() {
            return value == null ? 0 : 1;
        }

        public CartesianShapeValues.CartesianShapeValue get(CartesianShapeValues.CartesianShapeValue defaultValue) {
            return get(0, defaultValue);
        }

        public CartesianShapeValues.CartesianShapeValue get(int index, CartesianShapeValues.CartesianShapeValue defaultValue) {
            if (isEmpty() || index != 0) {
                return defaultValue;
            }

            return value;
        }

        @Override
        public Iterator<CartesianShapeValues.CartesianShapeValue> iterator() {
            return new Iterator<>() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < size();
                }

                @Override
                public CartesianShapeValues.CartesianShapeValue next() {
                    if (hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return value;
                }
            };
        }
    }
}
