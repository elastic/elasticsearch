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
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryProcessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(GeoShapeFieldMapper.class);

    private static Builder builder(FieldMapper in) {
        return ((ShapeFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
        final Parameter<Boolean> hasDocValues;

        private final Version version;
        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<Explicit<Boolean>> coerce;
        final Parameter<Explicit<Orientation>> orientation = orientationParam(m -> builder(m).orientation.get());

        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, Version version, boolean ignoreMalformedByDefault, boolean coerceByDefault) {
            super(name);
            this.version = version;
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.coerce = coerceParam(m -> builder(m).coerce.get(), coerceByDefault);
            this.hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), Version.V_8_4_0.onOrBefore(version));
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
                context.buildFullName(name),
                indexed.get(),
                hasDocValues.get(),
                orientation.get().value(),
                parser,
                meta.get()
            );
            return new ShapeFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), parser, this);
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

        private final ShapeQueryProcessor queryProcessor;

        public ShapeFieldType(
            String name,
            boolean indexed,
            boolean hasDocValues,
            Orientation orientation,
            Parser<Geometry> parser,
            Map<String, String> meta
        ) {
            super(name, indexed, false, hasDocValues, parser, orientation, meta);
            this.queryProcessor = new ShapeQueryProcessor();
        }

        @Override
        public Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            return queryProcessor.shapeQuery(shape, fieldName, relation, context, hasDocValues());
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
    protected void index(DocumentParserContext context, Geometry geometry) throws IOException {
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
            simpleName(),
            builder.version,
            builder.ignoreMalformed.getDefaultValue().value(),
            builder.coerce.getDefaultValue().value()
        ).init(this);
    }

    @Override
    public ShapeFieldType fieldType() {
        return (ShapeFieldType) super.fieldType();
    }
}
