/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * FieldMapper for indexing {@link LatLonShape}s.
 * <p>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeQueryBuilder}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))
 */
public class GeoShapeFieldMapper extends AbstractShapeGeometryFieldMapper<Geometry> {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(GeoShapeFieldMapper.class);

    public static final String CONTENT_TYPE = "geo_shape";

    private static Builder builder(FieldMapper in) {
        return ((GeoShapeFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<Explicit<Boolean>> coerce;
        final Parameter<Explicit<Orientation>> orientation = orientationParam(m -> builder(m).orientation.get());

        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, boolean ignoreMalformedByDefault, boolean coerceByDefault) {
            super(name);
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.coerce = coerceParam(m -> builder(m).coerce.get(), coerceByDefault);
        }

        public Builder ignoreZValue(boolean ignoreZValue) {
            this.ignoreZValue.setValue(Explicit.explicitBoolean(ignoreZValue));
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(indexed, ignoreMalformed, ignoreZValue, coerce, orientation, meta);
        }

        @Override
        public GeoShapeFieldMapper build(MapperBuilderContext context) {
            if (multiFieldsBuilder.hasMultiFields()) {
                DEPRECATION_LOGGER.warn(
                    DeprecationCategory.MAPPINGS,
                    "geo_shape_multifields",
                    "Adding multifields to [geo_shape] mappers has no effect and will be forbidden in future"
                );
            }
            GeometryParser geometryParser = new GeometryParser(
                orientation.get().value().getAsBoolean(),
                coerce.get().value(),
                ignoreZValue.get().value()
            );
            GeoShapeParser geoShapeParser = new GeoShapeParser(geometryParser, orientation.get().value());
            GeoShapeFieldType ft = new GeoShapeFieldType(
                context.buildFullName(name),
                indexed.get(),
                orientation.get().value(),
                geoShapeParser,
                meta.get()
            );
            return new GeoShapeFieldMapper(
                name,
                ft,
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                new GeoShapeIndexer(orientation.get().value(), context.buildFullName(name)),
                geoShapeParser,
                this
            );
        }
    }

    public static class GeoShapeFieldType extends AbstractShapeGeometryFieldType<Geometry> implements GeoShapeQueryable {

        public GeoShapeFieldType(String name, boolean indexed, Orientation orientation, Parser<Geometry> parser, Map<String, String> meta) {
            super(name, indexed, false, false, parser, orientation, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0)
            if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(Version.V_7_5_0)) {
                throw new QueryShardException(
                    context,
                    ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "]."
                );
            }
            final LatLonGeometry[] luceneGeometries = GeoShapeUtils.toLuceneGeometry(fieldName, context, shape, relation);
            if (luceneGeometries.length == 0) {
                return new MatchNoDocsQuery();
            }
            return LatLonShape.newGeometryQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
        }

        @Override
        protected Function<List<Geometry>, List<Object>> getFormatter(String format) {
            return GeometryFormatterFactory.getFormatter(format, Function.identity());
        }
    }

    @Deprecated
    public static Mapper.TypeParser PARSER = (name, node, parserContext) -> {
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(parserContext.getSettings());
        boolean coerceByDefault = COERCE_SETTING.get(parserContext.getSettings());
        FieldMapper.Builder builder = new Builder(name, ignoreMalformedByDefault, coerceByDefault);
        builder.parse(name, parserContext, node);
        return builder;
    };

    private final Builder builder;
    private final GeoShapeIndexer indexer;

    public GeoShapeFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        GeoShapeIndexer indexer,
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
        this.indexer = indexer;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), builder.ignoreMalformed.getDefaultValue().value(), builder.coerce.getDefaultValue().value()).init(
            this
        );
    }

    @Override
    protected void index(DocumentParserContext context, Geometry geometry) throws IOException {
        if (geometry == null) {
            return;
        }
        context.doc().addAll(indexer.indexShape(geometry));
        context.addToFieldNames(fieldType().name());
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
