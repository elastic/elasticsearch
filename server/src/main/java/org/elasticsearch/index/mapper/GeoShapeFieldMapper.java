/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.QueryShardException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
public class GeoShapeFieldMapper extends AbstractShapeGeometryFieldMapper<Geometry, Geometry> {

    public static final String CONTENT_TYPE = "geo_shape";

    private static Builder builder(FieldMapper in) {
        return ((GeoShapeFieldMapper)in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<Explicit<Boolean>> coerce;
        final Parameter<Explicit<Orientation>> orientation = orientationParam(m -> builder(m).orientation.get());

        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, boolean ignoreMalformedByDefault, boolean coerceByDefault) {
            super (name);
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.coerce = coerceParam(m -> builder(m).coerce.get(), coerceByDefault);
        }

        public Builder ignoreZValue(boolean ignoreZValue) {
            this.ignoreZValue.setValue(new Explicit<>(ignoreZValue, true));
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(indexed, ignoreMalformed, ignoreZValue, coerce, orientation, meta);
        }

        @Override
        public GeoShapeFieldMapper build(ContentPath contentPath) {
            GeometryParser geometryParser = new GeometryParser(
                orientation.get().value().getAsBoolean(),
                coerce.get().value(),
                ignoreZValue.get().value());
            GeoShapeParser geoShapeParser = new GeoShapeParser(geometryParser);
            GeoShapeFieldType ft = new GeoShapeFieldType(
                buildFullName(contentPath),
                indexed.get(),
                orientation.get().value(),
                geoShapeParser,
                meta.get());
            return new GeoShapeFieldMapper(name, ft, multiFieldsBuilder.build(this, contentPath), copyTo.build(),
                new GeoShapeIndexer(orientation.get().value().getAsBoolean(), buildFullName(contentPath)),
                geoShapeParser, this);
        }
    }

    public static class GeoShapeFieldType extends AbstractShapeGeometryFieldType implements GeoShapeQueryable {

        public GeoShapeFieldType(String name, boolean indexed, Orientation orientation,
                                 Parser<Geometry> parser, Map<String, String> meta) {
            super(name, indexed, false, false, false, parser, orientation, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0)
            if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(Version.V_7_5_0)) {
                throw new QueryShardException(context,
                    ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "].");
            }
            final LatLonGeometry[] luceneGeometries = GeoShapeUtils.toLuceneGeometry(fieldName, context, shape, relation);
            if (luceneGeometries.length == 0) {
                return new MatchNoDocsQuery();
            }
            return LatLonShape.newGeometryQuery(fieldName, relation.getLuceneRelation(), luceneGeometries);
        }
    }

    @SuppressWarnings("deprecation")
    public static Mapper.TypeParser PARSER = (name, node, parserContext) -> {
        FieldMapper.Builder builder;
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(parserContext.getSettings());
        boolean coerceByDefault = COERCE_SETTING.get(parserContext.getSettings());
        if (LegacyGeoShapeFieldMapper.containsDeprecatedParameter(node.keySet())) {
            builder = new LegacyGeoShapeFieldMapper.Builder(
                name,
                parserContext.indexVersionCreated(),
                ignoreMalformedByDefault,
                coerceByDefault);
        } else {
            builder = new Builder(name, ignoreMalformedByDefault, coerceByDefault);
        }
        builder.parse(name, parserContext, node);
        return builder;
    };

    private final Builder builder;

    public GeoShapeFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                               MultiFields multiFields, CopyTo copyTo,
                               Indexer<Geometry, Geometry> indexer, Parser<Geometry> parser, Builder builder) {
        super(simpleName,
            mappedFieldType,
            builder.ignoreMalformed.get(),
            builder.coerce.get(),
            builder.ignoreZValue.get(),
            builder.orientation.get(),
            multiFields,
            copyTo,
            indexer,
            parser);
        this.builder = builder;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(
            simpleName(),
            builder.ignoreMalformed.getDefaultValue().value(),
            builder.coerce.getDefaultValue().value()
        ).init(this);
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void checkIncomingMergeType(FieldMapper mergeWith) {
        if (mergeWith instanceof LegacyGeoShapeFieldMapper) {
            String strategy = ((LegacyGeoShapeFieldMapper)mergeWith).strategy();
            throw new IllegalArgumentException("mapper [" + name()
                + "] of type [geo_shape] cannot change strategy from [BKD] to [" + strategy + "]");
        }
        super.checkIncomingMergeType(mergeWith);
    }

    @Override
    protected void addStoredFields(ParseContext context, Geometry geometry) {
        // noop: we currently do not store geo_shapes
        // @todo store as geojson string?
    }

    @Override
    protected void addDocValuesFields(String name, Geometry geometry, List<IndexableField> fields, ParseContext context) {
        // we will throw a mapping exception before we get here
    }

    @Override
    protected void addMultiFields(ParseContext context, Geometry geometry) {
        // noop (completion suggester currently not compatible with geo_shape)
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
