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

import org.apache.lucene.document.LatLonBoundingBox;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.geo.GeoUtils.MAX_LON;
import static org.elasticsearch.common.geo.GeoUtils.MIN_LON;
import static org.elasticsearch.common.geo.GeoUtils.parseBoundingBox;
import static org.elasticsearch.index.mapper.TypeParsers.parseField;
import static org.elasticsearch.common.geo.GeoUtils.rectangleToJson;

/**
 * Field mapper for geo_bounding_box field types.
 */
public class GeoBoundingBoxFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "geo_bounding_box";
    public static final Version SUPPORTED_IN_VERSION = Version.V_6_2_0;

    public static class Defaults {
        public static final GeoBoundingBoxFieldType FIELD_TYPE = new GeoBoundingBoxFieldType();
        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
            FIELD_TYPE.setHasDocValues(false);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, GeoBoundingBoxFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public GeoBoundingBoxFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                               MappedFieldType defaultFieldType, Settings indexSettings, MultiFields multiFields,
                                               CopyTo copyTo) {
            setupFieldType(context);
            return new GeoBoundingBoxFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        }

        @Override
        public GeoBoundingBoxFieldMapper build(BuilderContext context) {
            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo);
        }

        /** todo add support for docValues */
        @Override
        public Builder docValues(boolean docValues) {
            if (docValues == true) {
                throw new IllegalArgumentException("field [" + name + "] does not currently support " + TypeParsers.DOC_VALUES);
            }
            return super.docValues(docValues);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new GeoBoundingBoxFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    public GeoBoundingBoxFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                     Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        context.path().add(simpleName());
        Rectangle rect = context.parseExternalValue(Rectangle.class);
        try {
            if (rect == null) {
                rect = parseBoundingBox(context.parser());
            }
            indexFields(context, rect);
        } catch (Exception e) {
            throw new ElasticsearchParseException("failed to index [{}] field. [{}]", name(), e.getMessage());
        }

        context.path().remove();
        return null;
    }

    protected void indexFields(final ParseContext context, final Rectangle rect) throws IOException {
        if (fieldType().indexOptions() != IndexOptions.NONE) {
            if (rect.crossesDateline()) {
                throw new ElasticsearchParseException("failed to index [{}] field. Dateline crossing bounding boxes "
                    + "are not supported", name());
            } else {
                context.doc().add(new LatLonBoundingBox(name(), rect.minLat, rect.minLon, rect.maxLat, rect.maxLon));
            }
        }
        if (fieldType().stored()) {
            // todo: With the exception of CRS, there is no official BBOX or RECT geometry type in the GeoJSON or WKT RFC
            // For now we use the ES string representation of a bounding_box so that it can be parsed by
            // GeoUtils.parseBoundingBox; there could be other ways
            context.doc().add(new StoredField(fieldType().name(), rectangleToJson(rect)));
        }
        if (fieldType().stored() || fieldType().indexOptions() != IndexOptions.NONE) {
            List<IndexableField> fields = new ArrayList<>(1);
            createFieldNamesField(context, fields);
            for (IndexableField field : fields) {
                context.doc().add(field);
            }
        }
        // todo: add multifields support
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static final class GeoBoundingBoxFieldType extends MappedFieldType {
        GeoBoundingBoxFieldType() {
        }

        protected GeoBoundingBoxFieldType(GeoBoundingBoxFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new GeoBoundingBoxFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean hasDocValues() {
            return false;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Geo fields do not support exact searching, use dedicated geo queries instead: ["
                + name() + "]");
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
                                ShapeRelation relation, DateTimeZone timeZone, DateMathParser parser, QueryShardContext context) {
            return newLatLonBBoxQuery(context, (GeoPoint)lowerTerm, (GeoPoint)upperTerm, relation);
        }

        private boolean crossesDateline(final GeoPoint topLeft, final GeoPoint bottomRight) {
            return bottomRight.lon() < topLeft.lon();
        }

        private boolean touchesDateline(final GeoPoint topLeft, final GeoPoint bottomRight) {
            return topLeft.lon() == MIN_LON || bottomRight.lon() == MAX_LON;
        }

        private Query newLatLonBBoxQuery(QueryShardContext context, GeoPoint topLeft, GeoPoint bottomRight,
                                         ShapeRelation shapeRelation) {
            ShapeRelation relation = shapeRelation == null ? ShapeRelation.INTERSECTS : shapeRelation;
            switch (relation) {
                case INTERSECTS: return newIntersectsQuery(topLeft, bottomRight, relation);
                case CONTAINS: return newContainsQuery(topLeft, bottomRight, relation);
                case WITHIN: return newWithinQuery(topLeft, bottomRight, relation);
                case DISJOINT: return newDisjointQuery(context, topLeft, bottomRight);
                default: throw new ElasticsearchException("[{}] query does not support relation [{}]",
                    GeoBoundingBoxQueryBuilder.NAME, relation);
            }
        }

        /** build the lucene query from {@link LatLonBoundingBox} helper methods */
        private Query newBBoxQuery(final String field, final double minLat, final double minLon,
                                   final double maxLat, final double maxLon, final ShapeRelation relation) {
            ShapeRelation r = relation == null || relation.equals(ShapeRelation.DISJOINT) ? ShapeRelation.INTERSECTS : relation;
            switch(r) {
                case INTERSECTS:
                    return LatLonBoundingBox.newIntersectsQuery(field, minLat, minLon, maxLat, maxLon);
                case CONTAINS:
                    return LatLonBoundingBox.newContainsQuery(field, minLat, minLon, maxLat, maxLon);
                case WITHIN:
                    return LatLonBoundingBox.newWithinQuery(field, minLat, minLon, maxLat, maxLon);
                case CROSSES:
                    return LatLonBoundingBox.newCrossesQuery(field, minLat, minLon, maxLat, maxLon);
                default:
                    throw new IllegalArgumentException("[" + name() + "] query does not support relation [" + r + "]");
            }
        }

        private Query newXDLQuery(final GeoPoint topLeft, final GeoPoint bottomRight, ShapeRelation relation,
                BooleanClause.Occur occur) {
            BooleanQuery.Builder bqb = new BooleanQuery.Builder();
            bqb.add(newBBoxQuery(name(), bottomRight.lat(), topLeft.lon(), topLeft.lat(), GeoUtils.MAX_LON, relation), occur);
            bqb.add(newBBoxQuery(name(), bottomRight.lat(), GeoUtils.MIN_LON, topLeft.lat(), bottomRight.lon(), relation), occur);
            return bqb.build();
        }

        /**
         * build a query that handles dateline touching, e.g. an indexed box with lon -180 should intersect a query
         * with bottomRight.lon == 180 since the dateline touches
         */
        private Query newTouchDLQuery(final GeoPoint topLeft, final GeoPoint bottomRight, ShapeRelation relation,
                                      BooleanClause.Occur occur) {
            // note this does not solve the case where a user specifies a lon range of 180.0 : -179.0.
            // perhaps coerce parameter can do that, or we just tell the user to correct it
            BooleanQuery.Builder bqb = new BooleanQuery.Builder();
            bqb.add(newBBoxQuery(name(), bottomRight.lat(), topLeft.lon(), topLeft.lat(), bottomRight.lon(), relation), occur);
            if (topLeft.lon() == MIN_LON) {
                bqb.add(newBBoxQuery(name(), bottomRight.lat(), MAX_LON, topLeft.lat(), MAX_LON, relation), occur);
            } else if (bottomRight.lon() == MAX_LON) {
                bqb.add(newBBoxQuery(name(), bottomRight.lat(), MIN_LON, topLeft.lat(), MIN_LON, relation), occur);
            }
            return bqb.build();
        }

        private Query newIntersectsQuery(final GeoPoint topLeft, final GeoPoint bottomRight, final ShapeRelation relation) {
            return newIntersectsQuery(topLeft, bottomRight, relation, BooleanClause.Occur.SHOULD);
        }

        private Query newIntersectsQuery(final GeoPoint topLeft, final GeoPoint bottomRight, final ShapeRelation relation,
                                         BooleanClause.Occur occur) {
            if (crossesDateline(topLeft, bottomRight)) {
                return newXDLQuery(topLeft, bottomRight, relation, occur);
            } else if (touchesDateline(topLeft, bottomRight)) {
                return newTouchDLQuery(topLeft, bottomRight, relation, occur);
            }
            return newBBoxQuery(name(), bottomRight.lat(), topLeft.lon(), topLeft.lat(), bottomRight.lon(), relation);
        }

        private Query newContainsQuery(final GeoPoint topLeft, final GeoPoint bottomRight, final ShapeRelation relation) {
            if (crossesDateline(topLeft, bottomRight)) {
                throw new ElasticsearchParseException("[{}] relation not supported for dateline crossing "
                    + "bounding box queries", relation);
            }
            return newBBoxQuery(name(), bottomRight.lat(), topLeft.lon(), topLeft.lat(), bottomRight.lon(), relation);
        }

        private Query newWithinQuery(final GeoPoint topLeft, final GeoPoint bottomRight, final ShapeRelation relation) {
            if (crossesDateline(topLeft, bottomRight)) {
                BooleanQuery.Builder bqb = new BooleanQuery.Builder();
                bqb.add(newBBoxQuery(name(), bottomRight.lat(), topLeft.lon(), topLeft.lat(), MAX_LON, relation),
                    BooleanClause.Occur.SHOULD);
                bqb.add(newBBoxQuery(name(), bottomRight.lat(), MIN_LON, topLeft.lat(), bottomRight.lon(), relation),
                    BooleanClause.Occur.SHOULD);
                return bqb.build();
            }
            return newBBoxQuery(name(), bottomRight.lat(), topLeft.lon(), topLeft.lat(), bottomRight.lon(), relation);
        }

        private Query newDisjointQuery(QueryShardContext context, final GeoPoint topLeft, final GeoPoint bottomRight) {
            BooleanQuery.Builder bqb = new BooleanQuery.Builder();
            bqb.add(ExistsQueryBuilder.newFilter(context, name()), BooleanClause.Occur.MUST);
            bqb.add(LatLonBoundingBox.newIntersectsQuery(name(), bottomRight.lat(), topLeft.lon(), topLeft.lat(),
                bottomRight.lon()), BooleanClause.Occur.MUST_NOT);
            return bqb.build();
        }
    }
}
