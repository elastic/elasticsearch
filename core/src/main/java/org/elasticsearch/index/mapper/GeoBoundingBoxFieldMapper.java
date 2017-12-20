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
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.geo.GeoUtils.parseBoundingBox;
import static org.elasticsearch.index.mapper.TypeParsers.parseField;
import static org.elasticsearch.common.geo.GeoUtils.rectangleToJson;

/**
 * Field mapper for geo_bounding_box field types.
 */
public class GeoBoundingBoxFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "geo_bounding_box";
    public static final Version SUPPORTED_IN_VERSION = Version.V_6_1_0;
    public static final String FIELD_XDL_SUFFIX = "__xdl";

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
                indexXDL(context, rect);
            } else if (rect.minLon == -180D && rect.maxLon == 180D) {
                indexFullLonRange(context, rect);
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

    private void indexXDL(final ParseContext context, final Rectangle rect) throws IOException {
        // index western bbox:
        context.doc().add(new LatLonBoundingBox(name() + FIELD_XDL_SUFFIX,
            rect.minLat, GeoUtils.MIN_LON, rect.maxLat, rect.maxLon));
        // index eastern bbox:
        context.doc().add(new LatLonBoundingBox(name(), rect.minLat, rect.minLon, rect.maxLat, GeoUtils.MAX_LON));
    }

    private void indexFullLonRange(final ParseContext context, final Rectangle rect) throws IOException {
        // index western bbox:
        context.doc().add(new LatLonBoundingBox(name() + FIELD_XDL_SUFFIX,
            rect.minLat, GeoUtils.MIN_LON, rect.maxLat, GeoUtils.MAX_LON));
        // index eastern bbox:
        context.doc().add(new LatLonBoundingBox(name(), rect.minLat, GeoUtils.MIN_LON, rect.maxLat, GeoUtils.MAX_LON));
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
    }
}
