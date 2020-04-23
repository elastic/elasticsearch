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

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointDVIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.VectorGeoPointShapeQueryProcessor;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * Field Mapper for geo_point types.
 *
 * Uses lucene 6 LatLonPoint encoding
 */
public class GeoPointFieldMapper extends AbstractGeometryFieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "geo_point";

    public static class Names extends AbstractGeometryFieldMapper.Names {
        public static final ParseField NULL_VALUE = new ParseField("null_value");
    }

    public static class Builder extends AbstractGeometryFieldMapper.Builder<Builder, GeoPointFieldMapper> {
        public Builder(String name) {
            super(name, new GeoPointFieldType(), new GeoPointFieldType());
            builder = this;
        }

        public GeoPointFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                         MappedFieldType defaultFieldType, Settings indexSettings,
                                         MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                         Explicit<Boolean> ignoreZValue, CopyTo copyTo) {
            setupFieldType(context);
            return new GeoPointFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, multiFields,
                ignoreMalformed, ignoreZValue, copyTo);
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            GeoPointFieldType fieldType = (GeoPointFieldType)fieldType();
            fieldType.setGeometryQueryBuilder(new VectorGeoPointShapeQueryProcessor());
        }

        @Override
        public GeoPointFieldMapper build(BuilderContext context) {
            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), ignoreMalformed(context),
                ignoreZValue(context), copyTo);
        }

        @Override
        public GeoPointFieldType fieldType() {
            return (GeoPointFieldType)fieldType;
        }
    }

    public static class TypeParser extends AbstractGeometryFieldMapper.TypeParser<Builder> {
        @Override
        protected Builder newBuilder(String name, Map<String, Object> params) {
            return new GeoPointFieldMapper.Builder(name);
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, Map<String, Object> params, ParserContext parserContext) {
            Builder builder = super.parse(name, node, params, parserContext);
            parseField(builder, name, node, parserContext);
            Object nullValue = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (Names.NULL_VALUE.match(propName, LoggingDeprecationHandler.INSTANCE)) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    nullValue = propNode;
                    iterator.remove();
                }
            }

            if (nullValue != null) {
                boolean ignoreZValue = builder.ignoreZValue().value();
                boolean ignoreMalformed = builder.ignoreMalformed().value();
                GeoPoint point = GeoUtils.parseGeoPoint(nullValue, ignoreZValue);
                if (ignoreMalformed == false) {
                    if (point.lat() > 90.0 || point.lat() < -90.0) {
                        throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "]");
                    }
                    if (point.lon() > 180.0 || point.lon() < -180) {
                        throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "]");
                    }
                } else {
                    GeoUtils.normalizePoint(point);
                }
                builder.nullValue(point);
            }
            return builder;
        }
    }

    public GeoPointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                               Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                               Explicit<Boolean> ignoreZValue, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, ignoreMalformed, ignoreZValue, multiFields, copyTo);
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class GeoPointFieldType extends AbstractGeometryFieldType {
        public GeoPointFieldType() {
            super();
            setHasDocValues(true);
            setDimensions(2, Integer.BYTES);
        }

        GeoPointFieldType(GeoPointFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public MappedFieldType clone() {
            return new GeoPointFieldType(this);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new AbstractLatLonPointDVIndexFieldData.Builder();
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return CoreValuesSourceType.GEOPOINT;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return super.existsQuery(context);
            }
        }
    }

    protected void parse(ParseContext context, GeoPoint point) throws IOException {

        if (ignoreMalformed.value() == false) {
            if (point.lat() > 90.0 || point.lat() < -90.0) {
                throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
            }
            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
            }
        } else {
            if (isNormalizable(point.lat()) && isNormalizable(point.lon())) {
                GeoUtils.normalizePoint(point);
            } else {
                throw new ElasticsearchParseException("cannot normalize the point - not a number");
            }
        }
        if (fieldType().indexOptions() != IndexOptions.NONE) {
            context.doc().add(new LatLonPoint(fieldType().name(), point.lat(), point.lon()));
        }
        if (fieldType().stored()) {
            context.doc().add(new StoredField(fieldType().name(), point.toString()));
        }
        if (fieldType.hasDocValues()) {
            context.doc().add(new LatLonDocValuesField(fieldType().name(), point.lat(), point.lon()));
        } else if (fieldType().stored() || fieldType().indexOptions() != IndexOptions.NONE) {
            List<IndexableField> fields = new ArrayList<>(1);
            createFieldNamesField(context, fields);
            for (IndexableField field : fields) {
                context.doc().add(field);
            }
        }
        // if the mapping contains multifields then use the geohash string
        if (multiFields.iterator().hasNext()) {
            multiFields.parse(this, context.createExternalValueContext(point.geohash()));
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        context.path().add(simpleName());

        try {
            GeoPoint sparse = context.parseExternalValue(GeoPoint.class);

            if (sparse != null) {
                parse(context, sparse);
            } else {
                sparse = new GeoPoint();
                XContentParser.Token token = context.parser().currentToken();
                if (token == XContentParser.Token.START_ARRAY) {
                    token = context.parser().nextToken();
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        double lon = context.parser().doubleValue();
                        context.parser().nextToken();
                        double lat = context.parser().doubleValue();
                        token = context.parser().nextToken();
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            GeoPoint.assertZValue(ignoreZValue.value(), context.parser().doubleValue());
                        } else if (token != XContentParser.Token.END_ARRAY) {
                            throw new ElasticsearchParseException("[{}] field type does not accept > 3 dimensions", CONTENT_TYPE);
                        }
                        parse(context, sparse.reset(lat, lon));
                    } else {
                        while (token != XContentParser.Token.END_ARRAY) {
                            parseGeoPointIgnoringMalformed(context, sparse);
                            token = context.parser().nextToken();
                        }
                    }
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    if (fieldType.nullValue() != null) {
                        parse(context, (GeoPoint) fieldType.nullValue());
                    }
                } else {
                    parseGeoPointIgnoringMalformed(context, sparse);
                }
            }
        } catch (Exception ex) {
            throw new MapperParsingException("failed to parse field [{}] of type [{}]", ex, fieldType().name(), fieldType().typeName());
        }

        context.path().remove();
    }

    /**
     * Parses geopoint represented as an object or an array, ignores malformed geopoints if needed
     */
    private void parseGeoPointIgnoringMalformed(ParseContext context, GeoPoint sparse) throws IOException {
        try {
            parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse, ignoreZValue.value()));
        } catch (ElasticsearchParseException e) {
            if (ignoreMalformed.value() == false) {
                throw e;
            }
            context.addIgnoredField(fieldType.name());
        }
    }

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field(Names.NULL_VALUE.getPreferredName(), fieldType().nullValue());
        }
    }

    private boolean isNormalizable(double coord) {
        return Double.isNaN(coord) == false && Double.isInfinite(coord) == false;
    }
}
