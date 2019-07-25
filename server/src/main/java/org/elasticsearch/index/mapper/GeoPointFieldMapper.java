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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointDVIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

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
public class GeoPointFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "geo_point";

    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
        public static final ParseField IGNORE_Z_VALUE = new ParseField("ignore_z_value");
        public static final String NULL_VALUE = "null_value";
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final GeoPointFieldType FIELD_TYPE = new GeoPointFieldType();
        public static final Explicit<Boolean> IGNORE_Z_VALUE = new Explicit<>(true, false);

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setDimensions(2, Integer.BYTES);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, GeoPointFieldMapper> {
        protected Boolean ignoreMalformed;
        private Boolean ignoreZValue;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return GeoPointFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        protected Explicit<Boolean> ignoreZValue(BuilderContext context) {
            if (ignoreZValue != null) {
                return new Explicit<>(ignoreZValue, true);
            }
            return Defaults.IGNORE_Z_VALUE;
        }

        public Builder ignoreZValue(final boolean ignoreZValue) {
            this.ignoreZValue = ignoreZValue;
            return this;
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
        public GeoPointFieldMapper build(BuilderContext context) {
            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), ignoreMalformed(context),
                ignoreZValue(context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new GeoPointFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            Object nullValue = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (propName.equals(Names.IGNORE_MALFORMED)) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + "." + Names.IGNORE_MALFORMED));
                    iterator.remove();
                } else if (propName.equals(Names.IGNORE_Z_VALUE.getPreferredName())) {
                    builder.ignoreZValue(XContentMapValues.nodeBooleanValue(propNode,
                            name + "." + Names.IGNORE_Z_VALUE.getPreferredName()));
                    iterator.remove();
                } else if (propName.equals(Names.NULL_VALUE)) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    nullValue = propNode;
                    iterator.remove();
                }
            }

            if (nullValue != null) {
                boolean ignoreZValue = builder.ignoreZValue == null ? Defaults.IGNORE_Z_VALUE.value() : builder.ignoreZValue;
                boolean ignoreMalformed = builder.ignoreMalformed == null ? Defaults.IGNORE_MALFORMED.value() : builder.ignoreZValue;
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

    protected Explicit<Boolean> ignoreMalformed;
    protected Explicit<Boolean> ignoreZValue;

    public GeoPointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                               Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                               Explicit<Boolean> ignoreZValue, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        GeoPointFieldMapper gpfmMergeWith = (GeoPointFieldMapper) mergeWith;
        if (gpfmMergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gpfmMergeWith.ignoreMalformed;
        }
        if (gpfmMergeWith.ignoreZValue.explicit()) {
            this.ignoreZValue = gpfmMergeWith.ignoreZValue;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class GeoPointFieldType extends MappedFieldType {
        public GeoPointFieldType() {
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
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
        if (includeDefaults || ignoreZValue.explicit()) {
            builder.field(Names.IGNORE_Z_VALUE.getPreferredName(), ignoreZValue.value());
        }

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field(Names.NULL_VALUE, fieldType().nullValue());
        }
    }

    public Explicit<Boolean> ignoreZValue() {
        return ignoreZValue;
    }

    private boolean isNormalizable(double coord) {
        return Double.isNaN(coord) == false && Double.isInfinite(coord) == false;
    }
}
