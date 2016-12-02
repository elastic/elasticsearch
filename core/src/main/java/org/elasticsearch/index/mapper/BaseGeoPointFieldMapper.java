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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractGeoPointDVIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * GeoPointFieldMapper base class to maintain backward compatibility
 */
public abstract class BaseGeoPointFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "geo_point";
    protected static final DeprecationLogger deprecationLogger = new DeprecationLogger(Loggers.getLogger(BaseGeoPointFieldMapper.class));
    public static class Names {
        public static final String LAT = "lat";
        public static final String LAT_SUFFIX = "." + LAT;
        public static final String LON = "lon";
        public static final String LON_SUFFIX = "." + LON;
        public static final String GEOHASH = "geohash";
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults {
        public static final boolean ENABLE_LATLON = false;
        public static final boolean ENABLE_GEOHASH = false;
        public static final boolean ENABLE_GEOHASH_PREFIX = false;
        public static final int GEO_HASH_PRECISION = GeoHashUtils.PRECISION;
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
    }

    public abstract static class Builder<T extends Builder, Y extends BaseGeoPointFieldMapper> extends FieldMapper.Builder<T, Y> {

        protected Boolean ignoreMalformed;

        public Builder(String name, MappedFieldType fieldType) {
            super(name, fieldType, fieldType);
        }

        @Override
        public GeoPointFieldType fieldType() {
            return (GeoPointFieldType)fieldType;
        }

        public T ignoreMalformed(boolean ignoreMalformed) {
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
            return Defaults.IGNORE_MALFORMED;
        }

        public abstract Y build(BuilderContext context, String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, FieldMapper latMapper, FieldMapper lonMapper,
                                FieldMapper geoHashMapper, MultiFields multiFields, Explicit<Boolean> ignoreMalformed, CopyTo copyTo);

        public Y build(Mapper.BuilderContext context) {
            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                null, null, null, multiFieldsBuilder.build(this, context), ignoreMalformed(context), copyTo);
        }
    }

    public abstract static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder<?, ?> builder = new LatLonPointFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (propName.equals(Names.IGNORE_MALFORMED)) {
                    builder.ignoreMalformed(XContentMapValues.lenientNodeBooleanValue(propNode));
                    iterator.remove();
                }
            }

            return builder;
        }
    }

    public static class GeoPointFieldType extends MappedFieldType {

        GeoPointFieldType() {}

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
        public IndexFieldData.Builder fielddataBuilder() {
            return new AbstractGeoPointDVIndexFieldData.Builder();
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, DateTimeZone timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName()
                    + "] does not support custom time zones");
            }
            return DocValueFormat.GEOHASH;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Geo fields do not support exact searching, use dedicated geo queries instead: [" + name() + "]");
        }

        @Override
        public FieldStats.GeoPoint stats(IndexReader reader) throws IOException {
            String field = name();
            FieldInfo fi = org.apache.lucene.index.MultiFields.getMergedFieldInfos(reader).fieldInfo(field);
            if (fi == null) {
                return null;
            }

            Terms terms = org.apache.lucene.index.MultiFields.getTerms(reader, field);
            if (terms == null) {
                return new FieldStats.GeoPoint(reader.maxDoc(), 0L, -1L, -1L, isSearchable(), isAggregatable());
            }
            GeoPoint minPt = GeoPoint.fromGeohash(NumericUtils.sortableBytesToLong(terms.getMin().bytes, terms.getMin().offset));
            GeoPoint maxPt = GeoPoint.fromGeohash(NumericUtils.sortableBytesToLong(terms.getMax().bytes, terms.getMax().offset));
            return new FieldStats.GeoPoint(reader.maxDoc(), terms.getDocCount(), -1L, terms.getSumTotalTermFreq(), isSearchable(),
                isAggregatable(), minPt, maxPt);
        }
    }

    protected Explicit<Boolean> ignoreMalformed;

    protected BaseGeoPointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings,
                                      FieldMapper latMapper, FieldMapper lonMapper, FieldMapper geoHashMapper,
                                      MultiFields multiFields, Explicit<Boolean> ignoreMalformed, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        BaseGeoPointFieldMapper gpfmMergeWith = (BaseGeoPointFieldMapper) mergeWith;
        if (gpfmMergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gpfmMergeWith.ignoreMalformed;
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

    protected void parse(ParseContext context, GeoPoint point, String geoHash) throws IOException {
        multiFields.parse(this, context.createExternalValueContext(point));
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        context.path().add(simpleName());

        GeoPoint sparse = context.parseExternalValue(GeoPoint.class);

        if (sparse != null) {
            parse(context, sparse, null);
        } else {
            sparse = new GeoPoint();
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                token = context.parser().nextToken();
                if (token == XContentParser.Token.START_ARRAY) {
                    // its an array of array of lon/lat [ [1.2, 1.3], [1.4, 1.5] ]
                    while (token != XContentParser.Token.END_ARRAY) {
                      try {
                          parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                      } catch (ElasticsearchParseException e) {
                          if (ignoreMalformed.value() == false) {
                              throw e;
                          }
                      }
                      token = context.parser().nextToken();
                    }
                } else {
                    // its an array of other possible values
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        double lon = context.parser().doubleValue();
                        token = context.parser().nextToken();
                        double lat = context.parser().doubleValue();
                        while ((token = context.parser().nextToken()) != XContentParser.Token.END_ARRAY);
                        parse(context, sparse.reset(lat, lon), null);
                    } else {
                        while (token != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                parsePointFromString(context, sparse, context.parser().text());
                            } else {
                                try {
                                    parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                                } catch (ElasticsearchParseException e) {
                                    if (ignoreMalformed.value() == false) {
                                        throw e;
                                    }
                                }
                            }
                          token = context.parser().nextToken();
                        }
                    }
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                parsePointFromString(context, sparse, context.parser().text());
            } else if (token != XContentParser.Token.VALUE_NULL) {
                try {
                    parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                } catch (ElasticsearchParseException e) {
                    if (ignoreMalformed.value() == false) {
                        throw e;
                    }
                }
            }
        }

        context.path().remove();
        return null;
    }

    private void parsePointFromString(ParseContext context, GeoPoint sparse, String point) throws IOException {
        if (point.indexOf(',') < 0) {
            parse(context, sparse.resetFromGeoHash(point), point);
        } else {
            parse(context, sparse.resetFromString(point), null);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
    }

}
