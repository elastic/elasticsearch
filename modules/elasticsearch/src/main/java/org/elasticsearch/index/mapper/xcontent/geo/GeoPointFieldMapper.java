/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.xcontent.geo;

import org.apache.lucene.document.Field;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.xcontent.*;
import org.elasticsearch.index.search.geo.GeoHashUtils;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * Parsing: We handle:
 *
 * - "field" : "geo_hash"
 * - "field" : "lat,lon"
 * - "field" : {
 * "lat" : 1.1,
 * "lon" : 2.1
 * }
 *
 * @author kimchy (shay.banon)
 */
public class GeoPointFieldMapper implements XContentMapper, ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "geo_point";

    public static class Names {
        public static final String LAT = "lat";
        public static final String LAT_SUFFIX = "." + LAT;
        public static final String LON = "lon";
        public static final String LON_SUFFIX = "." + LON;
        public static final String GEOHASH = "geohash";
        public static final String GEOHASH_SUFFIX = "." + GEOHASH;
    }

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
        public static final Field.Store STORE = Field.Store.NO;
        public static final boolean ENABLE_LATLON = false;
        public static final boolean ENABLE_GEOHASH = false;
        public static final int PRECISION = GeoHashUtils.PRECISION;
    }

    public static class Builder extends XContentMapper.Builder<Builder, GeoPointFieldMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private boolean enableGeoHash = Defaults.ENABLE_GEOHASH;

        private boolean enableLatLon = Defaults.ENABLE_LATLON;

        private Integer precisionStep;

        private int precision = Defaults.PRECISION;

        private Field.Store store = Defaults.STORE;

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder enableGeoHash(boolean enableGeoHash) {
            this.enableGeoHash = enableGeoHash;
            return this;
        }

        public Builder enableLatLon(boolean enableLatLon) {
            this.enableLatLon = enableLatLon;
            return this;
        }

        public Builder precisionStep(int precisionStep) {
            this.precisionStep = precisionStep;
            return this;
        }

        public Builder precision(int precision) {
            this.precision = precision;
            return this;
        }

        public Builder store(Field.Store store) {
            this.store = store;
            return this;
        }

        @Override public GeoPointFieldMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            GeoStringFieldMapper geoStringMapper = new GeoStringFieldMapper.Builder(name)
                    .index(Field.Index.NOT_ANALYZED).omitNorms(true).omitTermFreqAndPositions(true).includeInAll(false).store(store).build(context);


            NumberFieldMapper latMapper = null;
            NumberFieldMapper lonMapper = null;

            context.path().add(name);
            if (enableLatLon) {
                NumberFieldMapper.Builder latMapperBuilder = doubleField(Names.LAT).includeInAll(false);
                NumberFieldMapper.Builder lonMapperBuilder = doubleField(Names.LON).includeInAll(false);
                if (precisionStep != null) {
                    latMapperBuilder.precisionStep(precisionStep);
                    lonMapperBuilder.precisionStep(precisionStep);
                }
                latMapper = (NumberFieldMapper) latMapperBuilder.includeInAll(false).store(store).build(context);
                lonMapper = (NumberFieldMapper) lonMapperBuilder.includeInAll(false).store(store).build(context);
            }
            StringFieldMapper geohashMapper = null;
            if (enableGeoHash) {
                geohashMapper = stringField(Names.GEOHASH).index(Field.Index.NOT_ANALYZED).includeInAll(false).omitNorms(true).omitTermFreqAndPositions(true).build(context);
            }
            context.path().remove();

            context.path().pathType(origPathType);

            return new GeoPointFieldMapper(name, pathType, enableLatLon, enableGeoHash, precisionStep, precision, latMapper, lonMapper, geohashMapper, geoStringMapper);
        }
    }

    public static class TypeParser implements XContentMapper.TypeParser {
        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.pathType(parsePathType(name, fieldNode.toString()));
                } else if (fieldName.equals("store")) {
                    builder.store(parseStore(name, fieldNode.toString()));
                } else if (fieldName.equals("lat_lon")) {
                    builder.enableLatLon(XContentMapValues.nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("geohash")) {
                    builder.enableGeoHash(XContentMapValues.nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("precision_step")) {
                    builder.precisionStep(XContentMapValues.nodeIntegerValue(fieldNode));
                } else if (fieldName.equals("geohash_precision")) {
                    builder.precision(XContentMapValues.nodeIntegerValue(fieldNode));
                }
            }
            return builder;
        }
    }

    private final String name;

    private final ContentPath.Type pathType;

    private final boolean enableLatLon;

    private final boolean enableGeoHash;

    private final Integer precisionStep;

    private final int precision;

    private final NumberFieldMapper latMapper;

    private final NumberFieldMapper lonMapper;

    private final StringFieldMapper geohashMapper;

    private final StringFieldMapper geoStringMapper;

    public GeoPointFieldMapper(String name, ContentPath.Type pathType, boolean enableLatLon, boolean enableGeoHash, Integer precisionStep, int precision,
                               NumberFieldMapper latMapper, NumberFieldMapper lonMapper, StringFieldMapper geohashMapper, StringFieldMapper geoStringMapper) {
        this.name = name;
        this.pathType = pathType;
        this.enableLatLon = enableLatLon;
        this.enableGeoHash = enableGeoHash;
        this.precisionStep = precisionStep;
        this.precision = precision;

        this.latMapper = latMapper;
        this.lonMapper = lonMapper;
        this.geoStringMapper = geoStringMapper;
        this.geohashMapper = geohashMapper;
    }

    @Override public String name() {
        return this.name;
    }

    @Override public void parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);
        context.path().add(name);

        XContentParser.Token token = context.parser().currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            if (token == XContentParser.Token.START_ARRAY) {
                // its an array of array of lon/lat [ [1.2, 1.3], [1.4, 1.5] ]
                while (token != XContentParser.Token.END_ARRAY) {
                    token = context.parser().nextToken();
                    Double lon = context.parser().doubleValue();
                    token = context.parser().nextToken();
                    Double lat = context.parser().doubleValue();
                    while ((token = context.parser().nextToken()) != XContentParser.Token.END_ARRAY) {

                    }
                    parseLatLon(context, lat, lon);
                    token = context.parser().nextToken();
                }
            } else {
                // its an array of other possible values
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    Double lon = context.parser().doubleValue();
                    token = context.parser().nextToken();
                    Double lat = context.parser().doubleValue();
                    while ((token = context.parser().nextToken()) != XContentParser.Token.END_ARRAY) {

                    }
                    parseLatLon(context, lat, lon);
                } else {
                    while (token != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.START_OBJECT) {
                            parseObjectLatLon(context);
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            parseStringLatLon(context);
                        }
                        token = context.parser().nextToken();
                    }
                }
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            parseObjectLatLon(context);
        } else if (token == XContentParser.Token.VALUE_STRING) {
            parseStringLatLon(context);
        }

        context.path().remove();
        context.path().pathType(origPathType);
    }

    private void parseStringLatLon(ParseContext context) throws IOException {
        String value = context.parser().text();
        int comma = value.indexOf(',');
        if (comma != -1) {
            double lat = Double.parseDouble(value.substring(0, comma).trim());
            double lon = Double.parseDouble(value.substring(comma + 1).trim());
            parseLatLon(context, lat, lon);
        } else { // geo hash
            parseGeohash(context, value);
        }
    }

    private void parseObjectLatLon(ParseContext context) throws IOException {
        XContentParser.Token token;
        String currentName = context.parser().currentName();
        Double lat = null;
        Double lon = null;
        String geohash = null;
        while ((token = context.parser().nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = context.parser().currentName();
            } else if (token.isValue()) {
                if (currentName.equals(Names.LAT)) {
                    lat = context.parser().doubleValue();
                } else if (currentName.equals(Names.LON)) {
                    lon = context.parser().doubleValue();
                } else if (currentName.equals(Names.GEOHASH)) {
                    geohash = context.parser().text();
                }
            }
        }
        if (geohash != null) {
            parseGeohash(context, geohash);
        } else if (lat != null && lon != null) {
            parseLatLon(context, lat, lon);
        }
    }

    private void parseLatLon(ParseContext context, Double lat, Double lon) throws IOException {
        context.externalValue(lat.toString() + ',' + lon.toString());
        geoStringMapper.parse(context);
        if (enableGeoHash) {
            context.externalValue(GeoHashUtils.encode(lat, lon, precision));
            geohashMapper.parse(context);
        }
        if (enableLatLon) {
            context.externalValue(lat);
            latMapper.parse(context);
            context.externalValue(lon);
            lonMapper.parse(context);
        }
    }

    private void parseGeohash(ParseContext context, String geohash) throws IOException {
        double[] values = GeoHashUtils.decode(geohash);
        context.externalValue(Double.toString(values[0]) + ',' + Double.toString(values[1]));
        geoStringMapper.parse(context);
        if (enableGeoHash) {
            context.externalValue(geohash);
            geohashMapper.parse(context);
        }
        if (enableLatLon) {
            context.externalValue(values[0]);
            latMapper.parse(context);
            context.externalValue(values[1]);
            lonMapper.parse(context);
        }
    }

    @Override public void close() {
        if (latMapper != null) {
            latMapper.close();
        }
        if (lonMapper != null) {
            lonMapper.close();
        }
        if (geohashMapper != null) {
            geohashMapper.close();
        }
        if (geoStringMapper != null) {
            geoStringMapper.close();
        }
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // TODO
    }

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        geoStringMapper.traverse(fieldMapperListener);
        if (enableGeoHash) {
            geohashMapper.traverse(fieldMapperListener);
        }
        if (enableLatLon) {
            latMapper.traverse(fieldMapperListener);
            lonMapper.traverse(fieldMapperListener);
        }
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", CONTENT_TYPE);
        if (pathType != Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase());
        }
        if (enableLatLon != Defaults.ENABLE_LATLON) {
            builder.field("lat_lon", enableLatLon);
        }
        if (enableGeoHash != Defaults.ENABLE_GEOHASH) {
            builder.field("geohash", enableGeoHash);
        }
        if (geoStringMapper.store() != Defaults.STORE) {
            builder.field("store", geoStringMapper.store().name().toLowerCase());
        }
        if (precision != Defaults.PRECISION) {
            builder.field("geohash_precision", precision);
        }
        if (precisionStep != null) {
            builder.field("precision_step", precisionStep);
        }

        builder.endObject();
        return builder;
    }

    public static class GeoStringFieldMapper extends StringFieldMapper {

        public static class Builder extends AbstractFieldMapper.OpenBuilder<Builder, StringFieldMapper> {

            protected String nullValue = Defaults.NULL_VALUE;

            public Builder(String name) {
                super(name);
                builder = this;
            }

            public Builder nullValue(String nullValue) {
                this.nullValue = nullValue;
                return this;
            }

            @Override public Builder includeInAll(Boolean includeInAll) {
                this.includeInAll = includeInAll;
                return this;
            }

            @Override public GeoStringFieldMapper build(BuilderContext context) {
                GeoStringFieldMapper fieldMapper = new GeoStringFieldMapper(buildNames(context),
                        index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, nullValue,
                        indexAnalyzer, searchAnalyzer);
                fieldMapper.includeInAll(includeInAll);
                return fieldMapper;
            }
        }

        public GeoStringFieldMapper(Names names, Field.Index index, Field.Store store, Field.TermVector termVector, float boost, boolean omitNorms, boolean omitTermFreqAndPositions, String nullValue, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
            super(names, index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, nullValue, indexAnalyzer, searchAnalyzer);
        }

        @Override public FieldDataType fieldDataType() {
            return GeoPointFieldDataType.TYPE;
        }
    }
}
