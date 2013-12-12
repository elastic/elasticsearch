/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.mapper.geo;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseStore;

/**
 * Parsing: We handle:
 * <p/>
 * - "field" : "geo_hash"
 * - "field" : "lat,lon"
 * - "field" : {
 * "lat" : 1.1,
 * "lon" : 2.1
 * }
 */
public class GeoPointFieldMapper implements Mapper, ArrayValueMapperParser {

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
        public static final boolean STORE = false;
        public static final boolean ENABLE_LATLON = false;
        public static final boolean ENABLE_GEOHASH = false;
        public static final boolean ENABLE_GEOHASH_PREFIX = false;
        public static final int PRECISION = GeoHashUtils.PRECISION;
        public static final boolean NORMALIZE_LAT = true;
        public static final boolean NORMALIZE_LON = true;
        public static final boolean VALIDATE_LAT = true;
        public static final boolean VALIDATE_LON = true;

        public static final FieldType FIELD_TYPE = new FieldType(StringFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends Mapper.Builder<Builder, GeoPointFieldMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private boolean enableGeoHash = Defaults.ENABLE_GEOHASH;

        private boolean enableGeohashPrefix = Defaults.ENABLE_GEOHASH_PREFIX;

        private boolean enableLatLon = Defaults.ENABLE_LATLON;

        private Integer precisionStep;

        private int precision = Defaults.PRECISION;

        private boolean store = Defaults.STORE;

        boolean validateLat = Defaults.VALIDATE_LAT;
        boolean validateLon = Defaults.VALIDATE_LON;
        boolean normalizeLat = Defaults.NORMALIZE_LAT;
        boolean normalizeLon = Defaults.NORMALIZE_LON;

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

        public Builder geohashPrefix(boolean enableGeohashPrefix) {
            this.enableGeohashPrefix = enableGeohashPrefix;
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

        public Builder store(boolean store) {
            this.store = store;
            return this;
        }

        @Override
        public GeoPointFieldMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            GeoStringFieldMapper geoStringMapper = new GeoStringFieldMapper.Builder(name)
                    .includeInAll(false).store(store).build(context);

            DoubleFieldMapper latMapper = null;
            DoubleFieldMapper lonMapper = null;

            context.path().add(name);
            if (enableLatLon) {
                NumberFieldMapper.Builder<?, ?> latMapperBuilder = doubleField(Names.LAT).includeInAll(false);
                NumberFieldMapper.Builder<?, ?> lonMapperBuilder = doubleField(Names.LON).includeInAll(false);
                if (precisionStep != null) {
                    latMapperBuilder.precisionStep(precisionStep);
                    lonMapperBuilder.precisionStep(precisionStep);
                }
                latMapper = (DoubleFieldMapper) latMapperBuilder.includeInAll(false).store(store).build(context);
                lonMapper = (DoubleFieldMapper) lonMapperBuilder.includeInAll(false).store(store).build(context);
            }
            StringFieldMapper geohashMapper = null;
            if (enableGeoHash) {
                geohashMapper = stringField(Names.GEOHASH).index(true).tokenized(false).includeInAll(false).omitNorms(true).indexOptions(IndexOptions.DOCS_ONLY).build(context);
            }
            context.path().remove();

            context.path().pathType(origPathType);

            return new GeoPointFieldMapper(name, pathType, enableLatLon, enableGeoHash, enableGeohashPrefix, precisionStep, precision,
                    latMapper, lonMapper, geohashMapper, geoStringMapper,
                    validateLon, validateLat, normalizeLon, normalizeLat);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = geoPointField(name);

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
                } else if (fieldName.equals("geohash_prefix")) {
                    builder.geohashPrefix(XContentMapValues.nodeBooleanValue(fieldNode));
                    if (XContentMapValues.nodeBooleanValue(fieldNode)) {
                        // automatically set geohash to true as well...
                        // TODO: Should we do this in the builder
                        builder.enableGeoHash(true);
                    }
                } else if (fieldName.equals("precision_step")) {
                    builder.precisionStep(XContentMapValues.nodeIntegerValue(fieldNode));
                } else if (fieldName.equals("geohash_precision")) {
                    builder.precision(XContentMapValues.nodeIntegerValue(fieldNode));
                } else if (fieldName.equals("validate")) {
                    builder.validateLat = XContentMapValues.nodeBooleanValue(fieldNode);
                    builder.validateLon = XContentMapValues.nodeBooleanValue(fieldNode);
                } else if (fieldName.equals("validate_lon")) {
                    builder.validateLon = XContentMapValues.nodeBooleanValue(fieldNode);
                } else if (fieldName.equals("validate_lat")) {
                    builder.validateLat = XContentMapValues.nodeBooleanValue(fieldNode);
                } else if (fieldName.equals("normalize")) {
                    builder.normalizeLat = XContentMapValues.nodeBooleanValue(fieldNode);
                    builder.normalizeLon = XContentMapValues.nodeBooleanValue(fieldNode);
                } else if (fieldName.equals("normalize_lat")) {
                    builder.normalizeLat = XContentMapValues.nodeBooleanValue(fieldNode);
                } else if (fieldName.equals("normalize_lon")) {
                    builder.normalizeLon = XContentMapValues.nodeBooleanValue(fieldNode);
                }
            }
            return builder;
        }
    }

    private final String name;

    private final ContentPath.Type pathType;

    private final boolean enableLatLon;

    private final boolean enableGeoHash;

    private final boolean enableGeohashPrefix;

    private final Integer precisionStep;

    private final int precision;

    private final DoubleFieldMapper latMapper;

    private final DoubleFieldMapper lonMapper;

    private final StringFieldMapper geohashMapper;

    private final GeoStringFieldMapper geoStringMapper;

    private final boolean validateLon;
    private final boolean validateLat;

    private final boolean normalizeLon;
    private final boolean normalizeLat;

    public GeoPointFieldMapper(String name, ContentPath.Type pathType, boolean enableLatLon, boolean enableGeoHash, boolean enableGeohashPrefix, Integer precisionStep, int precision,
                               DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper, StringFieldMapper geohashMapper, GeoStringFieldMapper geoStringMapper,
                               boolean validateLon, boolean validateLat,
                               boolean normalizeLon, boolean normalizeLat) {
        this.name = name;
        this.pathType = pathType;
        this.enableLatLon = enableLatLon;
        this.enableGeoHash = enableGeoHash || enableGeohashPrefix; // implicitly enable geohashes if geohash_prefix is set
        this.enableGeohashPrefix = enableGeohashPrefix;
        this.precisionStep = precisionStep;
        this.precision = precision;

        this.latMapper = latMapper;
        this.lonMapper = lonMapper;
        this.geoStringMapper = geoStringMapper;
        this.geohashMapper = geohashMapper;

        this.geoStringMapper.geoMapper = this;

        this.validateLat = validateLat;
        this.validateLon = validateLon;

        this.normalizeLat = normalizeLat;
        this.normalizeLon = normalizeLon;
    }

    @Override
    public String name() {
        return this.name;
    }

    public DoubleFieldMapper latMapper() {
        return latMapper;
    }

    public DoubleFieldMapper lonMapper() {
        return lonMapper;
    }

    public GeoStringFieldMapper stringMapper() {
        return this.geoStringMapper;
    }

    public StringFieldMapper geoHashStringMapper() {
        return this.geohashMapper;
    }

    public boolean isEnableLatLon() {
        return enableLatLon;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
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
                    double lon = context.parser().doubleValue();
                    token = context.parser().nextToken();
                    double lat = context.parser().doubleValue();
                    while ((token = context.parser().nextToken()) != XContentParser.Token.END_ARRAY) {

                    }
                    parseLatLon(context, lat, lon);
                    token = context.parser().nextToken();
                }
            } else {
                // its an array of other possible values
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    double lon = context.parser().doubleValue();
                    token = context.parser().nextToken();
                    double lat = context.parser().doubleValue();
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

    private void parseGeohashField(ParseContext context, String geohash) throws IOException {
        int len = Math.min(precision, geohash.length());
        int min = enableGeohashPrefix ? 1 : geohash.length();

        for (int i = len; i >= min; i--) {
            context.externalValue(geohash.substring(0, i));
            // side effect of this call is adding the field
            geohashMapper.parse(context);
        }
    }

    private void parseLatLon(ParseContext context, double lat, double lon) throws IOException {
        if (normalizeLat || normalizeLon) {
            GeoPoint point = new GeoPoint(lat, lon);
            GeoUtils.normalizePoint(point, normalizeLat, normalizeLon);
            lat = point.lat();
            lon = point.lon();
        }

        if (validateLat) {
            if (lat > 90.0 || lat < -90.0) {
                throw new ElasticSearchIllegalArgumentException("illegal latitude value [" + lat + "] for " + name);
            }
        }
        if (validateLon) {
            if (lon > 180.0 || lon < -180) {
                throw new ElasticSearchIllegalArgumentException("illegal longitude value [" + lon + "] for " + name);
            }
        }

        context.externalValue(Double.toString(lat) + ',' + Double.toString(lon));
        geoStringMapper.parse(context);
        if (enableGeoHash) {
            parseGeohashField(context, GeoHashUtils.encode(lat, lon, precision));
        }
        if (enableLatLon) {
            context.externalValue(lat);
            latMapper.parse(context);
            context.externalValue(lon);
            lonMapper.parse(context);
        }
    }

    private void parseGeohash(ParseContext context, String geohash) throws IOException {
        GeoPoint point = GeoHashUtils.decode(geohash);

        if (normalizeLat || normalizeLon) {
            GeoUtils.normalizePoint(point, normalizeLat, normalizeLon);
        }

        if (validateLat) {
            if (point.lat() > 90.0 || point.lat() < -90.0) {
                throw new ElasticSearchIllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name);
            }
        }
        if (validateLon) {
            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new ElasticSearchIllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name);
            }
        }

        context.externalValue(Double.toString(point.lat()) + ',' + Double.toString(point.lon()));
        geoStringMapper.parse(context);
        if (enableGeoHash) {
            parseGeohashField(context, geohash);
        }
        if (enableLatLon) {
            context.externalValue(point.lat());
            latMapper.parse(context);
            context.externalValue(point.lon());
            lonMapper.parse(context);
        }
    }

    @Override
    public void close() {
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

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // TODO
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        geoStringMapper.traverse(fieldMapperListener);
        if (enableGeoHash) {
            geohashMapper.traverse(fieldMapperListener);
        }
        if (enableLatLon) {
            latMapper.traverse(fieldMapperListener);
            lonMapper.traverse(fieldMapperListener);
        }
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", CONTENT_TYPE);
        if (pathType != Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase(Locale.ROOT));
        }
        if (enableLatLon != Defaults.ENABLE_LATLON) {
            builder.field("lat_lon", enableLatLon);
        }
        if (enableGeoHash != Defaults.ENABLE_GEOHASH) {
            builder.field("geohash", enableGeoHash);
        }
        if (enableGeohashPrefix != Defaults.ENABLE_GEOHASH_PREFIX) {
            builder.field("geohash_prefix", enableGeohashPrefix);
        }
        if (geoStringMapper.fieldType().stored() != Defaults.STORE) {
            builder.field("store", geoStringMapper.fieldType().stored());
        }
        if (precision != Defaults.PRECISION) {
            builder.field("geohash_precision", precision);
        }
        if (precisionStep != null) {
            builder.field("precision_step", precisionStep);
        }
        if (!validateLat && !validateLon) {
            builder.field("validate", false);
        } else {
            if (validateLat != Defaults.VALIDATE_LAT) {
                builder.field("validate_lat", validateLat);
            }
            if (validateLon != Defaults.VALIDATE_LON) {
                builder.field("validate_lon", validateLon);
            }
        }
        if (!normalizeLat && !normalizeLon) {
            builder.field("normalize", false);
        } else {
            if (normalizeLat != Defaults.NORMALIZE_LAT) {
                builder.field("normalize_lat", normalizeLat);
            }
            if (normalizeLon != Defaults.NORMALIZE_LON) {
                builder.field("normalize_lon", normalizeLon);
            }
        }

        builder.endObject();
        return builder;
    }

    public static class GeoStringFieldMapper extends StringFieldMapper {

        public static class Builder extends AbstractFieldMapper.OpenBuilder<Builder, StringFieldMapper> {

            protected String nullValue = Defaults.NULL_VALUE;

            public Builder(String name) {
                super(name, new FieldType(GeoPointFieldMapper.Defaults.FIELD_TYPE));
                builder = this;
            }

            public Builder nullValue(String nullValue) {
                this.nullValue = nullValue;
                return this;
            }

            @Override
            public Builder includeInAll(Boolean includeInAll) {
                this.includeInAll = includeInAll;
                return this;
            }

            @Override
            public GeoStringFieldMapper build(BuilderContext context) {
                GeoStringFieldMapper fieldMapper = new GeoStringFieldMapper(buildNames(context),
                        boost, fieldType, nullValue, indexAnalyzer, searchAnalyzer, provider, fieldDataSettings);
                fieldMapper.includeInAll(includeInAll);
                return fieldMapper;
            }
        }

        GeoPointFieldMapper geoMapper;

        public GeoStringFieldMapper(Names names, float boost, FieldType fieldType, String nullValue,
                                    NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
                                    PostingsFormatProvider provider, @Nullable Settings fieldDataSettings) {
            super(names, boost, fieldType, nullValue, indexAnalyzer, searchAnalyzer, searchAnalyzer, Defaults.POSITION_OFFSET_GAP, Defaults.IGNORE_ABOVE,
                    provider, null, fieldDataSettings);
        }

        @Override
        public FieldType defaultFieldType() {
            return GeoPointFieldMapper.Defaults.FIELD_TYPE;
        }

        @Override
        public FieldDataType defaultFieldDataType() {
            return new FieldDataType("geo_point");
        }

        public GeoPointFieldMapper geoMapper() {
            return geoMapper;
        }
    }
}
