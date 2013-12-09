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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;

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
public class GeoPointFieldMapper extends AbstractFieldMapper<GeoPoint> implements ArrayValueMapperParser {

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
        public static final int GEO_HASH_PRECISION = GeoHashUtils.PRECISION;
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

    public static class Builder extends AbstractFieldMapper.Builder<Builder, GeoPointFieldMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private boolean enableGeoHash = Defaults.ENABLE_GEOHASH;

        private boolean enableGeohashPrefix = Defaults.ENABLE_GEOHASH_PREFIX;

        private boolean enableLatLon = Defaults.ENABLE_LATLON;

        private Integer precisionStep;

        private int geoHashPrecision = Defaults.GEO_HASH_PRECISION;

        boolean validateLat = Defaults.VALIDATE_LAT;
        boolean validateLon = Defaults.VALIDATE_LON;
        boolean normalizeLat = Defaults.NORMALIZE_LAT;
        boolean normalizeLon = Defaults.NORMALIZE_LON;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
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

        public Builder geoHashPrecision(int precision) {
            this.geoHashPrecision = precision;
            return this;
        }

        public Builder fieldDataSettings(Settings settings) {
            this.fieldDataSettings = settings;
            return builder;
        }

        @Override
        public GeoPointFieldMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

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
                latMapper = (DoubleFieldMapper) latMapperBuilder.includeInAll(false).store(fieldType.stored()).build(context);
                lonMapper = (DoubleFieldMapper) lonMapperBuilder.includeInAll(false).store(fieldType.stored()).build(context);
            }
            StringFieldMapper geohashMapper = null;
            if (enableGeoHash) {
                geohashMapper = stringField(Names.GEOHASH).index(true).tokenized(false).includeInAll(false).omitNorms(true).indexOptions(IndexOptions.DOCS_ONLY).build(context);
            }
            context.path().remove();

            context.path().pathType(origPathType);

            // this is important: even if geo points feel like they need to be tokenized to distinguish lat from lon, we actually want to
            // store them as a single token.
            fieldType.setTokenized(false);

            return new GeoPointFieldMapper(buildNames(context), fieldType, indexAnalyzer, searchAnalyzer, postingsProvider, docValuesProvider, similarity, fieldDataSettings, context.indexSettings(), origPathType, enableLatLon, enableGeoHash, enableGeohashPrefix, precisionStep, geoHashPrecision, latMapper, lonMapper, geohashMapper, validateLon, validateLat, normalizeLon, normalizeLat);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = geoPointField(name);
            parseField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.pathType(parsePathType(name, fieldNode.toString()));
                } else if (fieldName.equals("lat_lon")) {
                    builder.enableLatLon(XContentMapValues.nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("geohash")) {
                    builder.enableGeoHash(XContentMapValues.nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("geohash_prefix")) {
                    builder.geohashPrefix(XContentMapValues.nodeBooleanValue(fieldNode));
                    if (XContentMapValues.nodeBooleanValue(fieldNode)) {
                        builder.enableGeoHash(true);
                    }
                } else if (fieldName.equals("precision_step")) {
                    builder.precisionStep(XContentMapValues.nodeIntegerValue(fieldNode));
                } else if (fieldName.equals("geohash_precision")) {
                    builder.geoHashPrecision(XContentMapValues.nodeIntegerValue(fieldNode));
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

    /**
     * A byte-aligned fixed-length encoding for latitudes and longitudes.
     */
    public static final class Encoding {

        // With 14 bytes we already have better precision than a double since a double has 11 bits of exponent
        private static final int MAX_NUM_BYTES = 14;

        private static final Encoding[] INSTANCES;
        static {
            INSTANCES = new Encoding[MAX_NUM_BYTES + 1];
            for (int numBytes = 2; numBytes <= MAX_NUM_BYTES; numBytes += 2) {
                INSTANCES[numBytes] = new Encoding(numBytes);
            }
        }

        /** Get an instance based on the number of bytes that has been used to encode values. */
        public static final Encoding of(int numBytesPerValue) {
            final Encoding instance = INSTANCES[numBytesPerValue];
            if (instance == null) {
                throw new ElasticSearchIllegalStateException("No encoding for " + numBytesPerValue + " bytes per value");
            }
            return instance;
        }

        /** Get an instance based on the expected precision. Here are examples of the number of required bytes per value depending on the
         *  expected precision:<ul>
         *  <li>1km: 4 bytes</li>
         *  <li>3m: 6 bytes</li>
         *  <li>1m: 8 bytes</li>
         *  <li>1cm: 8 bytes</li>
         *  <li>1mm: 10 bytes</li></ul> */
        public static final Encoding of(DistanceUnit.Distance precision) {
            for (Encoding encoding : INSTANCES) {
                if (encoding != null && encoding.precision().compareTo(precision) <= 0) {
                    return encoding;
                }
            }
            return INSTANCES[MAX_NUM_BYTES];
        }

        private final DistanceUnit.Distance precision;
        private final int numBytes;
        private final int numBytesPerCoordinate;
        private final double factor;

        private Encoding(int numBytes) {
            assert numBytes >= 1 && numBytes <= MAX_NUM_BYTES;
            assert (numBytes & 1) == 0; // we don't support odd numBytes for the moment
            this.numBytes = numBytes;
            this.numBytesPerCoordinate = numBytes / 2;
            this.factor = Math.pow(2, - numBytesPerCoordinate * 8 + 9);
            assert (1L << (numBytesPerCoordinate * 8 - 1)) * factor > 180 && (1L << (numBytesPerCoordinate * 8 - 2)) * factor < 180 : numBytesPerCoordinate + " " + factor;
            if (numBytes == MAX_NUM_BYTES) {
                // no precision loss compared to a double
                precision = new DistanceUnit.Distance(0, DistanceUnit.METERS);
            } else {
                precision = new DistanceUnit.Distance(
                        GeoDistance.PLANE.calculate(0, 0, factor / 2, factor / 2, DistanceUnit.METERS), // factor/2 because we use Math.round instead of a cast to convert the double to a long
                        DistanceUnit.METERS);
            }
        }

        public DistanceUnit.Distance precision() {
            return precision;
        }

        /** The number of bytes required to encode a single geo point. */
        public final int numBytes() {
            return numBytes;
        }

        /** The number of bits required to encode a single coordinate of a geo point. */
        public int numBitsPerCoordinate() {
            return numBytesPerCoordinate << 3;
        }

        /** Return the bits that encode a latitude/longitude. */
        public long encodeCoordinate(double lat) {
            return Math.round((lat + 180) / factor);
        }

        /** Decode a sequence of bits into the original coordinate. */
        public double decodeCoordinate(long bits) {
            return bits * factor - 180;
        }

        private void encodeBits(long bits, byte[] out, int offset) {
            for (int i = 0; i < numBytesPerCoordinate; ++i) {
                out[offset++] = (byte) bits;
                bits >>>= 8;
            }
            assert bits == 0;
        }

        private long decodeBits(byte [] in, int offset) {
            long r = in[offset++] & 0xFFL;
            for (int i = 1; i < numBytesPerCoordinate; ++i) {
                r = (in[offset++] & 0xFFL) << (i * 8);
            }
            return r;
        }

        /** Encode a geo point into a byte-array, over {@link #numBytes()} bytes. */
        public void encode(double lat, double lon, byte[] out, int offset) {
            encodeBits(encodeCoordinate(lat), out, offset);
            encodeBits(encodeCoordinate(lon), out, offset + numBytesPerCoordinate);
        }

        /** Decode a geo point from a byte-array, reading {@link #numBytes()} bytes. */
        public GeoPoint decode(byte[] in, int offset, GeoPoint out) {
            final long latBits = decodeBits(in, offset);
            final long lonBits = decodeBits(in, offset + numBytesPerCoordinate);
            return decode(latBits, lonBits, out);
        }

        /** Decode a geo point from the bits of the encoded latitude and longitudes. */
        public GeoPoint decode(long latBits, long lonBits, GeoPoint out) {
            final double lat = decodeCoordinate(latBits);
            final double lon = decodeCoordinate(lonBits);
            return out.reset(lat, lon);
        }

    }

    private final ContentPath.Type pathType;

    private final boolean enableLatLon;

    private final boolean enableGeoHash;

    private final boolean enableGeohashPrefix;

    private final Integer precisionStep;

    private final int geoHashPrecision;

    private final DoubleFieldMapper latMapper;

    private final DoubleFieldMapper lonMapper;

    private final StringFieldMapper geohashMapper;

    private final boolean validateLon;
    private final boolean validateLat;

    private final boolean normalizeLon;
    private final boolean normalizeLat;

    public GeoPointFieldMapper(FieldMapper.Names names, FieldType fieldType,
            NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
            PostingsFormatProvider postingsFormat, DocValuesFormatProvider docValuesFormat,
            SimilarityProvider similarity, @Nullable Settings fieldDataSettings, Settings indexSettings,
            ContentPath.Type pathType, boolean enableLatLon, boolean enableGeoHash, boolean enableGeohashPrefix, Integer precisionStep, int geoHashPrecision,
            DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper, StringFieldMapper geohashMapper,
            boolean validateLon, boolean validateLat,
            boolean normalizeLon, boolean normalizeLat) {
        super(names, 1f, fieldType, null, indexAnalyzer, postingsFormat, docValuesFormat, similarity, fieldDataSettings, indexSettings);
        this.pathType = pathType;
        this.enableLatLon = enableLatLon;
        this.enableGeoHash = enableGeoHash || enableGeohashPrefix; // implicitly enable geohashes if geohash_prefix is set
        this.enableGeohashPrefix = enableGeohashPrefix;
        this.precisionStep = precisionStep;
        this.geoHashPrecision = geoHashPrecision;

        this.latMapper = latMapper;
        this.lonMapper = lonMapper;
        this.geohashMapper = geohashMapper;

        this.validateLat = validateLat;
        this.validateLon = validateLon;

        this.normalizeLat = normalizeLat;
        this.normalizeLon = normalizeLon;

        if (hasDocValues()) {
            throw new ElasticSearchIllegalStateException("Geo points don't support doc values"); // yet
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("geo_point");
    }

    public DoubleFieldMapper latMapper() {
        return latMapper;
    }

    public DoubleFieldMapper lonMapper() {
        return lonMapper;
    }

    public StringFieldMapper geoHashStringMapper() {
        return this.geohashMapper;
    }

    public boolean isEnableLatLon() {
        return enableLatLon;
    }

    public boolean isEnableGeohashPrefix() {
        return enableGeohashPrefix;
    }

    @Override
    public GeoPoint value(Object value) {
        if (value instanceof GeoPoint) {
            return (GeoPoint) value;
        } else {
            return GeoPoint.parseFromLatLon(value.toString());
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);
        context.path().add(name());

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
        int len = Math.min(geoHashPrecision, geohash.length());
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
                throw new ElasticSearchIllegalArgumentException("illegal latitude value [" + lat + "] for " + name());
            }
        }
        if (validateLon) {
            if (lon > 180.0 || lon < -180) {
                throw new ElasticSearchIllegalArgumentException("illegal longitude value [" + lon + "] for " + name());
            }
        }

        if (fieldType.indexed() || fieldType.stored()) {
            Field field = new Field(names.indexName(), Double.toString(lat) + ',' + Double.toString(lon), fieldType);
            context.doc().add(field);
        }
        if (enableGeoHash) {
            parseGeohashField(context, GeoHashUtils.encode(lat, lon, geoHashPrecision));
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
                throw new ElasticSearchIllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
            }
        }
        if (validateLon) {
            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new ElasticSearchIllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
            }
        }

        if (fieldType.indexed() || fieldType.stored()) {
            Field field = new Field(names.indexName(), Double.toString(point.lat()) + ',' + Double.toString(point.lon()), fieldType);
            context.doc().add(field);
        }
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
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        // TODO: geo-specific properties
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        super.traverse(fieldMapperListener);
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
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
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
        if (geoHashPrecision != Defaults.GEO_HASH_PRECISION) {
            builder.field("geohash_precision", geoHashPrecision);
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
    }

}
