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

package org.elasticsearch.index.mapper.geo;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Iterators;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.CustomNumericDocValuesField;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.doubleField;
import static org.elasticsearch.index.mapper.MapperBuilders.geoPointField;
import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;
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
public class GeoPointFieldMapper extends FieldMapper implements ArrayValueMapperParser {

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
        public static final boolean ENABLE_LATLON = false;
        public static final boolean ENABLE_GEOHASH = false;
        public static final boolean ENABLE_GEOHASH_PREFIX = false;
        public static final int GEO_HASH_PRECISION = GeoHashUtils.PRECISION;
        public static final boolean NORMALIZE_LAT = true;
        public static final boolean NORMALIZE_LON = true;
        public static final boolean VALIDATE_LAT = true;
        public static final boolean VALIDATE_LON = true;

        public static final MappedFieldType FIELD_TYPE = new GeoPointFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, GeoPointFieldMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private boolean enableGeoHash = Defaults.ENABLE_GEOHASH;

        private boolean enableGeohashPrefix = Defaults.ENABLE_GEOHASH_PREFIX;

        private boolean enableLatLon = Defaults.ENABLE_LATLON;

        private Integer precisionStep;

        private int geoHashPrecision = Defaults.GEO_HASH_PRECISION;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        @Override
        public GeoPointFieldType fieldType() {
            return (GeoPointFieldType)fieldType;
        }

        @Override
        public Builder multiFieldPathType(ContentPath.Type pathType) {
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

        @Override
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
            GeoPointFieldType geoPointFieldType = (GeoPointFieldType)fieldType;

            context.path().add(name);
            if (enableLatLon) {
                NumberFieldMapper.Builder<?, ?> latMapperBuilder = doubleField(Names.LAT).includeInAll(false);
                NumberFieldMapper.Builder<?, ?> lonMapperBuilder = doubleField(Names.LON).includeInAll(false);
                if (precisionStep != null) {
                    latMapperBuilder.precisionStep(precisionStep);
                    lonMapperBuilder.precisionStep(precisionStep);
                }
                latMapper = (DoubleFieldMapper) latMapperBuilder.includeInAll(false).store(fieldType.stored()).docValues(false).build(context);
                lonMapper = (DoubleFieldMapper) lonMapperBuilder.includeInAll(false).store(fieldType.stored()).docValues(false).build(context);
                geoPointFieldType.setLatLonEnabled(latMapper.fieldType(), lonMapper.fieldType());
            }
            StringFieldMapper geohashMapper = null;
            if (enableGeoHash || enableGeohashPrefix) {
                // TODO: possible also implicitly enable geohash if geohash precision is set
                geohashMapper = stringField(Names.GEOHASH).index(true).tokenized(false).includeInAll(false).omitNorms(true).indexOptions(IndexOptions.DOCS).build(context);
                geoPointFieldType.setGeohashEnabled(geohashMapper.fieldType(), geoHashPrecision, enableGeohashPrefix);
            }
            context.path().remove();

            context.path().pathType(origPathType);

            // this is important: even if geo points feel like they need to be tokenized to distinguish lat from lon, we actually want to
            // store them as a single token.
            fieldType.setTokenized(false);
            setupFieldType(context);
            fieldType.setHasDocValues(false);
            defaultFieldType.setHasDocValues(false);
            return new GeoPointFieldMapper(name, fieldType, defaultFieldType, context.indexSettings(), origPathType,
                     latMapper, lonMapper, geohashMapper, multiFieldsBuilder.build(this, context));
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = geoPointField(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path") && parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                    builder.multiFieldPathType(parsePathType(name, fieldNode.toString()));
                    iterator.remove();
                } else if (fieldName.equals("lat_lon")) {
                    builder.enableLatLon(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("geohash")) {
                    builder.enableGeoHash(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("geohash_prefix")) {
                    builder.geohashPrefix(XContentMapValues.nodeBooleanValue(fieldNode));
                    if (XContentMapValues.nodeBooleanValue(fieldNode)) {
                        builder.enableGeoHash(true);
                    }
                    iterator.remove();
                } else if (fieldName.equals("precision_step")) {
                    builder.precisionStep(XContentMapValues.nodeIntegerValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("geohash_precision")) {
                    if (fieldNode instanceof Integer) {
                        builder.geoHashPrecision(XContentMapValues.nodeIntegerValue(fieldNode));
                    } else {
                        builder.geoHashPrecision(GeoUtils.geoHashLevelsForPrecision(fieldNode.toString()));
                    }
                    iterator.remove();
                } else if (fieldName.equals("validate")) {
                    builder.fieldType().setValidateLat(XContentMapValues.nodeBooleanValue(fieldNode));
                    builder.fieldType().setValidateLon(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("validate_lon")) {
                    builder.fieldType().setValidateLon(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("validate_lat")) {
                    builder.fieldType().setValidateLat(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("normalize")) {
                    builder.fieldType().setNormalizeLat(XContentMapValues.nodeBooleanValue(fieldNode));
                    builder.fieldType().setNormalizeLon(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("normalize_lat")) {
                    builder.fieldType().setNormalizeLat(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("normalize_lon")) {
                    builder.fieldType().setNormalizeLon(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (parseMultiField(builder, name, parserContext, fieldName, fieldNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class GeoPointFieldType extends MappedFieldType {

        private MappedFieldType geohashFieldType;
        private int geohashPrecision;
        private boolean geohashPrefixEnabled;

        private MappedFieldType latFieldType;
        private MappedFieldType lonFieldType;
        private boolean validateLon = true;
        private boolean validateLat = true;
        private boolean normalizeLon = true;
        private boolean normalizeLat = true;

        public GeoPointFieldType() {}

        protected GeoPointFieldType(GeoPointFieldType ref) {
            super(ref);
            this.geohashFieldType = ref.geohashFieldType; // copying ref is ok, this can never be modified
            this.geohashPrecision = ref.geohashPrecision;
            this.geohashPrefixEnabled = ref.geohashPrefixEnabled;
            this.latFieldType = ref.latFieldType; // copying ref is ok, this can never be modified
            this.lonFieldType = ref.lonFieldType; // copying ref is ok, this can never be modified
            this.validateLon = ref.validateLon;
            this.validateLat = ref.validateLat;
            this.normalizeLon = ref.normalizeLon;
            this.normalizeLat = ref.normalizeLat;
        }

        @Override
        public MappedFieldType clone() {
            return new GeoPointFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            GeoPointFieldType that = (GeoPointFieldType) o;
            return geohashPrecision == that.geohashPrecision &&
                geohashPrefixEnabled == that.geohashPrefixEnabled &&
                validateLon == that.validateLon &&
                validateLat == that.validateLat &&
                normalizeLon == that.normalizeLon &&
                normalizeLat == that.normalizeLat &&
                java.util.Objects.equals(geohashFieldType, that.geohashFieldType) &&
                java.util.Objects.equals(latFieldType, that.latFieldType) &&
                java.util.Objects.equals(lonFieldType, that.lonFieldType);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(super.hashCode(), geohashFieldType, geohashPrecision, geohashPrefixEnabled, latFieldType, lonFieldType, validateLon, validateLat, normalizeLon, normalizeLat);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
        
        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            GeoPointFieldType other = (GeoPointFieldType)fieldType;
            if (isLatLonEnabled() != other.isLatLonEnabled()) {
                conflicts.add("mapper [" + names().fullName() + "] has different lat_lon");
            }
            if (isGeohashEnabled() != other.isGeohashEnabled()) {
                conflicts.add("mapper [" + names().fullName() + "] has different geohash");
            }
            if (geohashPrecision() != other.geohashPrecision()) {
                conflicts.add("mapper [" + names().fullName() + "] has different geohash_precision");
            }
            if (isGeohashPrefixEnabled() != other.isGeohashPrefixEnabled()) {
                conflicts.add("mapper [" + names().fullName() + "] has different geohash_prefix");
            }
            if (normalizeLat() != other.normalizeLat()) {
                conflicts.add("mapper [" + names().fullName() + "] has different normalize_lat");
            }
            if (normalizeLon() != other.normalizeLon()) {
                conflicts.add("mapper [" + names().fullName() + "] has different normalize_lon");
            }
            if (isLatLonEnabled() &&
                latFieldType().numericPrecisionStep() != other.latFieldType().numericPrecisionStep()) {
                conflicts.add("mapper [" + names().fullName() + "] has different precision_step");
            }
            if (validateLat() != other.validateLat()) {
                conflicts.add("mapper [" + names().fullName() + "] has different validate_lat");
            }
            if (validateLon() != other.validateLon()) {
                conflicts.add("mapper [" + names().fullName() + "] has different validate_lon");
            }
        }

        public boolean isGeohashEnabled() {
            return geohashFieldType != null;
        }

        public MappedFieldType geohashFieldType() {
            return geohashFieldType;
        }

        public int geohashPrecision() {
            return geohashPrecision;
        }

        public boolean isGeohashPrefixEnabled() {
            return geohashPrefixEnabled;
        }

        public void setGeohashEnabled(MappedFieldType geohashFieldType, int geohashPrecision, boolean geohashPrefixEnabled) {
            checkIfFrozen();
            this.geohashFieldType = geohashFieldType;
            this.geohashPrecision = geohashPrecision;
            this.geohashPrefixEnabled = geohashPrefixEnabled;
        }

        public boolean isLatLonEnabled() {
            return latFieldType != null;
        }

        public MappedFieldType latFieldType() {
            return latFieldType;
        }

        public MappedFieldType lonFieldType() {
            return lonFieldType;
        }

        public void setLatLonEnabled(MappedFieldType latFieldType, MappedFieldType lonFieldType) {
            checkIfFrozen();
            this.latFieldType = latFieldType;
            this.lonFieldType = lonFieldType;
        }

        public boolean validateLon() {
            return validateLon;
        }

        public void setValidateLon(boolean validateLon) {
            checkIfFrozen();
            this.validateLon = validateLon;
        }

        public boolean validateLat() {
            return validateLat;
        }

        public void setValidateLat(boolean validateLat) {
            checkIfFrozen();
            this.validateLat = validateLat;
        }

        public boolean normalizeLon() {
            return normalizeLon;
        }

        public void setNormalizeLon(boolean normalizeLon) {
            checkIfFrozen();
            this.normalizeLon = normalizeLon;
        }

        public boolean normalizeLat() {
            return normalizeLat;
        }

        public void setNormalizeLat(boolean normalizeLat) {
            checkIfFrozen();
            this.normalizeLat = normalizeLat;
        }

        @Override
        public GeoPoint value(Object value) {
            if (value instanceof GeoPoint) {
                return (GeoPoint) value;
            } else {
                return GeoPoint.parseFromLatLon(value.toString());
            }
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
                throw new IllegalStateException("No encoding for " + numBytesPerValue + " bytes per value");
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
                precision = new DistanceUnit.Distance(0, DistanceUnit.DEFAULT);
            } else {
                precision = new DistanceUnit.Distance(
                        GeoDistance.PLANE.calculate(0, 0, factor / 2, factor / 2, DistanceUnit.DEFAULT), // factor/2 because we use Math.round instead of a cast to convert the double to a long
                        DistanceUnit.DEFAULT);
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

    private final DoubleFieldMapper latMapper;

    private final DoubleFieldMapper lonMapper;

    private final StringFieldMapper geohashMapper;

    public GeoPointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings,
            ContentPath.Type pathType, DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper, StringFieldMapper geohashMapper,MultiFields multiFields) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, null);
        this.pathType = pathType;
        this.latMapper = latMapper;
        this.lonMapper = lonMapper;
        this.geohashMapper = geohashMapper;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public GeoPointFieldType fieldType() {
        return (GeoPointFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);
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
                        parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
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
                        parse(context, sparse.reset(lat, lon), null);
                    } else {
                        while (token != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                parsePointFromString(context, sparse, context.parser().text());
                            } else {
                                parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                            }
                            token = context.parser().nextToken();
                        }
                    }
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                parsePointFromString(context, sparse, context.parser().text());
            } else if (token != XContentParser.Token.VALUE_NULL) {
                parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
            }
        }

        context.path().remove();
        context.path().pathType(origPathType);
        return null;
    }

    private void addGeohashField(ParseContext context, String geohash) throws IOException {
        int len = Math.min(fieldType().geohashPrecision(), geohash.length());
        int min = fieldType().isGeohashPrefixEnabled() ? 1 : geohash.length();

        for (int i = len; i >= min; i--) {
            // side effect of this call is adding the field
            geohashMapper.parse(context.createExternalValueContext(geohash.substring(0, i)));
        }
    }

    private void parsePointFromString(ParseContext context, GeoPoint sparse, String point) throws IOException {
        if (point.indexOf(',') < 0) {
            parse(context, sparse.resetFromGeoHash(point), point);
        } else {
            parse(context, sparse.resetFromString(point), null);
        }
    }

    private void parse(ParseContext context, GeoPoint point, String geohash) throws IOException {
        if (fieldType().normalizeLat() || fieldType().normalizeLon()) {
            GeoUtils.normalizePoint(point, fieldType().normalizeLat(), fieldType().normalizeLon());
        }

        if (fieldType().validateLat()) {
            if (point.lat() > 90.0 || point.lat() < -90.0) {
                throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
            }
        }
        if (fieldType().validateLon()) {
            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
            }
        }

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().names().indexName(), Double.toString(point.lat()) + ',' + Double.toString(point.lon()), fieldType());
            context.doc().add(field);
        }
        if (fieldType().isGeohashEnabled()) {
            if (geohash == null) {
                geohash = GeoHashUtils.encode(point.lat(), point.lon());
            }
            addGeohashField(context, geohash);
        }
        if (fieldType().isLatLonEnabled()) {
            latMapper.parse(context.createExternalValueContext(point.lat()));
            lonMapper.parse(context.createExternalValueContext(point.lon()));
        }
        if (fieldType().hasDocValues()) {
            CustomGeoPointDocValuesField field = (CustomGeoPointDocValuesField) context.doc().getByKey(fieldType().names().indexName());
            if (field == null) {
                field = new CustomGeoPointDocValuesField(fieldType().names().indexName(), point.lat(), point.lon());
                context.doc().addWithKey(fieldType().names().indexName(), field);
            } else {
                field.add(point.lat(), point.lon());
            }
        }
        multiFields.parse(this, context);
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> extras = new ArrayList<>();
        if (fieldType().isGeohashEnabled()) {
            extras.add(geohashMapper);
        }
        if (fieldType().isLatLonEnabled()) {
            extras.add(latMapper);
            extras.add(lonMapper);
        }
        return Iterators.concat(super.iterator(), extras.iterator());
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || pathType != Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase(Locale.ROOT));
        }
        if (includeDefaults || fieldType().isLatLonEnabled() != Defaults.ENABLE_LATLON) {
            builder.field("lat_lon", fieldType().isLatLonEnabled());
        }
        if (includeDefaults || fieldType().isGeohashEnabled() != Defaults.ENABLE_GEOHASH) {
            builder.field("geohash", fieldType().isGeohashEnabled());
        }
        if (includeDefaults || fieldType().isGeohashPrefixEnabled() != Defaults.ENABLE_GEOHASH_PREFIX) {
            builder.field("geohash_prefix", fieldType().isGeohashPrefixEnabled());
        }
        if (fieldType().isGeohashEnabled() && (includeDefaults || fieldType().geohashPrecision() != Defaults.GEO_HASH_PRECISION)) {
            builder.field("geohash_precision", fieldType().geohashPrecision());
        }
        if (fieldType().isLatLonEnabled() && (includeDefaults || fieldType().latFieldType().numericPrecisionStep() != NumericUtils.PRECISION_STEP_DEFAULT)) {
            builder.field("precision_step", fieldType().latFieldType().numericPrecisionStep());
        }
        if (includeDefaults || fieldType().validateLat() != Defaults.VALIDATE_LAT || fieldType().validateLon() != Defaults.VALIDATE_LON) {
            if (fieldType().validateLat() && fieldType().validateLon()) {
                builder.field("validate", true);
            } else if (!fieldType().validateLat() && !fieldType().validateLon()) {
                builder.field("validate", false);
            } else {
                if (includeDefaults || fieldType().validateLat() != Defaults.VALIDATE_LAT) {
                    builder.field("validate_lat", fieldType().validateLat());
                }
                if (includeDefaults || fieldType().validateLon() != Defaults.VALIDATE_LON) {
                    builder.field("validate_lon", fieldType().validateLon());
                }
            }
        }
        if (includeDefaults || fieldType().normalizeLat() != Defaults.NORMALIZE_LAT || fieldType().normalizeLon() != Defaults.NORMALIZE_LON) {
            if (fieldType().normalizeLat() && fieldType().normalizeLon()) {
                builder.field("normalize", true);
            } else if (!fieldType().normalizeLat() && !fieldType().normalizeLon()) {
                builder.field("normalize", false);
            } else {
                if (includeDefaults || fieldType().normalizeLat() != Defaults.NORMALIZE_LAT) {
                    builder.field("normalize_lat", fieldType().normalizeLat());
                }
                if (includeDefaults || fieldType().normalizeLon() != Defaults.NORMALIZE_LON) {
                    builder.field("normalize_lon", fieldType().normalizeLon());
                }
            }
        }
    }

    public static class CustomGeoPointDocValuesField extends CustomNumericDocValuesField {

        private final ObjectHashSet<GeoPoint> points;

        public CustomGeoPointDocValuesField(String name, double lat, double lon) {
            super(name);
            points = new ObjectHashSet<>(2);
            points.add(new GeoPoint(lat, lon));
        }

        public void add(double lat, double lon) {
            points.add(new GeoPoint(lat, lon));
        }

        @Override
        public BytesRef binaryValue() {
            final byte[] bytes = new byte[points.size() * 16];
            int off = 0;
            for (Iterator<ObjectCursor<GeoPoint>> it = points.iterator(); it.hasNext(); ) {
                final GeoPoint point = it.next().value;
                ByteUtils.writeDoubleLE(point.getLat(), bytes, off);
                ByteUtils.writeDoubleLE(point.getLon(), bytes, off + 8);
                off += 16;
            }
            return new BytesRef(bytes);
        }
    }

}
