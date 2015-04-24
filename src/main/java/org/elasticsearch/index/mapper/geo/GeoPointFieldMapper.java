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

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Objects;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
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
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ObjectMapperListener;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.CustomNumericDocValuesField;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
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
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
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
            }
            StringFieldMapper geohashMapper = null;
            if (enableGeoHash) {
                geohashMapper = stringField(Names.GEOHASH).index(true).tokenized(false).includeInAll(false).omitNorms(true).indexOptions(IndexOptions.DOCS).build(context);
            }
            context.path().remove();

            context.path().pathType(origPathType);

            // this is important: even if geo points feel like they need to be tokenized to distinguish lat from lon, we actually want to
            // store them as a single token.
            fieldType.setTokenized(false);

            return new GeoPointFieldMapper(buildNames(context), fieldType, docValues, indexAnalyzer, searchAnalyzer,
                    similarity, fieldDataSettings, context.indexSettings(), origPathType, enableLatLon, enableGeoHash, enableGeohashPrefix, precisionStep,
                    geoHashPrecision, latMapper, lonMapper, geohashMapper, validateLon, validateLat, normalizeLon, normalizeLat
            , multiFieldsBuilder.build(this, context));
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
                    builder.validateLat = XContentMapValues.nodeBooleanValue(fieldNode);
                    builder.validateLon = XContentMapValues.nodeBooleanValue(fieldNode);
                    iterator.remove();
                } else if (fieldName.equals("validate_lon")) {
                    builder.validateLon = XContentMapValues.nodeBooleanValue(fieldNode);
                    iterator.remove();
                } else if (fieldName.equals("validate_lat")) {
                    builder.validateLat = XContentMapValues.nodeBooleanValue(fieldNode);
                    iterator.remove();
                } else if (fieldName.equals("normalize")) {
                    builder.normalizeLat = XContentMapValues.nodeBooleanValue(fieldNode);
                    builder.normalizeLon = XContentMapValues.nodeBooleanValue(fieldNode);
                    iterator.remove();
                } else if (fieldName.equals("normalize_lat")) {
                    builder.normalizeLat = XContentMapValues.nodeBooleanValue(fieldNode);
                    iterator.remove();
                } else if (fieldName.equals("normalize_lon")) {
                    builder.normalizeLon = XContentMapValues.nodeBooleanValue(fieldNode);
                    iterator.remove();
                } else if (parseMultiField(builder, name, parserContext, fieldName, fieldNode)) {
                    iterator.remove();
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
                throw new ElasticsearchIllegalStateException("No encoding for " + numBytesPerValue + " bytes per value");
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

    private final boolean enableLatLon;

    private final boolean enableGeoHash;

    private final boolean enableGeohashPrefix;

    private final Integer precisionStep;

    private final int geoHashPrecision;

    private final DoubleFieldMapper latMapper;

    private final DoubleFieldMapper lonMapper;

    private final StringFieldMapper geohashMapper;

    private boolean validateLon;
    private boolean validateLat;

    private final boolean normalizeLon;
    private final boolean normalizeLat;

    public GeoPointFieldMapper(FieldMapper.Names names, FieldType fieldType, Boolean docValues,
            NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
            SimilarityProvider similarity, @Nullable Settings fieldDataSettings, Settings indexSettings,
            ContentPath.Type pathType, boolean enableLatLon, boolean enableGeoHash, boolean enableGeohashPrefix, Integer precisionStep, int geoHashPrecision,
            DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper, StringFieldMapper geohashMapper,
            boolean validateLon, boolean validateLat,
            boolean normalizeLon, boolean normalizeLat, MultiFields multiFields) {
        super(names, 1f, fieldType, docValues, null, indexAnalyzer, similarity, null, fieldDataSettings, indexSettings, multiFields, null);
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
    
    @Override
    protected boolean defaultDocValues() {
        return false;
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

    int geoHashPrecision() {
        return geoHashPrecision;
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
    public Mapper parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);
        context.path().add(name());

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

    private void parseGeohashField(ParseContext context, String geohash) throws IOException {
        int len = Math.min(geoHashPrecision, geohash.length());
        int min = enableGeohashPrefix ? 1 : geohash.length();

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
        if (normalizeLat || normalizeLon) {
            GeoUtils.normalizePoint(point, normalizeLat, normalizeLon);
        }

        if (validateLat) {
            if (point.lat() > 90.0 || point.lat() < -90.0) {
                throw new ElasticsearchIllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
            }
        }
        if (validateLon) {
            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new ElasticsearchIllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
            }
        }

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(names.indexName(), Double.toString(point.lat()) + ',' + Double.toString(point.lon()), fieldType);
            context.doc().add(field);
        }
        if (enableGeoHash) {
            if (geohash == null) {
                geohash = GeoHashUtils.encode(point.lat(), point.lon());
            }
            parseGeohashField(context, geohash);
        }
        if (enableLatLon) {
            latMapper.parse(context.createExternalValueContext(point.lat()));
            lonMapper.parse(context.createExternalValueContext(point.lon()));
        }
        if (hasDocValues()) {
            CustomGeoPointDocValuesField field = (CustomGeoPointDocValuesField) context.doc().getByKey(names().indexName());
            if (field == null) {
                field = new CustomGeoPointDocValuesField(names().indexName(), point.lat(), point.lon());
                context.doc().addWithKey(names().indexName(), field);
            } else {
                field.add(point.lat(), point.lon());
            }
        }
        multiFields.parse(this, context);
    }

    @Override
    public void close() {
        super.close();
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
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        GeoPointFieldMapper fieldMergeWith = (GeoPointFieldMapper) mergeWith;

        if (this.enableLatLon != fieldMergeWith.enableLatLon) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different lat_lon");
        }
        if (this.enableGeoHash != fieldMergeWith.enableGeoHash) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different geohash");
        }
        if (this.geoHashPrecision != fieldMergeWith.geoHashPrecision) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different geohash_precision");
        }
        if (this.enableGeohashPrefix != fieldMergeWith.enableGeohashPrefix) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different geohash_prefix");
        }
        if (this.normalizeLat != fieldMergeWith.normalizeLat) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different normalize_lat");
        }
        if (this.normalizeLon != fieldMergeWith.normalizeLon) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different normalize_lon");
        }
        if (!Objects.equal(this.precisionStep, fieldMergeWith.precisionStep)) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different precision_step");
        }
        if (this.validateLat != fieldMergeWith.validateLat) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different validate_lat");
        }
        if (this.validateLon != fieldMergeWith.validateLon) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different validate_lon");
        }
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
        if (includeDefaults || pathType != Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase(Locale.ROOT));
        }
        if (includeDefaults || enableLatLon != Defaults.ENABLE_LATLON) {
            builder.field("lat_lon", enableLatLon);
        }
        if (includeDefaults || enableGeoHash != Defaults.ENABLE_GEOHASH) {
            builder.field("geohash", enableGeoHash);
        }
        if (includeDefaults || enableGeohashPrefix != Defaults.ENABLE_GEOHASH_PREFIX) {
            builder.field("geohash_prefix", enableGeohashPrefix);
        }
        if (includeDefaults || geoHashPrecision != Defaults.GEO_HASH_PRECISION) {
            builder.field("geohash_precision", geoHashPrecision);
        }
        if (includeDefaults || precisionStep != null) {
            builder.field("precision_step", precisionStep);
        }
        if (includeDefaults || validateLat != Defaults.VALIDATE_LAT || validateLon != Defaults.VALIDATE_LON) {
            if (validateLat && validateLon) {
                builder.field("validate", true);
            } else if (!validateLat && !validateLon) {
                builder.field("validate", false);
            } else {
                if (includeDefaults || validateLat != Defaults.VALIDATE_LAT) {
                    builder.field("validate_lat", validateLat);
                }
                if (includeDefaults || validateLon != Defaults.VALIDATE_LON) {
                    builder.field("validate_lon", validateLon);
                }
            }
        }
        if (includeDefaults || normalizeLat != Defaults.NORMALIZE_LAT || normalizeLon != Defaults.NORMALIZE_LON) {
            if (normalizeLat && normalizeLon) {
                builder.field("normalize", true);
            } else if (!normalizeLat && !normalizeLon) {
                builder.field("normalize", false);
            } else {
                if (includeDefaults || normalizeLat != Defaults.NORMALIZE_LAT) {
                    builder.field("normalize_lat", normalizeLat);
                }
                if (includeDefaults || normalizeLon != Defaults.NORMALIZE_LON) {
                    builder.field("normalize_lon", normalizeLat);
                }
            }
        }
    }

    public static class CustomGeoPointDocValuesField extends CustomNumericDocValuesField {

        public static final FieldType TYPE = new FieldType();
        static {
          TYPE.setDocValuesType(DocValuesType.BINARY);
          TYPE.freeze();
        }

        private final ObjectOpenHashSet<GeoPoint> points;

        public CustomGeoPointDocValuesField(String  name, double lat, double lon) {
            super(name);
            points = new ObjectOpenHashSet<>(2);
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
