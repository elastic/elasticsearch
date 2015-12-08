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
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.CustomNumericDocValuesField;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


/**
 * Parsing: We handle:
 * <p>
 * - "field" : "geo_hash"
 * - "field" : "lat,lon"
 * - "field" : {
 * "lat" : 1.1,
 * "lon" : 2.1
 * }
 */
public class GeoPointFieldMapperLegacy extends BaseGeoPointFieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "geo_point";

    public static class Names extends BaseGeoPointFieldMapper.Names {
        public static final String COERCE = "coerce";
    }

    public static class Defaults extends BaseGeoPointFieldMapper.Defaults{
        public static final Explicit<Boolean> COERCE = new Explicit(false, false);

        public static final GeoPointFieldType FIELD_TYPE = new GeoPointFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    /**
     * Concrete builder for legacy GeoPointField
     */
    public static class Builder extends BaseGeoPointFieldMapper.Builder<Builder, GeoPointFieldMapperLegacy> {

        private Boolean coerce;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        public Builder coerce(boolean coerce) {
            this.coerce = coerce;
            return builder;
        }

        protected Explicit<Boolean> coerce(BuilderContext context) {
            if (coerce != null) {
                return new Explicit<>(coerce, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(context.indexSettings().getAsBoolean("index.mapping.coerce", Defaults.COERCE.value()), false);
            }
            return Defaults.COERCE;
        }

        @Override
        public GeoPointFieldMapperLegacy build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                               MappedFieldType defaultFieldType, Settings indexSettings, ContentPath.Type pathType, DoubleFieldMapper latMapper,
                                               DoubleFieldMapper lonMapper, StringFieldMapper geoHashMapper, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                               CopyTo copyTo) {
            fieldType.setTokenized(false);
            setupFieldType(context);
            fieldType.setHasDocValues(false);
            defaultFieldType.setHasDocValues(false);
            return new GeoPointFieldMapperLegacy(simpleName, fieldType, defaultFieldType, indexSettings, pathType, latMapper, lonMapper,
                    geoHashMapper, multiFields, ignoreMalformed, coerce(context), copyTo);
        }

        @Override
        public GeoPointFieldMapperLegacy build(BuilderContext context) {
            return super.build(context);
        }
    }

    public static Builder parse(Builder builder, Map<String, Object> node, Mapper.TypeParser.ParserContext parserContext) throws MapperParsingException {
        final boolean indexCreatedBeforeV2_0 = parserContext.indexVersionCreated().before(Version.V_2_0_0);
        for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            String propName = Strings.toUnderscoreCase(entry.getKey());
            Object propNode = entry.getValue();
            if (indexCreatedBeforeV2_0 && propName.equals("validate")) {
                builder.ignoreMalformed = !XContentMapValues.nodeBooleanValue(propNode);
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && propName.equals("validate_lon")) {
                builder.ignoreMalformed = !XContentMapValues.nodeBooleanValue(propNode);
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && propName.equals("validate_lat")) {
                builder.ignoreMalformed = !XContentMapValues.nodeBooleanValue(propNode);
                iterator.remove();
            } else if (propName.equals(Names.COERCE)) {
                builder.coerce = XContentMapValues.nodeBooleanValue(propNode);
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && propName.equals("normalize")) {
                builder.coerce = XContentMapValues.nodeBooleanValue(propNode);
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && propName.equals("normalize_lat")) {
                builder.coerce = XContentMapValues.nodeBooleanValue(propNode);
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && propName.equals("normalize_lon")) {
                builder.coerce = XContentMapValues.nodeBooleanValue(propNode);
                iterator.remove();
            }
        }
        return builder;
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

    protected Explicit<Boolean> coerce;

    public GeoPointFieldMapperLegacy(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings,
                                     ContentPath.Type pathType, DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper,
                                     StringFieldMapper geoHashMapper, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                     Explicit<Boolean> coerce, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, pathType, latMapper, lonMapper, geoHashMapper, multiFields,
                ignoreMalformed, copyTo);
        this.coerce = coerce;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);

        GeoPointFieldMapperLegacy gpfmMergeWith = (GeoPointFieldMapperLegacy) mergeWith;
        if (gpfmMergeWith.coerce.explicit()) {
            if (coerce.explicit() && coerce.value() != gpfmMergeWith.coerce.value()) {
                throw new IllegalArgumentException("mapper [" + fieldType().names().fullName() + "] has different [coerce]");
            }
        }

        if (gpfmMergeWith.coerce.explicit()) {
            this.coerce = gpfmMergeWith.coerce;
        }
    }

    @Override
    protected void parse(ParseContext context, GeoPoint point, String geoHash) throws IOException {
        boolean validPoint = false;
        if (coerce.value() == false && ignoreMalformed.value() == false) {
            if (point.lat() > 90.0 || point.lat() < -90.0) {
                throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
            }
            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
            }
            validPoint = true;
        }

        if (coerce.value() == true && validPoint == false) {
            // by setting coerce to false we are assuming all geopoints are already in a valid coordinate system
            // thus this extra step can be skipped
            GeoUtils.normalizePoint(point, true, true);
        }

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().names().indexName(), Double.toString(point.lat()) + ',' + Double.toString(point.lon()), fieldType());
            context.doc().add(field);
        }

        super.parse(context, point, geoHash);

        if (fieldType().hasDocValues()) {
            CustomGeoPointDocValuesField field = (CustomGeoPointDocValuesField) context.doc().getByKey(fieldType().names().indexName());
            if (field == null) {
                field = new CustomGeoPointDocValuesField(fieldType().names().indexName(), point.lat(), point.lon());
                context.doc().addWithKey(fieldType().names().indexName(), field);
            } else {
                field.add(point.lat(), point.lon());
            }
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || coerce.explicit()) {
            builder.field(Names.COERCE, coerce.value());
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