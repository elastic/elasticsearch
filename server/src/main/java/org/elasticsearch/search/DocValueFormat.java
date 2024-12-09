/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/** A formatter for values as returned by the fielddata/doc-values APIs. */
public interface DocValueFormat extends NamedWriteable {
    long MASK_2_63 = 0x8000000000000000L;
    BigInteger BIGINTEGER_2_64_MINUS_ONE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE); // 2^64 -1

    /** Format a long value. This is used by terms and histogram aggregations
     *  to format keys for fields that use longs as a doc value representation
     *  such as the {@code long} and {@code date} fields. */
    default Object format(long value) {
        throw new UnsupportedOperationException();
    }

    /** Format a double value. This is used by terms and stats aggregations
     *  to format keys for fields that use numbers as a doc value representation
     *  such as the {@code long}, {@code double} or {@code date} fields. */
    default Object format(double value) {
        throw new UnsupportedOperationException();
    }

    /** Format a binary value. This is used by terms aggregations to format
     *  keys for fields that use binary doc value representations such as the
     *  {@code keyword} and {@code ip} fields. */
    default Object format(BytesRef value) {
        throw new UnsupportedOperationException();
    }

    /** Parse a value that was formatted with {@link #format(long)} back to the
     *  original long value. */
    default long parseLong(String value, boolean roundUp, LongSupplier now) {
        throw new UnsupportedOperationException();
    }

    /** Parse a value that was formatted with {@link #format(double)} back to
     *  the original double value. */
    default double parseDouble(String value, boolean roundUp, LongSupplier now) {
        throw new UnsupportedOperationException();
    }

    /** Parse a value that was formatted with {@link #format(BytesRef)} back
     *  to the original BytesRef. */
    default BytesRef parseBytesRef(Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Formats a value of a sort field in a search response. This is used by {@link SearchSortValues}
     * to avoid sending the internal representation of a value of a sort field in a search response.
     * The default implementation formats {@link BytesRef} but leave other types as-is.
     */
    default Object formatSortValue(Object value) {
        if (value instanceof BytesRef) {
            return format((BytesRef) value);
        }
        return value;
    }

    DocValueFormat RAW = RawDocValueFormat.INSTANCE;

    /**
     * Singleton, stateless formatter for "Raw" values, generally taken to mean keywords and other strings.
     */
    class RawDocValueFormat implements DocValueFormat {

        public static final DocValueFormat INSTANCE = new RawDocValueFormat();

        private RawDocValueFormat() {}

        @Override
        public String getWriteableName() {
            return "raw";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public Long format(long value) {
            return value;
        }

        @Override
        public Double format(double value) {
            return value;
        }

        @Override
        public String format(BytesRef value) {
            try {
                return value.utf8ToString();
            } catch (Exception | AssertionError e) {
                throw new IllegalArgumentException("Failed trying to format bytes as UTF8.  Possibly caused by a mapping mismatch", e);
            }
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            try {
                // Prefer parsing as a long to avoid losing precision
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                // retry as a double
            }
            double d = Double.parseDouble(value);
            if (roundUp) {
                d = Math.ceil(d);
            } else {
                d = Math.floor(d);
            }
            return Math.round(d);
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            return Double.parseDouble(value);
        }

        @Override
        public BytesRef parseBytesRef(Object value) {
            return new BytesRef(value.toString());
        }

        @Override
        public String toString() {
            return "raw";
        }
    };

    DocValueFormat DENSE_VECTOR = DenseVectorDocValueFormat.INSTANCE;

    /**
     * Singleton, stateless formatter, for dense vector values, no need to actually format anything
     */
    class DenseVectorDocValueFormat implements DocValueFormat {

        public static final DocValueFormat INSTANCE = new DenseVectorDocValueFormat();

        private DenseVectorDocValueFormat() {}

        @Override
        public String getWriteableName() {
            return "dense_vector";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String toString() {
            return "dense_vector";
        }
    };

    DocValueFormat BINARY = BinaryDocValueFormat.INSTANCE;

    /**
     * Singleton, stateless formatter, for representing bytes as base64 strings
     */
    class BinaryDocValueFormat implements DocValueFormat {

        public static final DocValueFormat INSTANCE = new BinaryDocValueFormat();

        private BinaryDocValueFormat() {}

        @Override
        public String getWriteableName() {
            return "binary";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String format(BytesRef value) {
            return Base64.getEncoder().encodeToString(Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length));
        }

        @Override
        public BytesRef parseBytesRef(Object value) {
            return new BytesRef(Base64.getDecoder().decode(value.toString()));
        }
    };

    static DocValueFormat withNanosecondResolution(final DocValueFormat format) {
        if (format instanceof DateTime dateTime) {
            return new DateTime(dateTime.formatter, dateTime.timeZone, DateFieldMapper.Resolution.NANOSECONDS, dateTime.formatSortValues);
        } else {
            throw new IllegalArgumentException("trying to convert a known date time formatter to a nanosecond one, wrong field used?");
        }
    }

    static DocValueFormat enableFormatSortValues(DocValueFormat format) {
        if (format instanceof DateTime dateTime) {
            return new DateTime(dateTime.formatter, dateTime.timeZone, dateTime.resolution, true);
        }
        throw new IllegalArgumentException("require a date_time formatter; got [" + format.getWriteableName() + "]");
    }

    final class DateTime implements DocValueFormat {

        public static final String NAME = "date_time";

        final DateFormatter formatter;
        final ZoneId timeZone;
        private final DateMathParser parser;
        final DateFieldMapper.Resolution resolution;
        final boolean formatSortValues;

        public DateTime(DateFormatter formatter, ZoneId timeZone, DateFieldMapper.Resolution resolution) {
            this(formatter, timeZone, resolution, false);
        }

        private DateTime(DateFormatter formatter, ZoneId timeZone, DateFieldMapper.Resolution resolution, boolean formatSortValues) {
            this.timeZone = Objects.requireNonNull(timeZone);
            this.formatter = formatter.withZone(timeZone);
            this.parser = this.formatter.toDateMathParser();
            this.resolution = resolution;
            this.formatSortValues = formatSortValues;
        }

        private DateTime(StreamInput in) throws IOException {
            String formatterPattern = in.readString();
            Locale locale = in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
                ? LocaleUtils.parse(in.readString())
                : DateFieldMapper.DEFAULT_LOCALE;
            String zoneId = in.readString();
            this.timeZone = ZoneId.of(zoneId);
            this.formatter = DateFormatter.forPattern(formatterPattern).withZone(this.timeZone).withLocale(locale);
            this.parser = formatter.toDateMathParser();
            this.resolution = DateFieldMapper.Resolution.ofOrdinal(in.readVInt());
            if (in.getTransportVersion().between(TransportVersions.V_7_7_0, TransportVersions.V_8_0_0)) {
                /* when deserialising from 7.7+ nodes expect a flag indicating if a pattern is of joda style
                   This is only used to support joda style indices in 7.x, in 8 we no longer support this.
                   All indices in 8 should use java style pattern. Hence we can ignore this flag.
                */
                in.readBoolean();
            }
            this.formatSortValues = in.readBoolean();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public static DateTime readFrom(StreamInput in) throws IOException {
            final DateTime dateTime = new DateTime(in);
            if (in instanceof DelayableWriteable.Deduplicator d) {
                return d.deduplicate(dateTime);
            }
            return dateTime;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(formatter.pattern());
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeString(formatter.locale().toString());
            }
            out.writeString(timeZone.getId());
            out.writeVInt(resolution.ordinal());
            if (out.getTransportVersion().between(TransportVersions.V_7_7_0, TransportVersions.V_8_0_0)) {
                /* when serializing to 7.7+  send out a flag indicating if a pattern is of joda style
                   This is only used to support joda style indices in 7.x, in 8 we no longer support this.
                   All indices in 8 should use java style pattern. Hence this flag is always false.
                */
                out.writeBoolean(false);
            }
            out.writeBoolean(formatSortValues);
        }

        public DateMathParser getDateMathParser() {
            return parser;
        }

        @Override
        public String format(long value) {
            return formatter.format(resolution.toInstant(value).atZone(timeZone));
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }

        @Override
        public Object formatSortValue(Object value) {
            if (formatSortValues) {
                if (value instanceof Long) {
                    return format((Long) value);
                }
            }
            return value;
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            return resolution.convert(parser.parse(value, now, roundUp, timeZone));
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            return parseLong(value, roundUp, now);
        }

        @Override
        public String toString() {
            return "DocValueFormat.DateTime(" + formatter + ", " + timeZone + ", " + resolution + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DateTime that = (DateTime) o;
            return formatter.equals(that.formatter)
                && timeZone.equals(that.timeZone)
                && resolution == that.resolution
                && formatSortValues == that.formatSortValues;
        }

        @Override
        public int hashCode() {
            return Objects.hash(formatter, timeZone, resolution, formatSortValues);
        }
    }

    DocValueFormat GEOHASH = GeoHashDocValueFormat.INSTANCE;

    /**
     * Singleton, stateless formatter for geo hash values
     */
    class GeoHashDocValueFormat implements DocValueFormat {

        public static final DocValueFormat INSTANCE = new GeoHashDocValueFormat();

        private GeoHashDocValueFormat() {}

        @Override
        public String getWriteableName() {
            return "geo_hash";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String format(long value) {
            return Geohash.stringEncode(value);
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }
    };

    DocValueFormat GEOTILE = GeoTileDocValueFormat.INSTANCE;

    class GeoTileDocValueFormat implements DocValueFormat {

        public static final DocValueFormat INSTANCE = new GeoTileDocValueFormat();

        private GeoTileDocValueFormat() {}

        @Override
        public String getWriteableName() {
            return "geo_tile";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String format(long value) {
            return GeoTileUtils.stringEncode(value);
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            return GeoTileUtils.longEncode(value);
        }
    };

    DocValueFormat BOOLEAN = BooleanDocValueFormat.INSTANCE;

    /**
     * Stateless, Singleton formatter for boolean values.  Parses the strings "true" and "false" as inputs.
     */
    class BooleanDocValueFormat implements DocValueFormat {

        public static final DocValueFormat INSTANCE = new BooleanDocValueFormat();

        private BooleanDocValueFormat() {}

        @Override
        public String getWriteableName() {
            return "bool";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public Boolean format(long value) {
            return value != 0;
        }

        @Override
        public Boolean format(double value) {
            return value != 0;
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            switch (value) {
                case "false":
                    return 0;
                case "true":
                    return 1;
            }
            throw new IllegalArgumentException("Cannot parse boolean [" + value + "], expected either [true] or [false]");
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            return parseLong(value, roundUp, now);
        }
    };

    IpDocValueFormat IP = IpDocValueFormat.INSTANCE;

    /**
     * Stateless, singleton formatter for IP address data
     */
    class IpDocValueFormat implements DocValueFormat {

        public static final IpDocValueFormat INSTANCE = new IpDocValueFormat();

        private IpDocValueFormat() {}

        @Override
        public String getWriteableName() {
            return "ip";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String format(BytesRef value) {
            try {
                byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
                InetAddress inet = InetAddressPoint.decode(bytes);
                return NetworkAddress.format(inet);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Failed trying to format bytes as IP address.  Possibly caused by a mapping mismatch",
                    e
                );
            }
        }

        @Override
        public BytesRef parseBytesRef(Object value) {
            return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(value.toString())));
        }

        @Override
        public String toString() {
            return "ip";
        }
    };

    final class Decimal implements DocValueFormat {

        public static final String NAME = "decimal";
        private static final DecimalFormatSymbols SYMBOLS = new DecimalFormatSymbols(Locale.ROOT);

        final String pattern;
        private final NumberFormat format;

        public Decimal(String pattern) {
            this.pattern = pattern;
            this.format = new DecimalFormat(pattern, SYMBOLS);
        }

        private Decimal(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public static Decimal readFrom(StreamInput in) throws IOException {
            final Decimal decimal = new Decimal(in);
            if (in instanceof DelayableWriteable.Deduplicator d) {
                return d.deduplicate(decimal);
            }
            return decimal;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(pattern);
        }

        @Override
        public String format(long value) {
            return format.format(value);
        }

        @Override
        public String format(double value) {
            /*
             * Explicitly check for NaN, since it formats to "�" or "NaN" depending on JDK version.
             *
             * Decimal formatter uses the JRE's default symbol list (via Locale.ROOT above).  In JDK8,
             * this translates into using {@link sun.util.locale.provider.JRELocaleProviderAdapter}, which loads
             * {@link sun.text.resources.FormatData} for symbols.  There, `NaN` is defined as `\ufffd` (�)
             *
             * In JDK9+, {@link sun.util.cldr.CLDRLocaleProviderAdapter} is used instead, which loads
             * {@link sun.text.resources.cldr.FormatData}.  There, `NaN` is defined as `"NaN"`
             *
             * Since the character � isn't very useful, and makes the output change depending on JDK version,
             * we manually check to see if the value is NaN and return the string directly.
             */
            if (Double.isNaN(value)) {
                return String.valueOf(Double.NaN);
            }
            return format.format(value);
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            Number n;
            try {
                n = format.parse(value);
            } catch (ParseException e) {
                throw new RuntimeException("Cannot parse the value [" + value + "] using the pattern [" + pattern + "]", e);
            }
            if (format.isParseIntegerOnly()) {
                return n.longValue();
            } else {
                double d = n.doubleValue();
                if (roundUp) {
                    d = Math.ceil(d);
                } else {
                    d = Math.floor(d);
                }
                return Math.round(d);
            }
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            Number n;
            try {
                n = format.parse(value);
            } catch (ParseException e) {
                throw new RuntimeException("Cannot parse the value [" + value + "] using the pattern [" + pattern + "]", e);
            }
            return n.doubleValue();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Decimal that = (Decimal) o;
            return Objects.equals(pattern, that.pattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern);
        }

        @Override
        public String toString() {
            return pattern;
        }
    };

    DocValueFormat UNSIGNED_LONG_SHIFTED = UnsignedLongShiftedDocValueFormat.INSTANCE;

    /**
     * DocValues format for unsigned 64 bit long values,
     * that are stored as shifted signed 64 bit long values.
     */
    class UnsignedLongShiftedDocValueFormat implements DocValueFormat {

        public static final DocValueFormat INSTANCE = new UnsignedLongShiftedDocValueFormat();

        private UnsignedLongShiftedDocValueFormat() {}

        @Override
        public String getWriteableName() {
            return "unsigned_long_shifted";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String toString() {
            return "unsigned_long_shifted";
        }

        /**
         * Formats the unsigned long to the shifted long format
         */
        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            long parsedValue = Long.parseUnsignedLong(value);
            // subtract 2^63 or 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000
            // equivalent to flipping the first bit
            return parsedValue ^ MASK_2_63;
        }

        /**
         * Formats a raw docValue that is stored in the shifted long format to the unsigned long representation.
         */
        @Override
        public Object format(long value) {
            // add 2^63 or 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000,
            // equivalent to flipping the first bit
            long formattedValue = value ^ MASK_2_63;
            if (formattedValue >= 0) {
                return formattedValue;
            } else {
                return BigInteger.valueOf(formattedValue).and(BIGINTEGER_2_64_MINUS_ONE);
            }
        }

        @Override
        public Object formatSortValue(Object value) {
            if (value instanceof Long) {
                return format((Long) value);
            }
            return value;
        }

        /**
         * Double docValues of the unsigned_long field type are already in the formatted representation,
         * so we don't need to do anything here
         */
        @Override
        public Double format(double value) {
            return value;
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            return Double.parseDouble(value);
        }
    };

    DocValueFormat TIME_SERIES_ID = new TimeSeriesIdDocValueFormat();

    /**
     * DocValues format for time series id.
     */
    class TimeSeriesIdDocValueFormat implements DocValueFormat {
        private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

        private TimeSeriesIdDocValueFormat() {}

        @Override
        public String getWriteableName() {
            return "tsid";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String toString() {
            return "tsid";
        }

        /**
         * @param value The TSID as a {@link BytesRef}
         * @return the Base 64 encoded TSID
         */
        @Override
        public Object format(BytesRef value) {
            try {
                // NOTE: if the tsid is a map of dimension key/value pairs (as it was before introducing
                // tsid hashing) we just decode the map and return it.
                return RoutingPathFields.decodeAsMap(value);
            } catch (Exception e) {
                // NOTE: otherwise the _tsid field is just a hash and we can't decode it
                return TimeSeriesIdFieldMapper.encodeTsid(value);
            }
        }

        @Override
        public BytesRef parseBytesRef(Object value) {
            if (value instanceof BytesRef valueAsBytesRef) {
                return valueAsBytesRef;
            }
            if (value instanceof String valueAsString) {
                return new BytesRef(BASE64_DECODER.decode(valueAsString));
            }
            return parseBytesRefMap(value);
        }

        /**
         * After introducing tsid hashing this tsid parsing logic is deprecated.
         * Tsid hashing does not allow us to parse the tsid extracting dimension fields key/values pairs.
         * @param value The Map encoding tsid dimension fields key/value pairs.
         *
         * @return a {@link BytesRef} representing a map of key/value pairs
         */
        private BytesRef parseBytesRefMap(Object value) {
            if (value instanceof Map<?, ?> == false) {
                throw new IllegalArgumentException("Cannot parse tsid object [" + value + "]");
            }

            Map<?, ?> m = (Map<?, ?>) value;
            RoutingPathFields routingPathFields = new RoutingPathFields(null);
            for (Map.Entry<?, ?> entry : m.entrySet()) {
                String f = entry.getKey().toString();
                Object v = entry.getValue();

                if (v instanceof String s) {
                    routingPathFields.addString(f, s);
                } else if (v instanceof Long l) {
                    routingPathFields.addLong(f, l);
                } else if (v instanceof Integer i) {
                    routingPathFields.addLong(f, i.longValue());
                } else if (v instanceof BigInteger ul) {
                    long ll = UNSIGNED_LONG_SHIFTED.parseLong(ul.toString(), false, () -> 0L);
                    routingPathFields.addUnsignedLong(f, ll);
                } else {
                    throw new IllegalArgumentException("Unexpected value in tsid object [" + v + "]");
                }
            }

            try {
                // NOTE: we can decode the tsid only if it is not hashed (represented as a map)
                return TimeSeriesIdFieldMapper.buildLegacyTsid(routingPathFields).toBytesRef();
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    };
}
