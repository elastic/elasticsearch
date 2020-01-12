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

package org.elasticsearch.search;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;
import java.util.function.LongSupplier;

/** A formatter for values as returned by the fielddata/doc-values APIs. */
public interface DocValueFormat extends NamedWriteable {

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
    default BytesRef parseBytesRef(String value) {
        throw new UnsupportedOperationException();
    }

    DocValueFormat RAW = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "raw";
        }

        @Override
        public void writeTo(StreamOutput out) {
        }

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
            return value.utf8ToString();
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
        public BytesRef parseBytesRef(String value) {
            return new BytesRef(value);
        }

        @Override
        public String toString() {
            return "raw";
        }
    };

    DocValueFormat BINARY = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "binary";
        }

        @Override
        public void writeTo(StreamOutput out) {
        }

        @Override
        public String format(BytesRef value) {
            return Base64.getEncoder()
                    .withoutPadding()
                    .encodeToString(Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length));
        }

        @Override
        public BytesRef parseBytesRef(String value) {
            return new BytesRef(Base64.getDecoder().decode(value));
        }
    };

    static DocValueFormat withNanosecondResolution(final DocValueFormat format) {
        if (format instanceof DateTime) {
            DateTime dateTime = (DateTime) format;
            return new DateTime(dateTime.formatter, dateTime.timeZone, DateFieldMapper.Resolution.NANOSECONDS);
        } else {
            throw new IllegalArgumentException("trying to convert a known date time formatter to a nanosecond one, wrong field used?");
        }
    }

    final class DateTime implements DocValueFormat {

        public static final String NAME = "date_time";

        final DateFormatter formatter;
        final ZoneId timeZone;
        private final DateMathParser parser;
        final DateFieldMapper.Resolution resolution;

        public DateTime(DateFormatter formatter, ZoneId timeZone, DateFieldMapper.Resolution resolution) {
            this.formatter = formatter;
            this.timeZone = Objects.requireNonNull(timeZone);
            this.parser = formatter.toDateMathParser();
            this.resolution = resolution;
        }

        public DateTime(StreamInput in) throws IOException {
            this.formatter = DateFormatter.forPattern(in.readString());
            this.parser = formatter.toDateMathParser();
            String zoneId = in.readString();
            this.timeZone = ZoneId.of(zoneId);
            this.resolution = DateFieldMapper.Resolution.ofOrdinal(in.readVInt());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(formatter.pattern());
            out.writeString(timeZone.getId());
            out.writeVInt(resolution.ordinal());
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
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            return resolution.convert(parser.parse(value, now, roundUp, timeZone));
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            return parseLong(value, roundUp, now);
        }
    }

    DocValueFormat GEOHASH = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "geo_hash";
        }

        @Override
        public void writeTo(StreamOutput out) {
        }

        @Override
        public String format(long value) {
            return Geohash.stringEncode(value);
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }
    };

    DocValueFormat GEOTILE = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "geo_tile";
        }

        @Override
        public void writeTo(StreamOutput out) {
        }

        @Override
        public String format(long value) {
            return GeoTileUtils.stringEncode(value);
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }
    };

    DocValueFormat BOOLEAN = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "bool";
        }

        @Override
        public void writeTo(StreamOutput out) {
        }

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

    DocValueFormat IP = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "ip";
        }

        @Override
        public void writeTo(StreamOutput out) {
        }

        @Override
        public String format(BytesRef value) {
            byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
            InetAddress inet = InetAddressPoint.decode(bytes);
            return NetworkAddress.format(inet);
        }

        @Override
        public BytesRef parseBytesRef(String value) {
            return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(value)));
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

        public Decimal(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public String getWriteableName() {
            return NAME;
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
                throw new RuntimeException(e);
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
                throw new RuntimeException(e);
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
    }
}
