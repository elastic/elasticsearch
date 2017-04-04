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
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;

/** A formatter for values as returned by the fielddata/doc-values APIs. */
public interface DocValueFormat extends NamedWriteable, ToXContentObject {

    /** Format a long value. This is used by terms and histogram aggregations
     *  to format keys for fields that use longs as a doc value representation
     *  such as the {@code long} and {@code date} fields. */
    String format(long value);

    /** Format a double value. This is used by terms and stats aggregations
     *  to format keys for fields that use numbers as a doc value representation
     *  such as the {@code long}, {@code double} or {@code date} fields. */
    String format(double value);

    /** Format a double value. This is used by terms aggregations to format
     *  keys for fields that use binary doc value representations such as the
     *  {@code keyword} and {@code ip} fields. */
    String format(BytesRef value);

    /** Parse a value that was formatted with {@link #format(long)} back to the
     *  original long value. */
    long parseLong(String value, boolean roundUp, LongSupplier now);

    /** Parse a value that was formatted with {@link #format(double)} back to
     *  the original double value. */
    double parseDouble(String value, boolean roundUp, LongSupplier now);

    /** Parse a value that was formatted with {@link #format(BytesRef)} back
     *  to the original BytesRef. */
    BytesRef parseBytesRef(String value);

    DocValueFormat RAW = new DocValueFormat() {

        static final String NAME = "raw";

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public String format(long value) {
            return Long.toString(value);
        }

        @Override
        public String format(double value) {
            return Double.toString(value);
        }

        @Override
        public String format(BytesRef value) {
            return value.utf8ToString();
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().startObject(NAME).endObject().endObject();
        }
    };

    final class DateTime implements DocValueFormat {

        public static final String NAME = "date_time";

        final FormatDateTimeFormatter formatter;
        final DateTimeZone timeZone;
        private final DateMathParser parser;

        public DateTime(FormatDateTimeFormatter formatter, DateTimeZone timeZone) {
            this.formatter = Objects.requireNonNull(formatter);
            this.timeZone = Objects.requireNonNull(timeZone);
            this.parser = new DateMathParser(formatter);
        }

        public DateTime(StreamInput in) throws IOException {
            this(Joda.forPattern(in.readString()), DateTimeZone.forID(in.readString()));
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(formatter.format());
            out.writeString(timeZone.getID());
        }

        @Override
        public String format(long value) {
            return formatter.printer().withZone(timeZone).print(value);
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }

        @Override
        public String format(BytesRef value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            return parser.parse(value, now, roundUp, timeZone);
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            return parseLong(value, roundUp, now);
        }

        @Override
        public BytesRef parseBytesRef(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(NAME);
            builder.field("pattern", formatter.format());
            builder.field("timezone", timeZone.getID());
            builder.endObject();
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            //TODO tlrx This is wrong
            DateTime that = (DateTime) o;
            return Objects.equals(formatter.format(), that.formatter.format())
                    && Objects.equals(formatter.locale(), that.formatter.locale())
                    && Objects.equals(timeZone, that.timeZone);
        }

        @Override
        public int hashCode() {
            return Objects.hash(formatter.format(), formatter.locale(), timeZone);
        }
    }

    DocValueFormat GEOHASH = new DocValueFormat() {

        static final String NAME = "geo_hash";

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public String format(long value) {
            return GeoHashUtils.stringEncode(value);
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }

        @Override
        public String format(BytesRef value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef parseBytesRef(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().startObject(NAME).endObject().endObject();
        }
    };

    DocValueFormat BOOLEAN = new DocValueFormat() {

        static final String NAME = "bool";

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public String format(long value) {
            return java.lang.Boolean.valueOf(value != 0).toString();
        }

        @Override
        public String format(double value) {
            return java.lang.Boolean.valueOf(value != 0).toString();
        }

        @Override
        public String format(BytesRef value) {
            throw new UnsupportedOperationException();
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

        @Override
        public BytesRef parseBytesRef(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().startObject(NAME).endObject().endObject();
        }
    };

    DocValueFormat IP = new DocValueFormat() {

        static final String NAME = "ip";

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public String format(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(BytesRef value) {
            byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
            InetAddress inet = InetAddressPoint.decode(bytes);
            return NetworkAddress.format(inet);
        }

        @Override
        public long parseLong(String value, boolean roundUp, LongSupplier now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double parseDouble(String value, boolean roundUp, LongSupplier now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef parseBytesRef(String value) {
            return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(value)));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().startObject(NAME).endObject().endObject();
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
            return format.format(value);
        }

        @Override
        public String format(BytesRef value) {
            throw new UnsupportedOperationException();
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
        public BytesRef parseBytesRef(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(NAME);
            builder.field("pattern", pattern);
            builder.endObject();
            return builder.endObject();
        }
    }

    static DocValueFormat fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);

        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);

        DocValueFormat docValueFormat = null;

        String currentFieldName = parser.currentName();
        if ("raw".equals(currentFieldName)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
            docValueFormat = DocValueFormat.RAW;

        } else if ("date_time".equals(currentFieldName)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

            String pattern = null, timezone = null;
            while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("pattern".equals(currentFieldName)) {
                        pattern = parser.text();
                    } else if ("timezone".equals(currentFieldName)) {
                        timezone = parser.text();
                    } else {
                        throwUnknownField(currentFieldName, parser.getTokenLocation());
                    }
                } else {
                    throwUnknownToken(token, parser.getTokenLocation());
                }
            }
            docValueFormat = new DateTime(Joda.forPattern(pattern), DateTimeZone.forID(timezone));
        }

        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return docValueFormat;
    }
}
