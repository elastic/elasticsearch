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
package org.elasticsearch.search.aggregations.support.format;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.TimeZone;

/**
 * A strategy for formatting time represented as millis long value to string
 */
public interface ValueFormatter extends Streamable {

    public final static ValueFormatter RAW = new Raw();
    public final static ValueFormatter IPv4 = new IPv4Formatter();
    public final static ValueFormatter GEOHASH = new GeoHash();
    public final static ValueFormatter BOOLEAN = new BooleanFormatter();

    /**
     * Uniquely identifies this formatter (used for efficient serialization)
     *
     * @return  The id of this formatter
     */
    byte id();

    /**
     * Formats the given millis time value (since the epoch) to string.
     *
     * @param value The long value to format.
     * @return      The formatted value as string.
     */
    String format(long value);

    /**
     * The 
     * @param value double The double value to format.
     * @return      The formatted value as string
     */
    String format(double value);


    static class Raw implements ValueFormatter {

        static final byte ID = 1;

        @Override
        public String format(long value) {
            return String.valueOf(value);
        }

        @Override
        public String format(double value) {
            return String.valueOf(value);
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }
    }

    /**
     * A time formatter which is based on date/time format.
     */
    public static class DateTime implements ValueFormatter {

        public static final ValueFormatter DEFAULT = new ValueFormatter.DateTime(DateFieldMapper.Defaults.DATE_TIME_FORMATTER);
        private DateTimeZone timeZone = DateTimeZone.UTC;

        public static DateTime mapper(DateFieldMapper mapper) {
            return new DateTime(mapper.dateTimeFormatter());
        }

        static final byte ID = 2;

        FormatDateTimeFormatter formatter;

        DateTime() {} // for serialization

        public DateTime(String format) {
            this.formatter = Joda.forPattern(format);
        }

        public DateTime(FormatDateTimeFormatter formatter) {
            this.formatter = formatter;
        }

        @Override
        public String format(long time) {
            return formatter.printer().withZone(timeZone).print(time);
        }

        public void setTimeZone(DateTimeZone timeZone) {
            this.timeZone = timeZone;
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            formatter = Joda.forPattern(in.readString());
            timeZone = DateTimeZone.forID(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(formatter.format());
            out.writeString(timeZone.getID());
        }
    }

    public static abstract class Number implements ValueFormatter {

        NumberFormat format;

        Number() {} // for serialization

        Number(NumberFormat format) {
            this.format = format;
        }

        @Override
        public String format(long value) {
            return format.format(value);
        }

        @Override
        public String format(double value) {
            return format.format(value);
        }

        public static class Pattern extends Number {

            private static final DecimalFormatSymbols SYMBOLS = new DecimalFormatSymbols(Locale.ROOT);

            static final byte ID = 4;

            String pattern;

            Pattern() {} // for serialization

            public Pattern(String pattern) {
                super(new DecimalFormat(pattern, SYMBOLS));
                this.pattern = pattern;
            }

            @Override
            public byte id() {
                return ID;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                pattern = in.readString();
                format = new DecimalFormat(pattern, SYMBOLS);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(pattern);
            }
        }
    }

    static class IPv4Formatter implements ValueFormatter {

        static final byte ID = 6;

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public String format(long value) {
            return IpFieldMapper.longToIp(value);
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }
    }

    static class GeoHash implements ValueFormatter {

        static final byte ID = 8;

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public String format(long value) {
            return GeoHashUtils.toString(value);
        }

        @Override
        public String format(double value) {
            return format((long) value);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
    
    static class BooleanFormatter implements ValueFormatter {

        static final byte ID = 10;

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public String format(long value) {
            return Boolean.valueOf(value != 0).toString();
        }

        @Override
        public String format(double value) {
            return Boolean.valueOf(value != 0).toString();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }
    }
}
