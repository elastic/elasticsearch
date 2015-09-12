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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.concurrent.Callable;

/**
 *
 */
public interface ValueParser {

    static final ValueParser IPv4 = new IPv4();
    static final ValueParser RAW = new Raw();
    static final ValueParser BOOLEAN = new Boolean();

    long parseLong(String value, SearchContext searchContext);

    double parseDouble(String value, SearchContext searchContext);


    /**
     * Knows how to parse datatime values based on date/time format
     */
    static class DateTime implements ValueParser {

        public static final DateTime DEFAULT = new DateTime(DateFieldMapper.Defaults.DATE_TIME_FORMATTER);

        private FormatDateTimeFormatter formatter;

        public DateTime(String format) {
            this(Joda.forPattern(format));
        }

        public DateTime(FormatDateTimeFormatter formatter) {
            this.formatter = formatter;
        }

        @Override
        public long parseLong(String value, SearchContext searchContext) {
            return formatter.parser().parseMillis(value);
        }

        @Override
        public double parseDouble(String value, SearchContext searchContext) {
            return parseLong(value, searchContext);
        }
    }

    /**
     * Knows how to parse datatime values based on elasticsearch's date math expression
     */
    static class DateMath implements ValueParser {

        public static final DateMath DEFAULT = new ValueParser.DateMath(new DateMathParser(DateFieldMapper.Defaults.DATE_TIME_FORMATTER), DateTimeZone.UTC);

        private DateMathParser parser;

        private DateTimeZone timezone = DateTimeZone.UTC;

        public DateMath(String format, DateTimeZone timezone) {
            this(new DateMathParser(Joda.forPattern(format)), timezone);
        }

        public DateMath(DateMathParser parser, @Nullable DateTimeZone timeZone) {
            this.parser = parser;
            if (timeZone != null) {
                this.timezone = timeZone;
            }
        }

        @Override
        public long parseLong(String value, final SearchContext searchContext) {
            final Callable<Long> now = new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return searchContext.nowInMillis();
                }
            };
            return parser.parse(value, now, false, timezone);
        }

        @Override
        public double parseDouble(String value, SearchContext searchContext) {
            return parseLong(value, searchContext);
        }

        public static DateMath mapper(DateFieldMapper.DateFieldType fieldType, @Nullable DateTimeZone timezone) {
            return new DateMath(new DateMathParser(fieldType.dateTimeFormatter()), timezone);
        }
    }

    /**
     * Knows how to parse IPv4 formats
     */
    static class IPv4 implements ValueParser {

        private IPv4() {
        }

        @Override
        public long parseLong(String value, SearchContext searchContext) {
            return IpFieldMapper.ipToLong(value);
        }

        @Override
        public double parseDouble(String value, SearchContext searchContext) {
            return parseLong(value, searchContext);
        }
    }

    static class Raw implements ValueParser {

        private Raw() {
        }

        @Override
        public long parseLong(String value, SearchContext searchContext) {
            return Long.parseLong(value);
        }

        @Override
        public double parseDouble(String value, SearchContext searchContext) {
            return Double.parseDouble(value);
        }
    }

    public static abstract class Number implements ValueParser {

        NumberFormat format;

        Number(NumberFormat format) {
           this.format = format;
        }

        public static class Pattern extends Number {

            private static final DecimalFormatSymbols SYMBOLS = new DecimalFormatSymbols(Locale.ROOT);

            public Pattern(String pattern) {
                super(new DecimalFormat(pattern, SYMBOLS));
            }

            @Override
            public long parseLong(String value, SearchContext searchContext) {
                try {
                    return format.parse(value).longValue();
                } catch (ParseException nfe) {
                    throw new AggregationExecutionException("Invalid number format [" + ((DecimalFormat) format).toPattern() + "]");
                }
            }

            @Override
            public double parseDouble(String value, SearchContext searchContext) {
                try {
                    return format.parse(value).doubleValue();
                } catch (ParseException nfe) {
                    throw new AggregationExecutionException("Invalid number format [" + ((DecimalFormat) format).toPattern() + "]");
                }
            }
        }
    }

    static class Boolean implements ValueParser {

        private Boolean() {
        }

        @Override
        public long parseLong(String value, SearchContext searchContext) {
            return java.lang.Boolean.parseBoolean(value) ? 1 : 0;
        }

        @Override
        public double parseDouble(String value, SearchContext searchContext) {
            return parseLong(value, searchContext);
        }
    }

}
