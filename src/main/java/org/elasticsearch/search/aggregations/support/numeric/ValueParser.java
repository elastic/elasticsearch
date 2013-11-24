/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.support.numeric;

import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public interface ValueParser {

    static final ValueParser IPv4 = new ValueParser.IPv4();

    long parseLong(String value, SearchContext searchContext);

    double parseDouble(String value, SearchContext searchContext);


    /**
     * Knows how to parse datatime values based on date/time format
     */
    static class DateTime implements ValueParser {

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

        public static final DateMath DEFAULT = new ValueParser.DateMath(new DateMathParser(DateFieldMapper.Defaults.DATE_TIME_FORMATTER, DateFieldMapper.Defaults.TIME_UNIT));

        private DateMathParser parser;

        public DateMath(String format, TimeUnit timeUnit) {
            this(new DateMathParser(Joda.forPattern(format), timeUnit));
        }

        public DateMath(DateMathParser parser) {
            this.parser = parser;
        }

        @Override
        public long parseLong(String value, SearchContext searchContext) {
            return parser.parse(value, searchContext.nowInMillis());
        }

        @Override
        public double parseDouble(String value, SearchContext searchContext) {
            return parseLong(value, searchContext);
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
}
