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

import org.elasticsearch.index.mapper.core.DateFieldMapper;

/**
 *
 */
public class ValueFormat {

    public static final ValueFormat RAW = new ValueFormat(ValueFormatter.RAW, ValueParser.RAW);
    public static final ValueFormat IPv4 = new ValueFormat(ValueFormatter.IPv4, ValueParser.IPv4);
    public static final ValueFormat BOOLEAN = new ValueFormat(ValueFormatter.BOOLEAN, ValueParser.BOOLEAN);

    private final ValueFormatter formatter;
    private final ValueParser parser;

    public ValueFormat(ValueFormatter formatter, ValueParser parser) {
        assert formatter != null && parser != null;
        this.formatter = formatter;
        this.parser = parser;
    }

    public ValueFormatter formatter() {
        return formatter;
    }

    public ValueParser parser() {
        return parser;
    }

    public abstract static class Patternable<VF extends Patternable<VF>> extends ValueFormat {

        private final String pattern;

        public Patternable(String pattern, ValueFormatter formatter, ValueParser parser) {
            super(formatter, parser);
            this.pattern = pattern;
        }

        public String pattern() {
            return pattern;
        }

        public abstract VF create(String pattern);
    }

    public static class DateTime extends Patternable<DateTime> {

        public static final DateTime DEFAULT = new DateTime(DateFieldMapper.Defaults.DATE_TIME_FORMATTER.format(), ValueFormatter.DateTime.DEFAULT, ValueParser.DateMath.DEFAULT);

        public static DateTime format(String format) {
            return new DateTime(format, new ValueFormatter.DateTime(format), new ValueParser.DateMath(format, DateFieldMapper.Defaults.TIME_UNIT));
        }

        public static DateTime mapper(DateFieldMapper mapper) {
            return new DateTime(mapper.dateTimeFormatter().format(), ValueFormatter.DateTime.mapper(mapper), ValueParser.DateMath.mapper(mapper));
        }

        public DateTime(String pattern, ValueFormatter formatter, ValueParser parser) {
            super(pattern, formatter, parser);
        }

        @Override
        public DateTime create(String pattern) {
            return format(pattern);
        }
    }

    public static class Number extends Patternable<Number> {

        public static Number format(String format) {
            return new Number(format, new ValueFormatter.Number.Pattern(format), new ValueParser.Number.Pattern(format));
        }

        public Number(String pattern, ValueFormatter formatter, ValueParser parser) {
            super(pattern, formatter, parser);
        }

        @Override
        public Number create(String pattern) {
            return format(pattern);
        }
    }

}
