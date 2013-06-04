/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.joda;

import java.util.Locale;

import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;

/**
 *
 */
public class Joda {

    public static FormatDateTimeFormatter forPattern(String input) {
        return forPattern(input, Locale.ROOT);
    }

    /**
     * Parses a joda based pattern, including some named ones (similar to the built in Joda ISO ones).
     */
    public static FormatDateTimeFormatter forPattern(String input, Locale locale) {
      return new PatternMatcher().getDateTimeFormatter(input, locale);
    }

    public static final DurationFieldType Quarters = new DurationFieldType("quarters") {
        private static final long serialVersionUID = -8167713675442491871L;

        public DurationField getField(Chronology chronology) {
            return new ScaledDurationField(chronology.months(), Quarters, 3);
        }
    };

    public static final DateTimeFieldType QuarterOfYear = new DateTimeFieldType("quarterOfYear") {
        private static final long serialVersionUID = -5677872459807379123L;

        public DurationFieldType getDurationType() {
            return Quarters;
        }

        public DurationFieldType getRangeDurationType() {
            return DurationFieldType.years();
        }

        public DateTimeField getField(Chronology chronology) {
            return new OffsetDateTimeField(new DividedDateTimeField(new OffsetDateTimeField(chronology.monthOfYear(), -1), QuarterOfYear, 3), 1);
        }
    };
}
