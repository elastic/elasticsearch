/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.util.joda;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * @author kimchy (Shay Banon)
 */
public class Joda {

    /**
     * Parses a joda based pattern, including some named ones (similar to the built in Joda ISO ones).
     */
    public static DateTimeFormatter forPattern(String input) {
        DateTimeFormatter formatter;
        if ("basicDate".equals(input)) {
            formatter = ISODateTimeFormat.basicDate();
        } else if ("basicDateTime".equals(input)) {
            formatter = ISODateTimeFormat.basicDateTime();
        } else if ("basicDateTimeNoMillis".equals(input)) {
            formatter = ISODateTimeFormat.basicDateTimeNoMillis();
        } else if ("basicOrdinalDate".equals(input)) {
            formatter = ISODateTimeFormat.basicOrdinalDate();
        } else if ("basicOrdinalDateTime".equals(input)) {
            formatter = ISODateTimeFormat.basicOrdinalDateTime();
        } else if ("basicOrdinalDateTimeNoMillis".equals(input)) {
            formatter = ISODateTimeFormat.basicOrdinalDateTimeNoMillis();
        } else if ("basicTime".equals(input)) {
            formatter = ISODateTimeFormat.basicTime();
        } else if ("basicTimeNoMillis".equals(input)) {
            formatter = ISODateTimeFormat.basicTimeNoMillis();
        } else if ("basicTTime".equals(input)) {
            formatter = ISODateTimeFormat.basicTTime();
        } else if ("basicTTimeNoMillis".equals(input)) {
            formatter = ISODateTimeFormat.basicTTimeNoMillis();
        } else if ("basicWeekDate".equals(input)) {
            formatter = ISODateTimeFormat.basicWeekDate();
        } else if ("basicWeekDateTime".equals(input)) {
            formatter = ISODateTimeFormat.basicWeekDateTime();
        } else if ("basicWeekDateTimeNoMillis".equals(input)) {
            formatter = ISODateTimeFormat.basicWeekDateTimeNoMillis();
        } else if ("date".equals(input)) {
            formatter = ISODateTimeFormat.date();
        } else if ("dateHour".equals(input)) {
            formatter = ISODateTimeFormat.dateHour();
        } else if ("dateHourMinute".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinute();
        } else if ("dateHourMinuteSecond".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecond();
        } else if ("dateHourMinuteSecondFraction".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecondFraction();
        } else if ("dateHourMinuteSecondMillis".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecondMillis();
        } else if ("dateOptionalTime".equals(input)) {
            formatter = ISODateTimeFormat.dateOptionalTimeParser();
        } else if ("dateTime".equals(input)) {
            formatter = ISODateTimeFormat.dateTime();
        } else if ("dateTimeNoMillis".equals(input)) {
            formatter = ISODateTimeFormat.dateTimeNoMillis();
        } else if ("hour".equals(input)) {
            formatter = ISODateTimeFormat.hour();
        } else if ("hourMinute".equals(input)) {
            formatter = ISODateTimeFormat.hourMinute();
        } else if ("hourMinuteSecond".equals(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecond();
        } else if ("hourMinuteSecondFraction".equals(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecondFraction();
        } else if ("hourMinuteSecondMillis".equals(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecondMillis();
        } else if ("ordinalDate".equals(input)) {
            formatter = ISODateTimeFormat.ordinalDate();
        } else if ("ordinalDateTime".equals(input)) {
            formatter = ISODateTimeFormat.ordinalDateTime();
        } else if ("ordinalDateTimeNoMillis".equals(input)) {
            formatter = ISODateTimeFormat.ordinalDateTimeNoMillis();
        } else if ("time".equals(input)) {
            formatter = ISODateTimeFormat.time();
        } else if ("tTime".equals(input)) {
            formatter = ISODateTimeFormat.tTime();
        } else if ("tTimeNoMillis".equals(input)) {
            formatter = ISODateTimeFormat.tTimeNoMillis();
        } else if ("weekDate".equals(input)) {
            formatter = ISODateTimeFormat.weekDate();
        } else if ("weekDateTime".equals(input)) {
            formatter = ISODateTimeFormat.weekDateTime();
        } else if ("weekyear".equals(input)) {
            formatter = ISODateTimeFormat.weekyear();
        } else if ("weekyearWeek".equals(input)) {
            formatter = ISODateTimeFormat.weekyearWeek();
        } else if ("year".equals(input)) {
            formatter = ISODateTimeFormat.year();
        } else if ("yearMonth".equals(input)) {
            formatter = ISODateTimeFormat.yearMonth();
        } else if ("yearMonthDay".equals(input)) {
            formatter = ISODateTimeFormat.yearMonthDay();
        } else {
            formatter = DateTimeFormat.forPattern(input);
        }
        return formatter;
    }
}
