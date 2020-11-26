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
package org.elasticsearch.common.rounding;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.joda.Joda;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.util.function.Function;

public enum DateTimeUnit {

    WEEK_OF_WEEKYEAR(   (byte) 1, tz -> ISOChronology.getInstance(tz).weekOfWeekyear()),
    YEAR_OF_CENTURY(    (byte) 2, tz -> ISOChronology.getInstance(tz).yearOfCentury()),
    QUARTER(            (byte) 3, tz -> Joda.QuarterOfYear.getField(ISOChronology.getInstance(tz))),
    MONTH_OF_YEAR(      (byte) 4, tz -> ISOChronology.getInstance(tz).monthOfYear()),
    DAY_OF_MONTH(       (byte) 5, tz -> ISOChronology.getInstance(tz).dayOfMonth()),
    HOUR_OF_DAY(        (byte) 6, tz -> ISOChronology.getInstance(tz).hourOfDay()),
    MINUTES_OF_HOUR(    (byte) 7, tz -> ISOChronology.getInstance(tz).minuteOfHour()),
    SECOND_OF_MINUTE(   (byte) 8, tz -> ISOChronology.getInstance(tz).secondOfMinute());

    private final byte id;
    private final Function<DateTimeZone, DateTimeField> fieldFunction;

    DateTimeUnit(byte id, Function<DateTimeZone, DateTimeField> fieldFunction) {
        this.id = id;
        this.fieldFunction = fieldFunction;
    }

    public byte id() {
        return id;
    }

    /**
     * @return the {@link DateTimeField} for the provided {@link DateTimeZone} for this time unit
     */
    public DateTimeField field(DateTimeZone tz) {
        return fieldFunction.apply(tz);
    }

    public static DateTimeUnit resolve(byte id) {
        switch (id) {
            case 1: return WEEK_OF_WEEKYEAR;
            case 2: return YEAR_OF_CENTURY;
            case 3: return QUARTER;
            case 4: return MONTH_OF_YEAR;
            case 5: return DAY_OF_MONTH;
            case 6: return HOUR_OF_DAY;
            case 7: return MINUTES_OF_HOUR;
            case 8: return SECOND_OF_MINUTE;
            default: throw new ElasticsearchException("Unknown date time unit id [" + id + "]");
        }
    }
}