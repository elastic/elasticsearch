package org.elasticsearch.script.expression;

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

import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.MultiValueMode;
import org.joda.time.ReadableDateTime;

/**
 * Expressions API for date objects (.date)
 */
final class DateObject {
    // no instance
    private DateObject() {}

    // supported variables
    static final String CENTURY_OF_ERA_VARIABLE       = "centuryOfEra";
    static final String DAY_OF_MONTH_VARIABLE         = "dayOfMonth";
    static final String DAY_OF_WEEK_VARIABLE          = "dayOfWeek";
    static final String DAY_OF_YEAR_VARIABLE          = "dayOfYear";
    static final String ERA_VARIABLE                  = "era";
    static final String HOUR_OF_DAY_VARIABLE          = "hourOfDay";
    static final String MILLIS_OF_DAY_VARIABLE        = "millisOfDay";
    static final String MILLIS_OF_SECOND_VARIABLE     = "millisOfSecond";
    static final String MINUTE_OF_DAY_VARIABLE        = "minuteOfDay";
    static final String MINUTE_OF_HOUR_VARIABLE       = "minuteOfHour";
    static final String MONTH_OF_YEAR_VARIABLE        = "monthOfYear";
    static final String SECOND_OF_DAY_VARIABLE        = "secondOfDay";
    static final String SECOND_OF_MINUTE_VARIABLE     = "secondOfMinute";
    static final String WEEK_OF_WEEK_YEAR_VARIABLE    = "weekOfWeekyear";
    static final String WEEK_YEAR_VARIABLE            = "weekyear";
    static final String YEAR_VARIABLE                 = "year";
    static final String YEAR_OF_CENTURY_VARIABLE      = "yearOfCentury";
    static final String YEAR_OF_ERA_VARIABLE          = "yearOfEra";

    // supported methods
    static final String GETCENTURY_OF_ERA_METHOD      = "getCenturyOfEra";
    static final String GETDAY_OF_MONTH_METHOD        = "getDayOfMonth";
    static final String GETDAY_OF_WEEK_METHOD         = "getDayOfWeek";
    static final String GETDAY_OF_YEAR_METHOD         = "getDayOfYear";
    static final String GETERA_METHOD                 = "getEra";
    static final String GETHOUR_OF_DAY_METHOD         = "getHourOfDay";
    static final String GETMILLIS_OF_DAY_METHOD       = "getMillisOfDay";
    static final String GETMILLIS_OF_SECOND_METHOD    = "getMillisOfSecond";
    static final String GETMINUTE_OF_DAY_METHOD       = "getMinuteOfDay";
    static final String GETMINUTE_OF_HOUR_METHOD      = "getMinuteOfHour";
    static final String GETMONTH_OF_YEAR_METHOD       = "getMonthOfYear";
    static final String GETSECOND_OF_DAY_METHOD       = "getSecondOfDay";
    static final String GETSECOND_OF_MINUTE_METHOD    = "getSecondOfMinute";
    static final String GETWEEK_OF_WEEK_YEAR_METHOD   = "getWeekOfWeekyear";
    static final String GETWEEK_YEAR_METHOD           = "getWeekyear";
    static final String GETYEAR_METHOD                = "getYear";
    static final String GETYEAR_OF_CENTURY_METHOD     = "getYearOfCentury";
    static final String GETYEAR_OF_ERA_METHOD         = "getYearOfEra";

    static DoubleValuesSource getVariable(IndexFieldData<?> fieldData, String fieldName, String variable) {
        switch (variable) {
            case CENTURY_OF_ERA_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getCenturyOfEra);
            case DAY_OF_MONTH_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getDayOfMonth);
            case DAY_OF_WEEK_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getDayOfWeek);
            case DAY_OF_YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getDayOfYear);
            case ERA_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getEra);
            case HOUR_OF_DAY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getHourOfDay);
            case MILLIS_OF_DAY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getMillisOfDay);
            case MILLIS_OF_SECOND_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getMillisOfSecond);
            case MINUTE_OF_DAY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getMinuteOfDay);
            case MINUTE_OF_HOUR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getMinuteOfHour);
            case MONTH_OF_YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getMonthOfYear);
            case SECOND_OF_DAY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getSecondOfDay);
            case SECOND_OF_MINUTE_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getSecondOfMinute);
            case WEEK_OF_WEEK_YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getWeekOfWeekyear);
            case WEEK_YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getWeekyear);
            case YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getYear);
            case YEAR_OF_CENTURY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getYearOfCentury);
            case YEAR_OF_ERA_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ReadableDateTime::getYearOfEra);
            default:
                throw new IllegalArgumentException("Member variable [" + variable +
                                                   "] does not exist for date object on field [" + fieldName + "].");
        }
    }

    static DoubleValuesSource getMethod(IndexFieldData<?> fieldData, String fieldName, String method) {
        switch (method) {
            case GETCENTURY_OF_ERA_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getCenturyOfEra);
            case GETDAY_OF_MONTH_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getDayOfMonth);
            case GETDAY_OF_WEEK_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getDayOfWeek);
            case GETDAY_OF_YEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getDayOfYear);
            case GETERA_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getEra);
            case GETHOUR_OF_DAY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getHourOfDay);
            case GETMILLIS_OF_DAY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getMillisOfDay);
            case GETMILLIS_OF_SECOND_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getMillisOfSecond);
            case GETMINUTE_OF_DAY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getMinuteOfDay);
            case GETMINUTE_OF_HOUR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getMinuteOfHour);
            case GETMONTH_OF_YEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getMonthOfYear);
            case GETSECOND_OF_DAY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getSecondOfDay);
            case GETSECOND_OF_MINUTE_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getSecondOfMinute);
            case GETWEEK_OF_WEEK_YEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getWeekOfWeekyear);
            case GETWEEK_YEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getWeekyear);
            case GETYEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getYear);
            case GETYEAR_OF_CENTURY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getYearOfCentury);
            case GETYEAR_OF_ERA_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ReadableDateTime::getYearOfEra);
            default:
                throw new IllegalArgumentException("Member method [" + method +
                                                   "] does not exist for date object on field [" + fieldName + "].");
        }
    }
}
