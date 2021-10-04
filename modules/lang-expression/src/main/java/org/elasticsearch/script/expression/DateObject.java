package org.elasticsearch.script.expression;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.time.ZonedDateTime;

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
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getCenturyOfEra);
            case DAY_OF_MONTH_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getDayOfMonth);
            case DAY_OF_WEEK_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getDayOfWeek);
            case DAY_OF_YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getDayOfYear);
            case ERA_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getEra);
            case HOUR_OF_DAY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getHourOfDay);
            case MILLIS_OF_DAY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getMillisOfDay);
            case MILLIS_OF_SECOND_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getMillisOfSecond);
            case MINUTE_OF_DAY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getMinuteOfDay);
            case MINUTE_OF_HOUR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getMinuteOfHour);
            case MONTH_OF_YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getMonthOfYear);
            case SECOND_OF_DAY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getSecondOfDay);
            case SECOND_OF_MINUTE_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getSecondOfMinute);
            case WEEK_OF_WEEK_YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getWeekOfWeekyear);
            case WEEK_YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getWeekyear);
            case YEAR_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getYear);
            case YEAR_OF_CENTURY_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getYearOfCentury);
            case YEAR_OF_ERA_VARIABLE:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getYearOfEra);
            default:
                throw new IllegalArgumentException("Member variable [" + variable +
                                                   "] does not exist for date object on field [" + fieldName + "].");
        }
    }

    static DoubleValuesSource getMethod(IndexFieldData<?> fieldData, String fieldName, String method) {
        switch (method) {
            case GETCENTURY_OF_ERA_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getCenturyOfEra);
            case GETDAY_OF_MONTH_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getDayOfMonth);
            case GETDAY_OF_WEEK_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getDayOfWeek);
            case GETDAY_OF_YEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getDayOfYear);
            case GETERA_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getEra);
            case GETHOUR_OF_DAY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getHourOfDay);
            case GETMILLIS_OF_DAY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getMillisOfDay);
            case GETMILLIS_OF_SECOND_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getMillisOfSecond);
            case GETMINUTE_OF_DAY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getMinuteOfDay);
            case GETMINUTE_OF_HOUR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getMinuteOfHour);
            case GETMONTH_OF_YEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getMonthOfYear);
            case GETSECOND_OF_DAY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getSecondOfDay);
            case GETSECOND_OF_MINUTE_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getSecondOfMinute);
            case GETWEEK_OF_WEEK_YEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getWeekOfWeekyear);
            case GETWEEK_YEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getWeekyear);
            case GETYEAR_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getYear);
            case GETYEAR_OF_CENTURY_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getYearOfCentury);
            case GETYEAR_OF_ERA_METHOD:
                return new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getYearOfEra);
            default:
                throw new IllegalArgumentException("Member method [" + method +
                                                   "] does not exist for date object on field [" + fieldName + "].");
        }
    }
}
