/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

/**
 * Expressions API for date objects (.date)
 */
final class DateObject {
    // no instance
    private DateObject() {}

    // supported variables
    static final String CENTURY_OF_ERA_VARIABLE = "centuryOfEra";
    static final String DAY_OF_MONTH_VARIABLE = "dayOfMonth";
    static final String DAY_OF_WEEK_VARIABLE = "dayOfWeek";
    static final String DAY_OF_YEAR_VARIABLE = "dayOfYear";
    static final String ERA_VARIABLE = "era";
    static final String HOUR_OF_DAY_VARIABLE = "hourOfDay";
    static final String MILLIS_OF_DAY_VARIABLE = "millisOfDay";
    static final String MILLIS_OF_SECOND_VARIABLE = "millisOfSecond";
    static final String MINUTE_OF_DAY_VARIABLE = "minuteOfDay";
    static final String MINUTE_OF_HOUR_VARIABLE = "minuteOfHour";
    static final String MONTH_OF_YEAR_VARIABLE = "monthOfYear";
    static final String SECOND_OF_DAY_VARIABLE = "secondOfDay";
    static final String SECOND_OF_MINUTE_VARIABLE = "secondOfMinute";
    static final String WEEK_OF_WEEK_YEAR_VARIABLE = "weekOfWeekyear";
    static final String WEEK_YEAR_VARIABLE = "weekyear";
    static final String YEAR_VARIABLE = "year";
    static final String YEAR_OF_CENTURY_VARIABLE = "yearOfCentury";
    static final String YEAR_OF_ERA_VARIABLE = "yearOfEra";

    // supported methods
    static final String GETCENTURY_OF_ERA_METHOD = "getCenturyOfEra";
    static final String GETDAY_OF_MONTH_METHOD = "getDayOfMonth";
    static final String GETDAY_OF_WEEK_METHOD = "getDayOfWeek";
    static final String GETDAY_OF_YEAR_METHOD = "getDayOfYear";
    static final String GETERA_METHOD = "getEra";
    static final String GETHOUR_OF_DAY_METHOD = "getHourOfDay";
    static final String GETMILLIS_OF_DAY_METHOD = "getMillisOfDay";
    static final String GETMILLIS_OF_SECOND_METHOD = "getMillisOfSecond";
    static final String GETMINUTE_OF_DAY_METHOD = "getMinuteOfDay";
    static final String GETMINUTE_OF_HOUR_METHOD = "getMinuteOfHour";
    static final String GETMONTH_OF_YEAR_METHOD = "getMonthOfYear";
    static final String GETSECOND_OF_DAY_METHOD = "getSecondOfDay";
    static final String GETSECOND_OF_MINUTE_METHOD = "getSecondOfMinute";
    static final String GETWEEK_OF_WEEK_YEAR_METHOD = "getWeekOfWeekyear";
    static final String GETWEEK_YEAR_METHOD = "getWeekyear";
    static final String GETYEAR_METHOD = "getYear";
    static final String GETYEAR_OF_CENTURY_METHOD = "getYearOfCentury";
    static final String GETYEAR_OF_ERA_METHOD = "getYearOfEra";

    static DoubleValuesSource getVariable(IndexFieldData<?> fieldData, String fieldName, String variable) {
        return switch (variable) {
            case CENTURY_OF_ERA_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.get(ChronoField.YEAR_OF_ERA) / 100
            );
            case DAY_OF_MONTH_VARIABLE -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getDayOfMonth);
            case DAY_OF_WEEK_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.getDayOfWeek().getValue()
            );
            case DAY_OF_YEAR_VARIABLE -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getDayOfYear);
            case ERA_VARIABLE -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, zdt -> zdt.get(ChronoField.ERA));
            case HOUR_OF_DAY_VARIABLE -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getHour);
            case MILLIS_OF_DAY_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.get(ChronoField.MILLI_OF_DAY)
            );
            case MILLIS_OF_SECOND_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.get(ChronoField.MILLI_OF_SECOND)
            );
            case MINUTE_OF_DAY_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.get(ChronoField.MINUTE_OF_DAY)
            );
            case MINUTE_OF_HOUR_VARIABLE -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getMinute);
            case MONTH_OF_YEAR_VARIABLE -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getMonthValue);
            case SECOND_OF_DAY_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.get(ChronoField.SECOND_OF_DAY)
            );
            case SECOND_OF_MINUTE_VARIABLE -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getSecond);
            case WEEK_OF_WEEK_YEAR_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.get(DateFormatters.WEEK_FIELDS_ROOT.weekOfWeekBasedYear())
            );
            case WEEK_YEAR_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.get(DateFormatters.WEEK_FIELDS_ROOT.weekBasedYear())
            );
            case YEAR_VARIABLE -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, variable, ZonedDateTime::getYear);
            case YEAR_OF_CENTURY_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.get(ChronoField.YEAR_OF_ERA) % 100
            );
            case YEAR_OF_ERA_VARIABLE -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                variable,
                zdt -> zdt.get(ChronoField.YEAR_OF_ERA)
            );
            default -> throw new IllegalArgumentException(
                "Member variable [" + variable + "] does not exist for date object on field [" + fieldName + "]."
            );
        };
    }

    static DoubleValuesSource getMethod(IndexFieldData<?> fieldData, String fieldName, String method) {
        return switch (method) {
            case GETCENTURY_OF_ERA_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.get(ChronoField.YEAR_OF_ERA) / 100
            );
            case GETDAY_OF_MONTH_METHOD -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getDayOfMonth);
            case GETDAY_OF_WEEK_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.getDayOfWeek().getValue()
            );
            case GETDAY_OF_YEAR_METHOD -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getDayOfYear);
            case GETERA_METHOD -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, zdt -> zdt.get(ChronoField.ERA));
            case GETHOUR_OF_DAY_METHOD -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getHour);
            case GETMILLIS_OF_DAY_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.get(ChronoField.MILLI_OF_DAY)
            );
            case GETMILLIS_OF_SECOND_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.get(ChronoField.MILLI_OF_SECOND)
            );
            case GETMINUTE_OF_DAY_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.get(ChronoField.MINUTE_OF_DAY)
            );
            case GETMINUTE_OF_HOUR_METHOD -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getMinute);
            case GETMONTH_OF_YEAR_METHOD -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getMonthValue);
            case GETSECOND_OF_DAY_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.get(ChronoField.SECOND_OF_DAY)
            );
            case GETSECOND_OF_MINUTE_METHOD -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getSecond);
            case GETWEEK_OF_WEEK_YEAR_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.get(DateFormatters.WEEK_FIELDS_ROOT.weekOfWeekBasedYear())
            );
            case GETWEEK_YEAR_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.get(DateFormatters.WEEK_FIELDS_ROOT.weekBasedYear())
            );
            case GETYEAR_METHOD -> new DateObjectValueSource(fieldData, MultiValueMode.MIN, method, ZonedDateTime::getYear);
            case GETYEAR_OF_CENTURY_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.get(ChronoField.YEAR_OF_ERA) % 100
            );
            case GETYEAR_OF_ERA_METHOD -> new DateObjectValueSource(
                fieldData,
                MultiValueMode.MIN,
                method,
                zdt -> zdt.get(ChronoField.YEAR_OF_ERA)
            );
            default -> throw new IllegalArgumentException(
                "Member method [" + method + "] does not exist for date object on field [" + fieldName + "]."
            );
        };
    }
}
