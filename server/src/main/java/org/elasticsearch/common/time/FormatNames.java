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

package org.elasticsearch.common.time;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum FormatNames {
    ISO8601("iso8601", "iso8601"),
    BASIC_DATE("basicDate", "basic_date"),
    BASIC_DATE_TIME("basicDateTime", "basic_date_time"),
    BASIC_DATE_TIME_NO_MILLIS("basicDateTimeNoMillis", "basic_date_time_no_millis"),
    BASIC_ORDINAL_DATE("basicOrdinalDate", "basic_ordinal_date"),
    BASIC_ORDINAL_DATE_TIME("basicOrdinalDateTime", "basic_ordinal_date_time"),
    BASIC_ORDINAL_DATE_TIME_NO_MILLIS("basicOrdinalDateTimeNoMillis", "basic_ordinal_date_time_no_millis"),
    BASIC_TIME("basicTime", "basic_time"),
    BASIC_TIME_NO_MILLIS("basicTimeNoMillis", "basic_time_no_millis"),
    BASIC_T_TIME("basicTTime", "basic_t_time"),
    BASIC_T_TIME_NO_MILLIS("basicTTimeNoMillis", "basic_t_time_no_millis"),
    BASIC_WEEK_DATE("basicWeekDate", "basic_week_date"),
    BASIC_WEEK_DATE_TIME("basicWeekDateTime", "basic_week_date_time"),
    BASIC_WEEK_DATE_TIME_NO_MILLIS("basicWeekDateTimeNoMillis", "basic_week_date_time_no_millis"),
    DATE("date", "date"),
    DATE_HOUR("dateHour", "date_hour"),
    DATE_HOUR_MINUTE("dateHourMinute", "date_hour_minute"),
    DATE_HOUR_MINUTE_SECOND("dateHourMinuteSecond", "date_hour_minute_second"),
    DATE_HOUR_MINUTE_SECOND_FRACTION("dateHourMinuteSecondFraction", "date_hour_minute_second_fraction"),
    DATE_HOUR_MINUTE_SECOND_MILLIS("dateHourMinuteSecondMillis", "date_hour_minute_second_millis"),
    DATE_OPTIONAL_TIME("dateOptionalTime", "date_optional_time"),
    DATE_TIME("dateTime", "date_time"),
    DATE_TIME_NO_MILLIS("dateTimeNoMillis", "date_time_no_millis"),
    HOUR("hour", "hour"),
    HOUR_MINUTE("hourMinute", "hour_minute"),
    HOUR_MINUTE_SECOND("hourMinuteSecond", "hour_minute_second"),
    HOUR_MINUTE_SECOND_FRACTION("hourMinuteSecondFraction", "hour_minute_second_fraction"),
    HOUR_MINUTE_SECOND_MILLIS("hourMinuteSecondMillis", "hour_minute_second_millis"),
    ORDINAL_DATE("ordinalDate", "ordinal_date"),
    ORDINAL_DATE_TIME("ordinalDateTime", "ordinal_date_time"),
    ORDINAL_DATE_TIME_NO_MILLIS("ordinalDateTimeNoMillis", "ordinal_date_time_no_millis"),
    TIME("time", "time"),
    TIME_NO_MILLIS("timeNoMillis", "time_no_millis"),
    T_TIME("tTime", "t_time"),
    T_TIME_NO_MILLIS("tTimeNoMillis", "t_time_no_millis"),
    WEEK_DATE("weekDate", "week_date"),
    WEEK_DATE_TIME("weekDateTime", "week_date_time"),
    WEEK_DATE_TIME_NO_MILLIS("weekDateTimeNoMillis", "week_date_time_no_millis"),
    WEEK_YEAR("weekyear", "week_year"),
    WEEK_YEAR_WEEK("weekyearWeek", "weekyear_week"),
    WEEKYEAR_WEEK_DAY("weekyearWeekDay", "weekyear_week_day"),
    YEAR("year", "year"),
    YEAR_MONTH("yearMonth", "year_month"),
    YEAR_MONTH_DAY("yearMonthDay", "year_month_day"),
    EPOCH_SECOND("epoch_second", "epoch_second"),
    EPOCH_MILLIS("epoch_millis", "epoch_millis"),
    // strict date formats here, must be at least 4 digits for year and two for months and two for day"
    STRICT_BASIC_WEEK_DATE("strictBasicWeekDate", "strict_basic_week_date"),
    STRICT_BASIC_WEEK_DATE_TIME("strictBasicWeekDateTime", "strict_basic_week_date_time"),
    STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS("strictBasicWeekDateTimeNoMillis", "strict_basic_week_date_time_no_millis"),
    STRICT_DATE("strictDate", "strict_date"),
    STRICT_DATE_HOUR("strictDateHour", "strict_date_hour"),
    STRICT_DATE_HOUR_MINUTE("strictDateHourMinute", "strict_date_hour_minute"),
    STRICT_DATE_HOUR_MINUTE_SECOND("strictDateHourMinuteSecond", "strict_date_hour_minute_second"),
    STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION("strictDateHourMinuteSecondFraction", "strict_date_hour_minute_second_fraction"),
    STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS("strictDateHourMinuteSecondMillis", "strict_date_hour_minute_second_millis"),
    STRICT_DATE_OPTIONAL_TIME("strictDateOptionalTime", "strict_date_optional_time"),
    STRICT_DATE_OPTIONAL_TIME_NANOS("strictDateOptionalTimeNanos", "strict_date_optional_time_nanos"),
    STRICT_DATE_TIME("strictDateTime", "strict_date_time"),
    STRICT_DATE_TIME_NO_MILLIS("strictDateTimeNoMillis", "strict_date_time_no_millis"),
    STRICT_HOUR("strictHour", "strict_hour"),
    STRICT_HOUR_MINUTE("strictHourMinute", "strict_hour_minute"),
    STRICT_HOUR_MINUTE_SECOND("strictHourMinuteSecond", "strict_hour_minute_second"),
    STRICT_HOUR_MINUTE_SECOND_FRACTION("strictHourMinuteSecondFraction", "strict_hour_minute_second_fraction"),
    STRICT_HOUR_MINUTE_SECOND_MILLIS("strictHourMinuteSecondMillis", "strict_hour_minute_second_millis"),
    STRICT_ORDINAL_DATE("strictOrdinalDate", "strict_ordinal_date"),
    STRICT_ORDINAL_DATE_TIME("strictOrdinalDateTime", "strict_ordinal_date_time"),
    STRICT_ORDINAL_DATE_TIME_NO_MILLIS("strictOrdinalDateTimeNoMillis", "strict_ordinal_date_time_no_millis"),
    STRICT_TIME("strictTime", "strict_time"),
    STRICT_TIME_NO_MILLIS("strictTimeNoMillis", "strict_time_no_millis"),
    STRICT_T_TIME("strictTTime", "strict_t_time"),
    STRICT_T_TIME_NO_MILLIS("strictTTimeNoMillis", "strict_t_time_no_millis"),
    STRICT_WEEK_DATE("strictWeekDate", "strict_week_date"),
    STRICT_WEEK_DATE_TIME("strictWeekDateTime", "strict_week_date_time"),
    STRICT_WEEK_DATE_TIME_NO_MILLIS("strictWeekDateTimeNoMillis", "strict_week_date_time_no_millis"),
    STRICT_WEEKYEAR("strictWeekyear", "strict_weekyear"),
    STRICT_WEEKYEAR_WEEK("strictWeekyearWeek", "strict_weekyear_week"),
    STRICT_WEEKYEAR_WEEK_DAY("strictWeekyearWeekDay", "strict_weekyear_week_day"),
    STRICT_YEAR("strictYear", "strict_year"),
    STRICT_YEAR_MONTH("strictYearMonth", "strict_year_month"),
    STRICT_YEAR_MONTH_DAY("strictYearMonthDay", "strict_year_month_day");

    private static final Set<String> ALL_NAMES = Arrays.stream(values())
                                                       .flatMap(n -> Stream.of(n.snakeCaseName, n.camelCaseName))
                                                       .collect(Collectors.toSet());
    private final String camelCaseName;
    private final String snakeCaseName;

    FormatNames(String camelCaseName, String snakeCaseName) {
        this.camelCaseName = camelCaseName;
        this.snakeCaseName = snakeCaseName;
    }

    public static boolean exist(String format) {
        return ALL_NAMES.contains(format);
    }

    public boolean matches(String format) {
        return format.equals(camelCaseName) || format.equals(snakeCaseName);
    }
}
