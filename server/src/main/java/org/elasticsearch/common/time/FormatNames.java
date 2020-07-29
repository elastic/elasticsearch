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

public enum FormatNames {
    ISO8601("iso8601"),
    BASIC_DATE("basic_date"),
    BASIC_DATE_TIME("basic_date_time"),
    BASIC_DATE_TIME_NO_MILLIS("basic_date_time_no_millis"),
    BASIC_ORDINAL_DATE("basic_ordinal_date"),
    BASIC_ORDINAL_DATE_TIME("basic_ordinal_date_time"),
    BASIC_ORDINAL_DATE_TIME_NO_MILLIS("basic_ordinal_date_time_no_millis"),
    BASIC_TIME("basic_time"),
    BASIC_TIME_NO_MILLIS("basic_time_no_millis"),
    BASIC_T_TIME("basic_t_time"),
    BASIC_T_TIME_NO_MILLIS("basic_t_time_no_millis"),
    BASIC_WEEK_DATE("basic_week_date"),
    BASIC_WEEK_DATE_TIME("basic_week_date_time"),
    BASIC_WEEK_DATE_TIME_NO_MILLIS("basic_week_date_time_no_millis"),
    DATE("date"),
    DATE_HOUR("date_hour"),
    DATE_HOUR_MINUTE("date_hour_minute"),
    DATE_HOUR_MINUTE_SECOND("date_hour_minute_second"),
    DATE_HOUR_MINUTE_SECOND_FRACTION("date_hour_minute_second_fraction"),
    DATE_HOUR_MINUTE_SECOND_MILLIS("date_hour_minute_second_millis"),
    DATE_OPTIONAL_TIME("date_optional_time"),
    DATE_TIME("date_time"),
    DATE_TIME_NO_MILLIS("date_time_no_millis"),
    HOUR("hour"),
    HOUR_MINUTE("hour_minute"),
    HOUR_MINUTE_SECOND("hour_minute_second"),
    HOUR_MINUTE_SECOND_FRACTION("hour_minute_second_fraction"),
    HOUR_MINUTE_SECOND_MILLIS("hour_minute_second_millis"),
    ORDINAL_DATE("ordinal_date"),
    ORDINAL_DATE_TIME("ordinal_date_time"),
    ORDINAL_DATE_TIME_NO_MILLIS("ordinal_date_time_no_millis"),
    TIME("time"),
    TIME_NO_MILLIS("time_no_millis"),
    T_TIME("t_time"),
    T_TIME_NO_MILLIS("t_time_no_millis"),
    WEEK_DATE("week_date"),
    WEEK_DATE_TIME("week_date_time"),
    WEEK_DATE_TIME_NO_MILLIS("week_date_time_no_millis"),
    WEEK_YEAR("week_year"),
    WEEK_YEAR_WEEK("weekyear_week"),
    WEEKYEAR_WEEK_DAY("weekyear_week_day"),
    YEAR("year"),
    YEAR_MONTH("year_month"),
    YEAR_MONTH_DAY("year_month_day"),
    EPOCH_SECOND("epoch_second"),
    EPOCH_MILLIS("epoch_millis"),
    // strict date formats here, must be at least 4 digits for year and two for months and two for day"
    STRICT_BASIC_WEEK_DATE("strict_basic_week_date"),
    STRICT_BASIC_WEEK_DATE_TIME("strict_basic_week_date_time"),
    STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS("strict_basic_week_date_time_no_millis"),
    STRICT_DATE("strict_date"),
    STRICT_DATE_HOUR("strict_date_hour"),
    STRICT_DATE_HOUR_MINUTE("strict_date_hour_minute"),
    STRICT_DATE_HOUR_MINUTE_SECOND("strict_date_hour_minute_second"),
    STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION("strict_date_hour_minute_second_fraction"),
    STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS("strict_date_hour_minute_second_millis"),
    STRICT_DATE_OPTIONAL_TIME("strict_date_optional_time"),
    STRICT_DATE_OPTIONAL_TIME_NANOS("strict_date_optional_time_nanos"),
    STRICT_DATE_TIME("strict_date_time"),
    STRICT_DATE_TIME_NO_MILLIS("strict_date_time_no_millis"),
    STRICT_HOUR("strict_hour"),
    STRICT_HOUR_MINUTE("strict_hour_minute"),
    STRICT_HOUR_MINUTE_SECOND("strict_hour_minute_second"),
    STRICT_HOUR_MINUTE_SECOND_FRACTION("strict_hour_minute_second_fraction"),
    STRICT_HOUR_MINUTE_SECOND_MILLIS("strict_hour_minute_second_millis"),
    STRICT_ORDINAL_DATE("strict_ordinal_date"),
    STRICT_ORDINAL_DATE_TIME("strict_ordinal_date_time"),
    STRICT_ORDINAL_DATE_TIME_NO_MILLIS("strict_ordinal_date_time_no_millis"),
    STRICT_TIME("strict_time"),
    STRICT_TIME_NO_MILLIS("strict_time_no_millis"),
    STRICT_T_TIME("strict_t_time"),
    STRICT_T_TIME_NO_MILLIS("strict_t_time_no_millis"),
    STRICT_WEEK_DATE("strict_week_date"),
    STRICT_WEEK_DATE_TIME("strict_week_date_time"),
    STRICT_WEEK_DATE_TIME_NO_MILLIS("strict_week_date_time_no_millis"),
    STRICT_WEEKYEAR("strict_weekyear"),
    STRICT_WEEKYEAR_WEEK("strict_weekyear_week"),
    STRICT_WEEKYEAR_WEEK_DAY("strict_weekyear_week_day"),
    STRICT_YEAR("strict_year"),
    STRICT_YEAR_MONTH("strict_year_month"),
    STRICT_YEAR_MONTH_DAY("strict_year_month_day");

    private final String snakeCaseName;

    FormatNames(String snakeCaseName) {
        this.snakeCaseName = snakeCaseName;
    }

    public boolean matches(String format) {
        return format.equals(snakeCaseName);
    }
}
