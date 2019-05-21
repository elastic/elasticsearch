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
    ISO8601("iso8601", "iso8601"),
    BASICDATE("basicDate", "basic_date"),
    BASICDATETIME("basicDateTime", "basic_date_time"),
    BASICDATETIMENOMILLIS("basicDateTimeNoMillis", "basic_date_time_no_millis"),
    BASICORDINALDATE("basicOrdinalDate", "basic_ordinal_date"),
    BASICORDINALDATETIME("basicOrdinalDateTime", "basic_ordinal_date_time"),
    BASICORDINALDATETIMENOMILLIS("basicOrdinalDateTimeNoMillis", "basic_ordinal_date_time_no_millis"),
    BASICTIME("basicTime", "basic_time"),
    BASICTIMENOMILLIS("basicTimeNoMillis", "basic_time_no_millis"),
    BASICTTIME("basicTTime", "basic_t_time"),
    BASICTTIMENOMILLIS("basicTTimeNoMillis", "basic_t_time_no_millis"),
    BASICWEEKDATE("basicWeekDate", "basic_week_date"),
    BASICWEEKDATETIME("basicWeekDateTime", "basic_week_date_time"),
    BASICWEEKDATETIMENOMILLIS("basicWeekDateTimeNoMillis", "basic_week_date_time_no_millis"),
    DATE("date", "date"),
    DATEHOUR("dateHour", "date_hour"),
    DATEHOURMINUTE("dateHourMinute", "date_hour_minute"),
    DATEHOURMINUTESECOND("dateHourMinuteSecond", "date_hour_minute_second"),
    DATEHOURMINUTESECONDFRACTION("dateHourMinuteSecondFraction", "date_hour_minute_second_fraction"),
    DATEHOURMINUTESECONDMILLIS("dateHourMinuteSecondMillis", "date_hour_minute_second_millis"),
    DATEOPTIONALTIME("dateOptionalTime", "date_optional_time"),
    DATETIME("dateTime", "date_time"),
    DATETIMENOMILLIS("dateTimeNoMillis", "date_time_no_millis"),
    HOUR("hour", "hour"),
    HOURMINUTE("hourMinute", "hour_minute"),
    HOURMINUTESECOND("hourMinuteSecond", "hour_minute_second"),
    HOURMINUTESECONDFRACTION("hourMinuteSecondFraction", "hour_minute_second_fraction"),
    HOURMINUTESECONDMILLIS("hourMinuteSecondMillis", "hour_minute_second_millis"),
    ORDINALDATE("ordinalDate", "ordinal_date"),
    ORDINALDATETIME("ordinalDateTime", "ordinal_date_time"),
    ORDINALDATETIMENOMILLIS("ordinalDateTimeNoMillis", "ordinal_date_time_no_millis"),
    TIME("time", "time"),
    TIMENOMILLIS("timeNoMillis", "time_no_millis"),
    TTIME("tTime", "t_time"),
    TTIMENOMILLIS("tTimeNoMillis", "t_time_no_millis"),
    WEEKDATE("weekDate", "week_date"),
    WEEKDATETIME("weekDateTime", "week_date_time"),
    WEEKDATETIMENOMILLIS("weekDateTimeNoMillis", "week_date_time_no_millis"),
    WEEKYEAR("weekyear", "week_year"),
    WEEKYEARWEEK("weekyearWeek", "weekyear_week"),
    WEEKYEARWEEKDAY("weekyearWeekDay", "weekyear_week_day"),
    YEAR("year", ""),
    YEARMONTH("yearMonth", "year_month"),
    YEARMONTHDAY("yearMonthDay", "year_month_day"),
    EPOCH_SECOND("epoch_second", "epoch_second"),
    EPOCH_MILLIS("epoch_millis", "epoch_millis"),
    // strict date formats here, must be at least 4 digits for year and two for months and two for day"
    STRICTBASICWEEKDATE("strictBasicWeekDate", "strict_basic_week_date"    ),
    STRICTBASICWEEKDATETIME("strictBasicWeekDateTime", "strict_basic_week_date_time"),
    STRICTBASICWEEKDATETIMENOMILLIS("strictBasicWeekDateTimeNoMillis", "strict_basic_week_date_time_no_millis"),
    STRICTDATE("strictDate", "strict_date"),
    STRICTDATEHOUR("strictDateHour", "strict_date_hour"),
    STRICTDATEHOURMINUTE("strictDateHourMinute", "strict_date_hour_minute"),
    STRICTDATEHOURMINUTESECOND("strictDateHourMinuteSecond", "strict_date_hour_minute_second"),
    STRICTDATEHOURMINUTESECONDFRACTION("strictDateHourMinuteSecondFraction", "strict_date_hour_minute_second_fraction"),
    STRICTDATEHOURMINUTESECONDMILLIS("strictDateHourMinuteSecondMillis", "strict_date_hour_minute_second_millis"),
    STRICTDATEOPTIONALTIME("strictDateOptionalTime", "strict_date_optional_time"),
    STRICTDATEOPTIONALTIMENANOS("strictDateOptionalTimeNanos", "strict_date_optional_time_nanos"),
    STRICTDATETIME("strictDateTime", "strict_date_time"),
    STRICTDATETIMENOMILLIS("strictDateTimeNoMillis", "strict_date_time_no_millis"),
    STRICTHOUR("strictHour", "strict_hour"),
    STRICTHOURMINUTE("strictHourMinute", "strict_hour_minute"),
    STRICTHOURMINUTESECOND("strictHourMinuteSecond", "strict_hour_minute_second"),
    STRICTHOURMINUTESECONDFRACTION("strictHourMinuteSecondFraction", "strict_hour_minute_second_fraction"),
    STRICTHOURMINUTESECONDMILLIS("strictHourMinuteSecondMillis", "strict_hour_minute_second_millis"),
    STRICTORDINALDATE("strictOrdinalDate", "strict_ordinal_date"),
    STRICTORDINALDATETIME("strictOrdinalDateTime", "strict_ordinal_date_time"),
    STRICTORDINALDATETIMENOMILLIS("strictOrdinalDateTimeNoMillis", "strict_ordinal_date_time_no_millis"),
    STRICTTIME("strictTime", "strict_time"),
    STRICTTIMENOMILLIS("strictTimeNoMillis", "strict_time_no_millis"),
    STRICTTTIME("strictTTime", "strict_t_time"),
    STRICTTTIMENOMILLIS("strictTTimeNoMillis", "strict_t_time_no_millis"),
    STRICTWEEKDATE("strictWeekDate", "strict_week_date"),
    STRICTWEEKDATETIME("strictWeekDateTime", "strict_week_date_time"),
    STRICTWEEKDATETIMENOMILLIS("strictWeekDateTimeNoMillis", "strict_week_date_time_no_millis"),
    STRICTWEEKYEAR("strictWeekyear", "strict_weekyear"),
    STRICTWEEKYEARWEEK("strictWeekyearWeek", "strict_weekyear_week"),
    STRICTWEEKYEARWEEKDAY("strictWeekyearWeekDay", "strict_weekyear_week_day"),
    STRICTYEAR("strictYear", "strict_year"),
    STRICTYEARMONTH("strictYearMonth", "strict_year_month"),
    STRICTYEARMONTHDAY("strictYearMonthDay", "strict_year_month_day");

    final String camelCaseName;
    final String snakeCaseName;

    FormatNames(String camelCaseName, String snakeCaseName) {
        this.camelCaseName = camelCaseName;
        this.snakeCaseName = snakeCaseName;
    }

    public static boolean exist(String format) {
        for(FormatNames name : values()){
            if(name.camelCaseName.equals(format) || name.snakeCaseName.equals(format))
                return true;
        }
        return false;
    }

    public boolean matches(String format) {
        return format.equals(camelCaseName) || format.equals(snakeCaseName);
    }
}
