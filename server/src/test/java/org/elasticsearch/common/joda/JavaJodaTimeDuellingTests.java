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

package org.elasticsearch.common.joda;

import org.elasticsearch.common.time.CompoundDateTimeFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class JavaJodaTimeDuellingTests extends ESTestCase {

    public void testTimeZoneFormatting() {
        assertSameDate("2001-01-01T00:00:00Z", "date_time_no_millis");
        // the following fail under java 8 but work under java 10, needs investigation
        assertSameDate("2001-01-01T00:00:00-0800", "date_time_no_millis");
        assertSameDate("2001-01-01T00:00:00+1030", "date_time_no_millis");
        assertSameDate("2001-01-01T00:00:00-08", "date_time_no_millis");
        assertSameDate("2001-01-01T00:00:00+10:30", "date_time_no_millis");

        // different timezone parsing styles require a different number of letters
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSXXX", Locale.ROOT);
        formatter.parse("20181126T121212.123Z");
        formatter.parse("20181126T121212.123-08:30");

        DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSXXXX", Locale.ROOT);
        formatter2.parse("20181126T121212.123+1030");
        formatter2.parse("20181126T121212.123-0830");

        // ... and can be combined, note that this is not an XOR, so one could append both timezones with this example
        DateTimeFormatter formatter3 = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS[XXXX][XXX]", Locale.ROOT);
        formatter3.parse("20181126T121212.123Z");
        formatter3.parse("20181126T121212.123-08:30");
        formatter3.parse("20181126T121212.123+1030");
        formatter3.parse("20181126T121212.123-0830");
    }

    public void testCustomTimeFormats() {
        assertSameDate("2010 12 06 11:05:15", "yyyy dd MM HH:mm:ss");
        assertSameDate("12/06", "dd/MM");
        assertSameDate("Nov 24 01:29:01 -0800", "MMM dd HH:mm:ss Z");
    }

    public void testDuellingFormatsValidParsing() {
        assertSameDate("1522332219", "epoch_second");
        assertSameDate("1522332219321", "epoch_millis");

        assertSameDate("20181126", "basic_date");
        assertSameDate("20181126T121212.123Z", "basic_date_time");
        assertSameDate("20181126T121212.123+10:00", "basic_date_time");
        assertSameDate("20181126T121212.123-0800", "basic_date_time");

        assertSameDate("20181126T121212Z", "basic_date_time_no_millis");
        assertSameDate("2018363", "basic_ordinal_date");
        assertSameDate("2018363T121212.123Z", "basic_ordinal_date_time");
        assertSameDate("2018363T121212Z", "basic_ordinal_date_time_no_millis");
        assertSameDate("121212.123Z", "basic_time");
        assertSameDate("121212Z", "basic_time_no_millis");
        assertSameDate("T121212.123Z", "basic_t_time");
        assertSameDate("T121212Z", "basic_t_time_no_millis");
        assertSameDate("2018W313", "basic_week_date");
        assertSameDate("1W313", "basic_week_date");
        assertSameDate("18W313", "basic_week_date");
        assertSameDate("2018W313T121212.123Z", "basic_week_date_time");
        assertSameDate("2018W313T121212Z", "basic_week_date_time_no_millis");

        assertSameDate("2018-12-31", "date");
        assertSameDate("18-5-6", "date");

        assertSameDate("2018-12-31T12", "date_hour");
        assertSameDate("2018-12-31T8", "date_hour");

        assertSameDate("2018-12-31T12:12", "date_hour_minute");
        assertSameDate("2018-12-31T8:3", "date_hour_minute");

        assertSameDate("2018-12-31T12:12:12", "date_hour_minute_second");
        assertSameDate("2018-12-31T12:12:1", "date_hour_minute_second");

        assertSameDate("2018-12-31T12:12:12.123", "date_hour_minute_second_fraction");
        assertSameDate("2018-12-31T12:12:12.123", "date_hour_minute_second_millis");
        assertSameDate("2018-12-31T12:12:12.1", "date_hour_minute_second_millis");
        assertSameDate("2018-12-31T12:12:12.1", "date_hour_minute_second_fraction");

        assertSameDate("2018-12-31", "date_optional_time");
        assertSameDate("2018-12-1", "date_optional_time");
        assertSameDate("2018-12-31T10:15:30", "date_optional_time");
        assertSameDate("2018-12-31T10:15:3", "date_optional_time");
        assertSameDate("2018-12-31T10:5:30", "date_optional_time");
        assertSameDate("2018-12-31T1:15:30", "date_optional_time");

        assertSameDate("2018-12-31T10:15:30.123Z", "date_time");
        assertSameDate("2018-12-31T10:15:30.11Z", "date_time");
        assertSameDate("2018-12-31T10:15:3.123Z", "date_time");

        assertSameDate("2018-12-31T10:15:30Z", "date_time_no_millis");
        assertSameDate("2018-12-31T10:5:30Z", "date_time_no_millis");
        assertSameDate("2018-12-31T10:15:3Z", "date_time_no_millis");
        assertSameDate("2018-12-31T1:15:30Z", "date_time_no_millis");

        assertSameDate("12", "hour");
        assertSameDate("01", "hour");
        assertSameDate("1", "hour");

        assertSameDate("12:12", "hour_minute");
        assertSameDate("12:01", "hour_minute");
        assertSameDate("12:1", "hour_minute");

        assertSameDate("12:12:12", "hour_minute_second");
        assertSameDate("12:12:01", "hour_minute_second");
        assertSameDate("12:12:1", "hour_minute_second");

        assertSameDate("12:12:12.123", "hour_minute_second_fraction");
        assertSameDate("12:12:12.1", "hour_minute_second_fraction");
        assertParseException("12:12:12", "hour_minute_second_fraction");
        assertSameDate("12:12:12.123", "hour_minute_second_millis");
        assertSameDate("12:12:12.1", "hour_minute_second_millis");
        assertParseException("12:12:12", "hour_minute_second_millis");

        assertSameDate("2018-128", "ordinal_date");
        assertSameDate("2018-1", "ordinal_date");

        assertSameDate("2018-128T10:15:30.123Z", "ordinal_date_time");
        assertSameDate("2018-1T10:15:30.123Z", "ordinal_date_time");

        assertSameDate("2018-128T10:15:30Z", "ordinal_date_time_no_millis");
        assertSameDate("2018-1T10:15:30Z", "ordinal_date_time_no_millis");

        assertSameDate("10:15:30.123Z", "time");
        assertSameDate("1:15:30.123Z", "time");
        assertSameDate("10:1:30.123Z", "time");
        assertSameDate("10:15:3.123Z", "time");
        assertParseException("10:15:3.1", "time");
        assertParseException("10:15:3Z", "time");

        assertSameDate("10:15:30Z", "time_no_millis");
        assertSameDate("01:15:30Z", "time_no_millis");
        assertSameDate("1:15:30Z", "time_no_millis");
        assertSameDate("10:5:30Z", "time_no_millis");
        assertSameDate("10:15:3Z", "time_no_millis");
        assertParseException("10:15:3", "time_no_millis");

        assertSameDate("T10:15:30.123Z", "t_time");
        assertSameDate("T1:15:30.123Z", "t_time");
        assertSameDate("T10:1:30.123Z", "t_time");
        assertSameDate("T10:15:3.123Z", "t_time");
        assertParseException("T10:15:3.1", "t_time");
        assertParseException("T10:15:3Z", "t_time");

        assertSameDate("T10:15:30Z", "t_time_no_millis");
        assertSameDate("T1:15:30Z", "t_time_no_millis");
        assertSameDate("T10:1:30Z", "t_time_no_millis");
        assertSameDate("T10:15:3Z", "t_time_no_millis");
        assertParseException("T10:15:3", "t_time_no_millis");

        assertSameDate("2012-W48-6", "week_date");
        assertSameDate("2012-W01-6", "week_date");
        assertSameDate("2012-W1-6", "week_date");
        // joda comes up with a different exception message here, so we have to adapt
        assertJodaParseException("2012-W1-8", "week_date",
            "Cannot parse \"2012-W1-8\": Value 8 for dayOfWeek must be in the range [1,7]");
        assertJavaTimeParseException("2012-W1-8", "week_date", "Text '2012-W1-8' could not be parsed");

        assertSameDate("2012-W48-6T10:15:30.123Z", "week_date_time");
        assertSameDate("2012-W1-6T10:15:30.123Z", "week_date_time");

        assertSameDate("2012-W48-6T10:15:30Z", "week_date_time_no_millis");
        assertSameDate("2012-W1-6T10:15:30Z", "week_date_time_no_millis");

        assertSameDate("2012", "year");
        assertSameDate("1", "year");
        assertSameDate("-2000", "year");

        assertSameDate("2012-12", "yearMonth");
        assertSameDate("1-1", "yearMonth");

        assertSameDate("2012-12-31", "yearMonthDay");
        assertSameDate("1-12-31", "yearMonthDay");
        assertSameDate("2012-1-31", "yearMonthDay");
        assertSameDate("2012-12-1", "yearMonthDay");

        assertSameDate("2018", "week_year");
        assertSameDate("1", "week_year");
        assertSameDate("2017", "week_year");

        assertSameDate("2018-W29", "weekyear_week");
        assertSameDate("2018-W1", "weekyear_week");

        assertSameDate("2012-W31-5", "weekyear_week_day");
        assertSameDate("2012-W1-1", "weekyear_week_day");
    }

    public void testDuelingStrictParsing() {
        assertSameDate("2018W313", "strict_basic_week_date");
        assertParseException("18W313", "strict_basic_week_date");
        assertSameDate("2018W313T121212.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T12128.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T81212.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T12812.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T12812.1Z", "strict_basic_week_date_time");
        assertSameDate("2018W313T121212Z", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12128Z", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T81212Z", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12812Z", "strict_basic_week_date_time_no_millis");
        assertSameDate("2018-12-31", "strict_date");
        assertParseException("2018-8-31", "strict_date");
        assertSameDate("2018-12-31T12", "strict_date_hour");
        assertParseException("2018-12-31T8", "strict_date_hour");
        assertSameDate("2018-12-31T12:12", "strict_date_hour_minute");
        assertParseException("2018-12-31T8:3", "strict_date_hour_minute");
        assertSameDate("2018-12-31T12:12:12", "strict_date_hour_minute_second");
        assertParseException("2018-12-31T12:12:1", "strict_date_hour_minute_second");
        assertSameDate("2018-12-31T12:12:12.123", "strict_date_hour_minute_second_fraction");
        assertSameDate("2018-12-31T12:12:12.123", "strict_date_hour_minute_second_millis");
        assertSameDate("2018-12-31T12:12:12.1", "strict_date_hour_minute_second_millis");
        assertSameDate("2018-12-31T12:12:12.1", "strict_date_hour_minute_second_fraction");
        assertParseException("2018-12-31T12:12:12", "strict_date_hour_minute_second_millis");
        assertParseException("2018-12-31T12:12:12", "strict_date_hour_minute_second_fraction");
        assertSameDate("2018-12-31", "strict_date_optional_time");
        assertParseException("2018-12-1", "strict_date_optional_time");
        assertParseException("2018-1-31", "strict_date_optional_time");
        assertSameDate("2018-12-31T10:15:30", "strict_date_optional_time");
        assertParseException("2018-12-31T10:15:3", "strict_date_optional_time");
        assertParseException("2018-12-31T10:5:30", "strict_date_optional_time");
        assertParseException("2018-12-31T9:15:30", "strict_date_optional_time");
        assertSameDate("2018-12-31T10:15:30.123Z", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.11Z", "strict_date_time");
        assertParseException("2018-12-31T10:15:3.123Z", "strict_date_time");
        assertParseException("2018-12-31T10:5:30.123Z", "strict_date_time");
        assertParseException("2018-12-31T1:15:30.123Z", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30Z", "strict_date_time_no_millis");
        assertParseException("2018-12-31T10:5:30Z", "strict_date_time_no_millis");
        assertParseException("2018-12-31T10:15:3Z", "strict_date_time_no_millis");
        assertParseException("2018-12-31T1:15:30Z", "strict_date_time_no_millis");
        assertSameDate("12", "strict_hour");
        assertSameDate("01", "strict_hour");
        assertParseException("1", "strict_hour");
        assertSameDate("12:12", "strict_hour_minute");
        assertSameDate("12:01", "strict_hour_minute");
        assertParseException("12:1", "strict_hour_minute");
        assertSameDate("12:12:12", "strict_hour_minute_second");
        assertSameDate("12:12:01", "strict_hour_minute_second");
        assertParseException("12:12:1", "strict_hour_minute_second");
        assertSameDate("12:12:12.123", "strict_hour_minute_second_fraction");
        assertSameDate("12:12:12.1", "strict_hour_minute_second_fraction");
        assertParseException("12:12:12", "strict_hour_minute_second_fraction");
        assertSameDate("12:12:12.123", "strict_hour_minute_second_millis");
        assertSameDate("12:12:12.1", "strict_hour_minute_second_millis");
        assertParseException("12:12:12", "strict_hour_minute_second_millis");
        assertSameDate("2018-128", "strict_ordinal_date");
        assertParseException("2018-1", "strict_ordinal_date");

        assertSameDate("2018-128T10:15:30.123Z", "strict_ordinal_date_time");
        assertParseException("2018-1T10:15:30.123Z", "strict_ordinal_date_time");

        assertSameDate("2018-128T10:15:30Z", "strict_ordinal_date_time_no_millis");
        assertParseException("2018-1T10:15:30Z", "strict_ordinal_date_time_no_millis");

        assertSameDate("10:15:30.123Z", "strict_time");
        assertParseException("1:15:30.123Z", "strict_time");
        assertParseException("10:1:30.123Z", "strict_time");
        assertParseException("10:15:3.123Z", "strict_time");
        assertParseException("10:15:3.1", "strict_time");
        assertParseException("10:15:3Z", "strict_time");

        assertSameDate("10:15:30Z", "strict_time_no_millis");
        assertSameDate("01:15:30Z", "strict_time_no_millis");
        assertParseException("1:15:30Z", "strict_time_no_millis");
        assertParseException("10:5:30Z", "strict_time_no_millis");
        assertParseException("10:15:3Z", "strict_time_no_millis");
        assertParseException("10:15:3", "strict_time_no_millis");

        assertSameDate("T10:15:30.123Z", "strict_t_time");
        assertParseException("T1:15:30.123Z", "strict_t_time");
        assertParseException("T10:1:30.123Z", "strict_t_time");
        assertParseException("T10:15:3.123Z", "strict_t_time");
        assertParseException("T10:15:3.1", "strict_t_time");
        assertParseException("T10:15:3Z", "strict_t_time");

        assertSameDate("T10:15:30Z", "strict_t_time_no_millis");
        assertParseException("T1:15:30Z", "strict_t_time_no_millis");
        assertParseException("T10:1:30Z", "strict_t_time_no_millis");
        assertParseException("T10:15:3Z", "strict_t_time_no_millis");
        assertParseException("T10:15:3", "strict_t_time_no_millis");

        assertSameDate("2012-W48-6", "strict_week_date");
        assertSameDate("2012-W01-6", "strict_week_date");
        assertParseException("2012-W1-6", "strict_week_date");
        assertParseException("2012-W1-8", "strict_week_date");

        assertSameDate("2012-W48-6", "strict_week_date");
        assertSameDate("2012-W01-6", "strict_week_date");
        assertParseException("2012-W1-6", "strict_week_date");
        // joda comes up with a different exception message here, so we have to adapt
        assertJodaParseException("2012-W01-8", "strict_week_date",
            "Cannot parse \"2012-W01-8\": Value 8 for dayOfWeek must be in the range [1,7]");
        assertJavaTimeParseException("2012-W01-8", "strict_week_date", "Text '2012-W01-8' could not be parsed");

        assertSameDate("2012-W48-6T10:15:30.123Z", "strict_week_date_time");
        assertParseException("2012-W1-6T10:15:30.123Z", "strict_week_date_time");

        assertSameDate("2012-W48-6T10:15:30Z", "strict_week_date_time_no_millis");
        assertParseException("2012-W1-6T10:15:30Z", "strict_week_date_time_no_millis");

        assertSameDate("2012", "strict_year");
        assertParseException("1", "strict_year");
        assertSameDate("-2000", "strict_year");

        assertSameDate("2012-12", "strict_year_month");
        assertParseException("1-1", "strict_year_month");

        assertSameDate("2012-12-31", "strict_year_month_day");
        assertParseException("1-12-31", "strict_year_month_day");
        assertParseException("2012-1-31", "strict_year_month_day");
        assertParseException("2012-12-1", "strict_year_month_day");

        assertSameDate("2018", "strict_weekyear");
        assertParseException("1", "strict_weekyear");

        assertSameDate("2018", "strict_weekyear");
        assertSameDate("2017", "strict_weekyear");
        assertParseException("1", "strict_weekyear");

        assertSameDate("2018-W29", "strict_weekyear_week");
        assertSameDate("2018-W01", "strict_weekyear_week");
        assertParseException("2018-W1", "strict_weekyear_week");

        assertSameDate("2012-W31-5", "strict_weekyear_week_day");
        assertParseException("2012-W1-1", "strict_weekyear_week_day");
    }

    public void testSeveralTimeFormats() {
        assertSameDate("2018-12-12", "year_month_day||ordinal_date");
        assertSameDate("2018-128", "year_month_day||ordinal_date");
    }

    private void assertSameDate(String input, String format) {
        FormatDateTimeFormatter jodaFormatter = Joda.forPattern(format);
        DateTime jodaDateTime = jodaFormatter.parser().parseDateTime(input);

        CompoundDateTimeFormatter javaTimeFormatter = DateFormatters.forPattern(format);
        TemporalAccessor javaTimeAccessor = javaTimeFormatter.parse(input);
        ZonedDateTime zonedDateTime = DateFormatters.toZonedDateTime(javaTimeAccessor);

        String msg = String.format(Locale.ROOT, "Input [%s] Format [%s] Joda [%s], Java [%s]", input, format, jodaDateTime,
            DateTimeFormatter.ISO_INSTANT.format(zonedDateTime.toInstant()));

        assertThat(msg, jodaDateTime.getMillis(), is(zonedDateTime.toInstant().toEpochMilli()));
    }

    private void assertParseException(String input, String format) {
        assertJodaParseException(input, format, "Invalid format: \"" + input);
        assertJavaTimeParseException(input, format, "Text '" + input + "' could not be parsed");
    }

    private void assertJodaParseException(String input, String format, String expectedMessage) {
        FormatDateTimeFormatter jodaFormatter = Joda.forPattern(format);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> jodaFormatter.parser().parseDateTime(input));
        assertThat(e.getMessage(), containsString(expectedMessage));
    }

    private void assertJavaTimeParseException(String input, String format, String expectedMessage) {
        CompoundDateTimeFormatter javaTimeFormatter = DateFormatters.forPattern(format);
        DateTimeParseException dateTimeParseException = expectThrows(DateTimeParseException.class, () -> javaTimeFormatter.parse(input));
        assertThat(dateTimeParseException.getMessage(), startsWith(expectedMessage));
    }
}
