/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.joda;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class JavaJodaTimeDuellingTests extends ESTestCase {
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }
    @BeforeClass
    public static void checkJvmProperties(){
        assert ("SPI,COMPAT".equals(System.getProperty("java.locale.providers")))
            : "`-Djava.locale.providers=SPI,COMPAT` needs to be set";
    }

    public void testIncorrectFormat() {
        assertParseException("2021-01-01T23-35-00Z", "strict_date_optional_time||epoch_millis");
        assertParseException("2021-01-01T23-35-00Z", "strict_date_optional_time");
    }

    public void testMinMillis() {
        String jodaFormatted = Joda.forPattern("strict_date_optional_time").formatMillis(Long.MIN_VALUE);
        String javaFormatted = DateFormatter.forPattern("strict_date_optional_time").formatMillis(Long.MIN_VALUE);
        Assert.assertEquals(jodaFormatted, javaFormatted);
    }
    public void testYearParsing() {
        //this one is considered a year
        assertSameDate("1234", "strict_date_optional_time||epoch_millis");
        //this one is considered a 12345milliseconds since epoch
        assertSameDate("12345", "strict_date_optional_time||epoch_millis");
    }

    public void testTimezoneParsing() {
        /** this testcase won't work in joda. See comment in {@link #testPartialTimeParsing()}
         *  assertSameDateAs("2016-11-30T+01", "strict_date_optional_time", "strict_date_optional_time");
         */
        assertSameDateAs("2016-11-30T00+01", "strict_date_optional_time", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T00+0100", "strict_date_optional_time", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T00+01:00", "strict_date_optional_time", "strict_date_optional_time");
    }

    public void testPartialTimeParsing() {
         /*
         This does not work in Joda as it reports 2016-11-30T01:00:00Z
         because StrictDateOptionalTime confuses +01 with an hour (which is a signed fixed length digit)
         assertSameDateAs("2016-11-30T+01", "strict_date_optional_time", "strict_date_optional_time");
         ES java.time implementation does not suffer from this,
         but we intentionally not allow parsing timezone without an time part as it is not allowed in iso8601
  */
        assertJavaTimeParseException("2016-11-30T+01","strict_date_optional_time");

        assertSameDateAs("2016-11-30T12+01", "strict_date_optional_time", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00+01", "strict_date_optional_time", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00:00+01", "strict_date_optional_time", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00:00.000+01", "strict_date_optional_time", "strict_date_optional_time");

        //without timezone
        assertSameDateAs("2016-11-30T", "strict_date_optional_time", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12", "strict_date_optional_time", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00", "strict_date_optional_time", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00:00", "strict_date_optional_time", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00:00.000", "strict_date_optional_time", "strict_date_optional_time");
    }

    // date_optional part of a parser names "strict_date_optional_time" or "date_optional"time
    // means that date part can be partially parsed.
    public void testPartialDateParsing() {
        assertSameDateAs("2001", "strict_date_optional_time_nanos", "strict_date_optional_time");
        assertSameDateAs("2001-01", "strict_date_optional_time_nanos", "strict_date_optional_time");
        assertSameDateAs("2001-01-01", "strict_date_optional_time_nanos", "strict_date_optional_time");

        assertSameDate("2001", "strict_date_optional_time");
        assertSameDate("2001-01", "strict_date_optional_time");
        assertSameDate("2001-01-01", "strict_date_optional_time");

        assertSameDate("2001", "date_optional_time");
        assertSameDate("2001-01", "date_optional_time");
        assertSameDate("2001-01-01", "date_optional_time");


        assertSameDateAs("2001", "iso8601", "strict_date_optional_time");
        assertSameDateAs("2001-01", "iso8601", "strict_date_optional_time");
        assertSameDateAs("2001-01-01", "iso8601", "strict_date_optional_time");

        assertSameDate("9999","date_optional_time||epoch_second");
    }

    public void testCompositeDateMathParsing(){
        //in all these examples the second pattern will be used
        assertDateMathEquals("2014-06-06T12:01:02.123", "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS");
        assertDateMathEquals("2014-06-06T12:01:02.123", "strict_date_time_no_millis||yyyy-MM-dd'T'HH:mm:ss.SSS");
        assertDateMathEquals("2014-06-06T12:01:02.123", "yyyy-MM-dd'T'HH:mm:ss+HH:MM||yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    public void testExceptionWhenCompositeParsingFailsDateMath(){
        //both parsing failures should contain pattern and input text in exception
        //both patterns fail parsing the input text due to only 2 digits of millis. Hence full text was not parsed.
        String pattern = "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SS";
        String text = "2014-06-06T12:01:02.123";
        ElasticsearchParseException e1 = expectThrows(ElasticsearchParseException.class,
            () -> dateMathToMillis(text, DateFormatter.forPattern(pattern)));
        assertThat(e1.getMessage(), containsString(pattern));
        assertThat(e1.getMessage(), containsString(text));

        ElasticsearchParseException e2 = expectThrows(ElasticsearchParseException.class,
            () -> dateMathToMillis(text, Joda.forPattern(pattern)));
        assertThat(e2.getMessage(), containsString(pattern));
        assertThat(e2.getMessage(), containsString(text));
    }

    private long dateMathToMillis(String text, DateFormatter dateFormatter) {
        DateFormatter javaFormatter = dateFormatter.withLocale(randomLocale(random()));
        DateMathParser javaDateMath = javaFormatter.toDateMathParser();
        return javaDateMath.parse(text, () -> 0, true, (ZoneId) null).toEpochMilli();
    }

    public void testDayOfWeek() {
        //7 (ok joda) vs 1 (java by default) but 7 with customized org.elasticsearch.common.time.IsoLocale.ISO8601
        ZonedDateTime now = LocalDateTime.of(2009,11,15,1,32,8,328402)
                                         .atZone(ZoneOffset.UTC); //Sunday
        DateFormatter jodaFormatter = Joda.forPattern("e").withLocale(Locale.ROOT).withZone(ZoneOffset.UTC);
        DateFormatter javaFormatter = DateFormatter.forPattern("8e").withZone(ZoneOffset.UTC);
        assertThat(jodaFormatter.format(now), equalTo(javaFormatter.format(now)));
    }

    public void testStartOfWeek() {
        //2019-21 (ok joda) vs 2019-22 (java by default) but 2019-21 with customized org.elasticsearch.common.time.IsoLocale.ISO8601
        ZonedDateTime now = LocalDateTime.of(2019,5,26,1,32,8,328402)
                                         .atZone(ZoneOffset.UTC);
        DateFormatter jodaFormatter = Joda.forPattern("xxxx-ww").withLocale(Locale.ROOT).withZone(ZoneOffset.UTC);
        DateFormatter javaFormatter = DateFormatter.forPattern("8YYYY-ww").withZone(ZoneOffset.UTC);
        assertThat(jodaFormatter.format(now), equalTo(javaFormatter.format(now)));
    }

    //these parsers should allow both ',' and '.' as a decimal point
    public void testDecimalPointParsing(){
        assertSameDate("2001-01-01T00:00:00.123Z", "strict_date_optional_time");
        assertSameDate("2001-01-01T00:00:00,123Z", "strict_date_optional_time");

        assertSameDate("2001-01-01T00:00:00.123Z", "date_optional_time");
        assertSameDate("2001-01-01T00:00:00,123Z", "date_optional_time");

        // only java.time has nanos parsing, but the results for 3digits should be the same
        DateFormatter jodaFormatter = Joda.forPattern("strict_date_optional_time");
        DateFormatter javaFormatter = DateFormatter.forPattern("strict_date_optional_time_nanos");
        assertSameDate("2001-01-01T00:00:00.123Z", "strict_date_optional_time_nanos", jodaFormatter, javaFormatter);
        assertSameDate("2001-01-01T00:00:00,123Z", "strict_date_optional_time_nanos", jodaFormatter, javaFormatter);

        assertParseException("2001-01-01T00:00:00.123,456Z", "strict_date_optional_time");
        assertParseException("2001-01-01T00:00:00.123,456Z", "date_optional_time");
        //This should fail, but java is ok with this because the field has the same value
//        assertJavaTimeParseException("2001-01-01T00:00:00.123,123Z", "strict_date_optional_time_nanos");
    }

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

    public void testCustomLocales() {
        // also ensure that locale based dates are the same
        assertSameDate("Di, 05 Dez 2000 02:55:00 -0800", "E, d MMM yyyy HH:mm:ss Z", LocaleUtils.parse("de"));
        assertSameDate("Mi, 06 Dez 2000 02:55:00 -0800", "E, d MMM yyyy HH:mm:ss Z", LocaleUtils.parse("de"));
        assertSameDate("Do, 07 Dez 2000 00:00:00 -0800", "E, d MMM yyyy HH:mm:ss Z", LocaleUtils.parse("de"));
        assertSameDate("Fr, 08 Dez 2000 00:00:00 -0800", "E, d MMM yyyy HH:mm:ss Z", LocaleUtils.parse("de"));

        DateTime dateTimeNow = DateTime.now(DateTimeZone.UTC);
        ZonedDateTime javaTimeNow = Instant.ofEpochMilli(dateTimeNow.getMillis()).atZone(ZoneOffset.UTC);
        assertSamePrinterOutput("E, d MMM yyyy HH:mm:ss Z", javaTimeNow, dateTimeNow, LocaleUtils.parse("de"));
    }

    public void testDuellingFormatsValidParsing() {
        assertSameDate("1522332219", "epoch_second");
        assertSameDate("0", "epoch_second");
        assertSameDate("1", "epoch_second");
        assertSameDate("1522332219321", "epoch_millis");
        assertSameDate("0", "epoch_millis");
        assertSameDate("1", "epoch_millis");

        assertSameDate("20181126", "basic_date");
        assertSameDate("20181126T121212.123Z", "basic_date_time");
        assertSameDate("20181126T121212.123+10:00", "basic_date_time");
        assertSameDate("20181126T121212.123-0800", "basic_date_time");

        assertSameDate("20181126T121212Z", "basic_date_time_no_millis");
        assertSameDate("20181126T121212+01:00", "basic_date_time_no_millis");
        assertSameDate("20181126T121212+0100", "basic_date_time_no_millis");
        assertSameDate("2018363", "basic_ordinal_date");
        assertSameDate("2018363T121212.1Z", "basic_ordinal_date_time");
        assertSameDate("2018363T121212.123Z", "basic_ordinal_date_time");
        assertSameDate("2018363T121212.123456789Z", "basic_ordinal_date_time");
        assertSameDate("2018363T121212.123+0100", "basic_ordinal_date_time");
        assertSameDate("2018363T121212.123+01:00", "basic_ordinal_date_time");
        assertSameDate("2018363T121212Z", "basic_ordinal_date_time_no_millis");
        assertSameDate("2018363T121212+0100", "basic_ordinal_date_time_no_millis");
        assertSameDate("2018363T121212+01:00", "basic_ordinal_date_time_no_millis");
        assertSameDate("121212.1Z", "basic_time");
        assertSameDate("121212.123Z", "basic_time");
        assertSameDate("121212.123456789Z", "basic_time");
        assertSameDate("121212.1+0100", "basic_time");
        assertSameDate("121212.123+0100", "basic_time");
        assertSameDate("121212.123+01:00", "basic_time");
        assertSameDate("121212Z", "basic_time_no_millis");
        assertSameDate("121212+0100", "basic_time_no_millis");
        assertSameDate("121212+01:00", "basic_time_no_millis");
        assertSameDate("T121212.1Z", "basic_t_time");
        assertSameDate("T121212.123Z", "basic_t_time");
        assertSameDate("T121212.123456789Z", "basic_t_time");
        assertSameDate("T121212.1+0100", "basic_t_time");
        assertSameDate("T121212.123+0100", "basic_t_time");
        assertSameDate("T121212.123+01:00", "basic_t_time");
        assertSameDate("T121212Z", "basic_t_time_no_millis");
        assertSameDate("T121212+0100", "basic_t_time_no_millis");
        assertSameDate("T121212+01:00", "basic_t_time_no_millis");
        assertSameDate("2018W313", "basic_week_date");
        assertSameDate("1W313", "basic_week_date");
        assertSameDate("18W313", "basic_week_date");
        assertSameDate("2018W313T121212.1Z", "basic_week_date_time");
        assertSameDate("2018W313T121212.123Z", "basic_week_date_time");
        assertSameDate("2018W313T121212.123456789Z", "basic_week_date_time");
        assertSameDate("2018W313T121212.123+0100", "basic_week_date_time");
        assertSameDate("2018W313T121212.123+01:00", "basic_week_date_time");
        assertSameDate("2018W313T121212Z", "basic_week_date_time_no_millis");
        assertSameDate("2018W313T121212+0100", "basic_week_date_time_no_millis");
        assertSameDate("2018W313T121212+01:00", "basic_week_date_time_no_millis");

        assertSameDate("2018-12-31", "date");
        assertSameDate("18-5-6", "date");
        assertSameDate("10000-5-6", "date");

        assertSameDate("2018-12-31T12", "date_hour");
        assertSameDate("2018-12-31T8", "date_hour");

        assertSameDate("2018-12-31T12:12", "date_hour_minute");
        assertSameDate("2018-12-31T8:3", "date_hour_minute");

        assertSameDate("2018-12-31T12:12:12", "date_hour_minute_second");
        assertSameDate("2018-12-31T12:12:1", "date_hour_minute_second");

        assertSameDate("2018-12-31T12:12:12.1", "date_hour_minute_second_fraction");
        assertSameDate("2018-12-31T12:12:12.123", "date_hour_minute_second_fraction");
        assertSameDate("2018-12-31T12:12:12.123456789", "date_hour_minute_second_fraction");
        assertSameDate("2018-12-31T12:12:12.1", "date_hour_minute_second_millis");
        assertSameDate("2018-12-31T12:12:12.123", "date_hour_minute_second_millis");
        assertParseException("2018-12-31T12:12:12.123456789", "date_hour_minute_second_millis");
        assertSameDate("2018-12-31T12:12:12.1", "date_hour_minute_second_millis");
        assertSameDate("2018-12-31T12:12:12.1", "date_hour_minute_second_fraction");

        assertSameDate("2018-05", "date_optional_time");
        assertSameDate("2018-05-30", "date_optional_time");
        assertSameDate("2018-05-30T20", "date_optional_time");
        assertSameDate("2018-05-30T20:21", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23.1", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23.123", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23.123456789", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23.123Z", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23.123456789Z", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23.1+0100", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23.123+0100", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23.1+01:00", "date_optional_time");
        assertSameDate("2018-05-30T20:21:23.123+01:00", "date_optional_time");
        assertSameDate("2018-12-1", "date_optional_time");
        assertSameDate("2018-12-31T10:15:30", "date_optional_time");
        assertSameDate("2018-12-31T10:15:3", "date_optional_time");
        assertSameDate("2018-12-31T10:5:30", "date_optional_time");
        assertSameDate("2018-12-31T1:15:30", "date_optional_time");

        assertSameDate("2018-12-31T10:15:30.1Z", "date_time");
        assertSameDate("2018-12-31T10:15:30.123Z", "date_time");
        assertSameDate("2018-12-31T10:15:30.123456789Z", "date_time");
        assertSameDate("2018-12-31T10:15:30.1+0100", "date_time");
        assertSameDate("2018-12-31T10:15:30.123+0100", "date_time");
        assertSameDate("2018-12-31T10:15:30.123+01:00", "date_time");
        assertSameDate("2018-12-31T10:15:30.1+01:00", "date_time");
        assertSameDate("2018-12-31T10:15:30.11Z", "date_time");
        assertSameDate("2018-12-31T10:15:30.11+0100", "date_time");
        assertSameDate("2018-12-31T10:15:30.11+01:00", "date_time");
        assertSameDate("2018-12-31T10:15:3.1Z", "date_time");
        assertSameDate("2018-12-31T10:15:3.123Z", "date_time");
        assertSameDate("2018-12-31T10:15:3.123456789Z", "date_time");
        assertSameDate("2018-12-31T10:15:3.1+0100", "date_time");
        assertSameDate("2018-12-31T10:15:3.123+0100", "date_time");
        assertSameDate("2018-12-31T10:15:3.123+01:00", "date_time");
        assertSameDate("2018-12-31T10:15:3.1+01:00", "date_time");

        assertSameDate("2018-12-31T10:15:30Z", "date_time_no_millis");
        assertSameDate("2018-12-31T10:15:30+0100", "date_time_no_millis");
        assertSameDate("2018-12-31T10:15:30+01:00", "date_time_no_millis");
        assertSameDate("2018-12-31T10:5:30Z", "date_time_no_millis");
        assertSameDate("2018-12-31T10:5:30+0100", "date_time_no_millis");
        assertSameDate("2018-12-31T10:5:30+01:00", "date_time_no_millis");
        assertSameDate("2018-12-31T10:15:3Z", "date_time_no_millis");
        assertSameDate("2018-12-31T10:15:3+0100", "date_time_no_millis");
        assertSameDate("2018-12-31T10:15:3+01:00", "date_time_no_millis");
        assertSameDate("2018-12-31T1:15:30Z", "date_time_no_millis");
        assertSameDate("2018-12-31T1:15:30+0100", "date_time_no_millis");
        assertSameDate("2018-12-31T1:15:30+01:00", "date_time_no_millis");

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
        assertSameDate("12:12:12.123456789", "hour_minute_second_fraction");
        assertSameDate("12:12:12.1", "hour_minute_second_fraction");
        assertParseException("12:12:12", "hour_minute_second_fraction");
        assertSameDate("12:12:12.123", "hour_minute_second_millis");
        assertParseException("12:12:12.123456789", "hour_minute_second_millis");
        assertSameDate("12:12:12.1", "hour_minute_second_millis");
        assertParseException("12:12:12", "hour_minute_second_millis");

        assertSameDate("2018-128", "ordinal_date");
        assertSameDate("2018-1", "ordinal_date");

        assertSameDate("2018-128T10:15:30.1Z", "ordinal_date_time");
        assertSameDate("2018-128T10:15:30.123Z", "ordinal_date_time");
        assertSameDate("2018-128T10:15:30.123456789Z", "ordinal_date_time");
        assertSameDate("2018-128T10:15:30.123+0100", "ordinal_date_time");
        assertSameDate("2018-128T10:15:30.123+01:00", "ordinal_date_time");
        assertSameDate("2018-1T10:15:30.1Z", "ordinal_date_time");
        assertSameDate("2018-1T10:15:30.123Z", "ordinal_date_time");
        assertSameDate("2018-1T10:15:30.123456789Z", "ordinal_date_time");
        assertSameDate("2018-1T10:15:30.123+0100", "ordinal_date_time");
        assertSameDate("2018-1T10:15:30.123+01:00", "ordinal_date_time");

        assertSameDate("2018-128T10:15:30Z", "ordinal_date_time_no_millis");
        assertSameDate("2018-128T10:15:30+0100", "ordinal_date_time_no_millis");
        assertSameDate("2018-128T10:15:30+01:00", "ordinal_date_time_no_millis");
        assertSameDate("2018-1T10:15:30Z", "ordinal_date_time_no_millis");
        assertSameDate("2018-1T10:15:30+0100", "ordinal_date_time_no_millis");
        assertSameDate("2018-1T10:15:30+01:00", "ordinal_date_time_no_millis");

        assertSameDate("10:15:30.1Z", "time");
        assertSameDate("10:15:30.123Z", "time");
        assertSameDate("10:15:30.123456789Z", "time");
        assertSameDate("10:15:30.123+0100", "time");
        assertSameDate("10:15:30.123+01:00", "time");
        assertSameDate("1:15:30.1Z", "time");
        assertSameDate("1:15:30.123Z", "time");
        assertSameDate("1:15:30.123+0100", "time");
        assertSameDate("1:15:30.123+01:00", "time");
        assertSameDate("10:1:30.1Z", "time");
        assertSameDate("10:1:30.123Z", "time");
        assertSameDate("10:1:30.123+0100", "time");
        assertSameDate("10:1:30.123+01:00", "time");
        assertSameDate("10:15:3.1Z", "time");
        assertSameDate("10:15:3.123Z", "time");
        assertSameDate("10:15:3.123+0100", "time");
        assertSameDate("10:15:3.123+01:00", "time");
        assertParseException("10:15:3.1", "time");
        assertParseException("10:15:3Z", "time");

        assertSameDate("10:15:30Z", "time_no_millis");
        assertSameDate("10:15:30+0100", "time_no_millis");
        assertSameDate("10:15:30+01:00", "time_no_millis");
        assertSameDate("01:15:30Z", "time_no_millis");
        assertSameDate("01:15:30+0100", "time_no_millis");
        assertSameDate("01:15:30+01:00", "time_no_millis");
        assertSameDate("1:15:30Z", "time_no_millis");
        assertSameDate("1:15:30+0100", "time_no_millis");
        assertSameDate("1:15:30+01:00", "time_no_millis");
        assertSameDate("10:5:30Z", "time_no_millis");
        assertSameDate("10:5:30+0100", "time_no_millis");
        assertSameDate("10:5:30+01:00", "time_no_millis");
        assertSameDate("10:15:3Z", "time_no_millis");
        assertSameDate("10:15:3+0100", "time_no_millis");
        assertSameDate("10:15:3+01:00", "time_no_millis");
        assertParseException("10:15:3", "time_no_millis");

        assertSameDate("T10:15:30.1Z", "t_time");
        assertSameDate("T10:15:30.123Z", "t_time");
        assertSameDate("T10:15:30.123456789Z", "t_time");
        assertSameDate("T10:15:30.1+0100", "t_time");
        assertSameDate("T10:15:30.123+0100", "t_time");
        assertSameDate("T10:15:30.123+01:00", "t_time");
        assertSameDate("T10:15:30.1+01:00", "t_time");
        assertSameDate("T1:15:30.123Z", "t_time");
        assertSameDate("T1:15:30.123+0100", "t_time");
        assertSameDate("T1:15:30.123+01:00", "t_time");
        assertSameDate("T10:1:30.123Z", "t_time");
        assertSameDate("T10:1:30.123+0100", "t_time");
        assertSameDate("T10:1:30.123+01:00", "t_time");
        assertSameDate("T10:15:3.123Z", "t_time");
        assertSameDate("T10:15:3.123+0100", "t_time");
        assertSameDate("T10:15:3.123+01:00", "t_time");
        assertParseException("T10:15:3.1", "t_time");
        assertParseException("T10:15:3Z", "t_time");

        assertSameDate("T10:15:30Z", "t_time_no_millis");
        assertSameDate("T10:15:30+0100", "t_time_no_millis");
        assertSameDate("T10:15:30+01:00", "t_time_no_millis");
        assertSameDate("T1:15:30Z", "t_time_no_millis");
        assertSameDate("T1:15:30+0100", "t_time_no_millis");
        assertSameDate("T1:15:30+01:00", "t_time_no_millis");
        assertSameDate("T10:1:30Z", "t_time_no_millis");
        assertSameDate("T10:1:30+0100", "t_time_no_millis");
        assertSameDate("T10:1:30+01:00", "t_time_no_millis");
        assertSameDate("T10:15:3Z", "t_time_no_millis");
        assertSameDate("T10:15:3+0100", "t_time_no_millis");
        assertSameDate("T10:15:3+01:00", "t_time_no_millis");
        assertParseException("T10:15:3", "t_time_no_millis");

        assertSameDate("2012-W48-6", "week_date");
        assertSameDate("2012-W01-6", "week_date");
        assertSameDate("2012-W1-6", "week_date");
        // joda comes up with a different exception message here, so we have to adapt
        assertJodaParseException("2012-W1-8", "week_date",
            "Cannot parse \"2012-W1-8\": Value 8 for dayOfWeek must be in the range [1,7]");
        assertJavaTimeParseException("2012-W1-8", "week_date");

        assertSameDate("2012-W48-6T10:15:30.1Z", "week_date_time");
        assertSameDate("2012-W48-6T10:15:30.123Z", "week_date_time");
        assertSameDate("2012-W48-6T10:15:30.123456789Z", "week_date_time");
        assertSameDate("2012-W48-6T10:15:30.1+0100", "week_date_time");
        assertSameDate("2012-W48-6T10:15:30.123+0100", "week_date_time");
        assertSameDate("2012-W48-6T10:15:30.1+01:00", "week_date_time");
        assertSameDate("2012-W48-6T10:15:30.123+01:00", "week_date_time");
        assertSameDate("2012-W1-6T10:15:30.1Z", "week_date_time");
        assertSameDate("2012-W1-6T10:15:30.123Z", "week_date_time");
        assertSameDate("2012-W1-6T10:15:30.1+0100", "week_date_time");
        assertSameDate("2012-W1-6T10:15:30.123+0100", "week_date_time");
        assertSameDate("2012-W1-6T10:15:30.1+01:00", "week_date_time");
        assertSameDate("2012-W1-6T10:15:30.123+01:00", "week_date_time");

        assertSameDate("2012-W48-6T10:15:30Z", "week_date_time_no_millis");
        assertSameDate("2012-W48-6T10:15:30+0100", "week_date_time_no_millis");
        assertSameDate("2012-W48-6T10:15:30+01:00", "week_date_time_no_millis");
        assertSameDate("2012-W1-6T10:15:30Z", "week_date_time_no_millis");
        assertSameDate("2012-W1-6T10:15:30+0100", "week_date_time_no_millis");
        assertSameDate("2012-W1-6T10:15:30+01:00", "week_date_time_no_millis");

        assertSameDate("2012", "year");
        assertSameDate("1", "year");
        assertSameDate("-2000", "year");

        assertSameDate("2012-12", "year_month");
        assertSameDate("1-1", "year_month");

        assertSameDate("2012-12-31", "year_month_day");
        assertSameDate("1-12-31", "year_month_day");
        assertSameDate("2012-1-31", "year_month_day");
        assertSameDate("2012-12-1", "year_month_day");

        assertSameDate("2018", "weekyear");
        assertSameDate("1", "weekyear");
        assertSameDate("2017", "weekyear");

        assertSameDate("2018-W29", "weekyear_week");
        assertSameDate("2018-W1", "weekyear_week");

        assertSameDate("2012-W31-5", "weekyear_week_day");
        assertSameDate("2012-W1-1", "weekyear_week_day");
    }

    public void testCompositeParsing(){
        //in all these examples the second pattern will be used
        assertSameDate("2014-06-06T12:01:02.123", "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS");
        assertSameDate("2014-06-06T12:01:02.123", "strict_date_time_no_millis||yyyy-MM-dd'T'HH:mm:ss.SSS");
        assertSameDate("2014-06-06T12:01:02.123", "yyyy-MM-dd'T'HH:mm:ss+HH:MM||yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    public void testExceptionWhenCompositeParsingFails(){
        assertParseException("2014-06-06T12:01:02.123", "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SS");
    }

    public void testDuelingStrictParsing() {
        assertSameDate("2018W313", "strict_basic_week_date");
        assertParseException("18W313", "strict_basic_week_date");
        assertSameDate("2018W313T121212.1Z", "strict_basic_week_date_time");
        assertSameDate("2018W313T121212.123Z", "strict_basic_week_date_time");
        assertSameDate("2018W313T121212.123456789Z", "strict_basic_week_date_time");
        assertSameDate("2018W313T121212.1+0100", "strict_basic_week_date_time");
        assertSameDate("2018W313T121212.123+0100", "strict_basic_week_date_time");
        assertSameDate("2018W313T121212.1+01:00", "strict_basic_week_date_time");
        assertSameDate("2018W313T121212.123+01:00", "strict_basic_week_date_time");
        assertParseException("2018W313T12128.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T12128.123456789Z", "strict_basic_week_date_time");
        assertParseException("2018W313T81212.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T12812.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T12812.1Z", "strict_basic_week_date_time");
        assertSameDate("2018W313T121212Z", "strict_basic_week_date_time_no_millis");
        assertSameDate("2018W313T121212+0100", "strict_basic_week_date_time_no_millis");
        assertSameDate("2018W313T121212+01:00", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12128Z", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12128+0100", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12128+01:00", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T81212Z", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T81212+0100", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T81212+01:00", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12812Z", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12812+0100", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12812+01:00", "strict_basic_week_date_time_no_millis");
        assertSameDate("2018-12-31", "strict_date");
        assertParseException("10000-12-31", "strict_date");
        assertParseException("2018-8-31", "strict_date");
        assertSameDate("2018-12-31T12", "strict_date_hour");
        assertParseException("2018-12-31T8", "strict_date_hour");
        assertSameDate("2018-12-31T12:12", "strict_date_hour_minute");
        assertParseException("2018-12-31T8:3", "strict_date_hour_minute");
        assertSameDate("2018-12-31T12:12:12", "strict_date_hour_minute_second");
        assertParseException("2018-12-31T12:12:1", "strict_date_hour_minute_second");
        assertSameDate("2018-12-31T12:12:12.1", "strict_date_hour_minute_second_fraction");
        assertSameDate("2018-12-31T12:12:12.123", "strict_date_hour_minute_second_fraction");
        assertSameDate("2018-12-31T12:12:12.123456789", "strict_date_hour_minute_second_fraction");
        assertSameDate("2018-12-31T12:12:12.123", "strict_date_hour_minute_second_millis");
        assertSameDate("2018-12-31T12:12:12.1", "strict_date_hour_minute_second_millis");
        assertSameDate("2018-12-31T12:12:12.1", "strict_date_hour_minute_second_fraction");
        assertParseException("2018-12-31T12:12:12", "strict_date_hour_minute_second_millis");
        assertParseException("2018-12-31T12:12:12", "strict_date_hour_minute_second_fraction");
        assertSameDate("2018-12-31", "strict_date_optional_time");
        assertParseException("2018-12-1", "strict_date_optional_time");
        assertParseException("2018-1-31", "strict_date_optional_time");
        assertParseException("10000-01-31", "strict_date_optional_time");
        assertSameDate("2010-01-05T02:00", "strict_date_optional_time");
        assertSameDate("2018-12-31T10:15:30", "strict_date_optional_time");
        assertSameDate("2018-12-31T10:15:30Z", "strict_date_optional_time");
        assertSameDate("2018-12-31T10:15:30+0100", "strict_date_optional_time");
        assertSameDate("2018-12-31T10:15:30+01:00", "strict_date_optional_time");
        assertParseException("2018-12-31T10:15:3", "strict_date_optional_time");
        assertParseException("2018-12-31T10:5:30", "strict_date_optional_time");
        assertParseException("2018-12-31T9:15:30", "strict_date_optional_time");
        assertSameDate("2015-01-04T00:00Z", "strict_date_optional_time");
        assertSameDate("2018-12-31T10:15:30.1Z", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.123Z", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.123456789Z", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.1+0100", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.123+0100", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.1+01:00", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.123+01:00", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.11Z", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.11+0100", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30.11+01:00", "strict_date_time");
        assertParseException("2018-12-31T10:15:3.123Z", "strict_date_time");
        assertParseException("2018-12-31T10:5:30.123Z", "strict_date_time");
        assertParseException("2018-12-31T1:15:30.123Z", "strict_date_time");
        assertSameDate("2018-12-31T10:15:30Z", "strict_date_time_no_millis");
        assertSameDate("2018-12-31T10:15:30+0100", "strict_date_time_no_millis");
        assertSameDate("2018-12-31T10:15:30+01:00", "strict_date_time_no_millis");
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
        assertSameDate("12:12:12.123456789", "strict_hour_minute_second_fraction");
        assertSameDate("12:12:12.1", "strict_hour_minute_second_fraction");
        assertParseException("12:12:12", "strict_hour_minute_second_fraction");
        assertSameDate("12:12:12.123", "strict_hour_minute_second_millis");
        assertSameDate("12:12:12.1", "strict_hour_minute_second_millis");
        assertParseException("12:12:12", "strict_hour_minute_second_millis");
        assertSameDate("2018-128", "strict_ordinal_date");
        assertParseException("2018-1", "strict_ordinal_date");

        assertSameDate("2018-128T10:15:30.1Z", "strict_ordinal_date_time");
        assertSameDate("2018-128T10:15:30.123Z", "strict_ordinal_date_time");
        assertSameDate("2018-128T10:15:30.123456789Z", "strict_ordinal_date_time");
        assertSameDate("2018-128T10:15:30.1+0100", "strict_ordinal_date_time");
        assertSameDate("2018-128T10:15:30.123+0100", "strict_ordinal_date_time");
        assertSameDate("2018-128T10:15:30.1+01:00", "strict_ordinal_date_time");
        assertSameDate("2018-128T10:15:30.123+01:00", "strict_ordinal_date_time");
        assertParseException("2018-1T10:15:30.123Z", "strict_ordinal_date_time");

        assertSameDate("2018-128T10:15:30Z", "strict_ordinal_date_time_no_millis");
        assertSameDate("2018-128T10:15:30+0100", "strict_ordinal_date_time_no_millis");
        assertSameDate("2018-128T10:15:30+01:00", "strict_ordinal_date_time_no_millis");
        assertParseException("2018-1T10:15:30Z", "strict_ordinal_date_time_no_millis");

        assertSameDate("10:15:30.1Z", "strict_time");
        assertSameDate("10:15:30.123Z", "strict_time");
        assertSameDate("10:15:30.123456789Z", "strict_time");
        assertSameDate("10:15:30.123+0100", "strict_time");
        assertSameDate("10:15:30.123+01:00", "strict_time");
        assertParseException("1:15:30.123Z", "strict_time");
        assertParseException("10:1:30.123Z", "strict_time");
        assertParseException("10:15:3.123Z", "strict_time");
        assertParseException("10:15:3.1", "strict_time");
        assertParseException("10:15:3Z", "strict_time");

        assertSameDate("10:15:30Z", "strict_time_no_millis");
        assertSameDate("10:15:30+0100", "strict_time_no_millis");
        assertSameDate("10:15:30+01:00", "strict_time_no_millis");
        assertSameDate("01:15:30Z", "strict_time_no_millis");
        assertSameDate("01:15:30+0100", "strict_time_no_millis");
        assertSameDate("01:15:30+01:00", "strict_time_no_millis");
        assertParseException("1:15:30Z", "strict_time_no_millis");
        assertParseException("10:5:30Z", "strict_time_no_millis");
        assertParseException("10:15:3Z", "strict_time_no_millis");
        assertParseException("10:15:3", "strict_time_no_millis");

        assertSameDate("T10:15:30.1Z", "strict_t_time");
        assertSameDate("T10:15:30.123Z", "strict_t_time");
        assertSameDate("T10:15:30.123456789Z", "strict_t_time");
        assertSameDate("T10:15:30.1+0100", "strict_t_time");
        assertSameDate("T10:15:30.123+0100", "strict_t_time");
        assertSameDate("T10:15:30.1+01:00", "strict_t_time");
        assertSameDate("T10:15:30.123+01:00", "strict_t_time");
        assertParseException("T1:15:30.123Z", "strict_t_time");
        assertParseException("T10:1:30.123Z", "strict_t_time");
        assertParseException("T10:15:3.123Z", "strict_t_time");
        assertParseException("T10:15:3.1", "strict_t_time");
        assertParseException("T10:15:3Z", "strict_t_time");

        assertSameDate("T10:15:30Z", "strict_t_time_no_millis");
        assertSameDate("T10:15:30+0100", "strict_t_time_no_millis");
        assertSameDate("T10:15:30+01:00", "strict_t_time_no_millis");
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
        assertJavaTimeParseException("2012-W01-8", "strict_week_date");

        assertSameDate("2012-W48-6T10:15:30.1Z", "strict_week_date_time");
        assertSameDate("2012-W48-6T10:15:30.123Z", "strict_week_date_time");
        assertSameDate("2012-W48-6T10:15:30.123456789Z", "strict_week_date_time");
        assertSameDate("2012-W48-6T10:15:30.1+0100", "strict_week_date_time");
        assertSameDate("2012-W48-6T10:15:30.123+0100", "strict_week_date_time");
        assertSameDate("2012-W48-6T10:15:30.1+01:00", "strict_week_date_time");
        assertSameDate("2012-W48-6T10:15:30.123+01:00", "strict_week_date_time");
        assertParseException("2012-W1-6T10:15:30.123Z", "strict_week_date_time");

        assertSameDate("2012-W48-6T10:15:30Z", "strict_week_date_time_no_millis");
        assertSameDate("2012-W48-6T10:15:30+0100", "strict_week_date_time_no_millis");
        assertSameDate("2012-W48-6T10:15:30+01:00", "strict_week_date_time_no_millis");
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

    public void testSamePrinterOutput() {
        int year = randomIntBetween(1970, 2030);
        int month = randomIntBetween(1, 12);
        int day = randomIntBetween(1, 28);
        int hour = randomIntBetween(0, 23);
        int minute = randomIntBetween(0, 59);
        int second = randomIntBetween(0, 59);

        ZonedDateTime javaDate = ZonedDateTime.of(year, month, day, hour, minute, second, 0, ZoneOffset.UTC);
        DateTime jodaDate = new DateTime(year, month, day, hour, minute, second, DateTimeZone.UTC);

        for (FormatNames format : FormatNames.values()) {
            if (format == FormatNames.ISO8601 || format == FormatNames.STRICT_DATE_OPTIONAL_TIME_NANOS) {
                // Nanos aren't supported by joda
                continue;
            }
            assertSamePrinterOutput(format.getName(), javaDate, jodaDate);
        }
    }

    public void testSamePrinterOutputWithTimeZone() {
        String format = "strict_date_optional_time";
        String dateInput = "2017-02-01T08:02:00.000-01:00";
        DateFormatter javaFormatter = DateFormatter.forPattern(format);
        TemporalAccessor javaDate = javaFormatter.parse(dateInput);

        DateFormatter jodaFormatter = Joda.forPattern(format);
        DateTime dateTime = jodaFormatter.parseJoda(dateInput);

        String javaDateString = javaFormatter.withZone(ZoneOffset.ofHours(-1)).format(javaDate);
        String jodaDateString = jodaFormatter.withZone(ZoneOffset.ofHours(-1)).formatJoda(dateTime);
        String message = String.format(Locale.ROOT, "expected string representation to be equal for format [%s]: joda [%s], java [%s]",
            format, jodaDateString, javaDateString);
        assertThat(message, javaDateString, is(jodaDateString));
    }

    public void testDateFormatterWithLocale() {
        Locale locale = randomLocale(random());
        String pattern = randomBoolean() ? "strict_date_optional_time||date_time" : "date_time||strict_date_optional_time";
        DateFormatter formatter = DateFormatter.forPattern(pattern).withLocale(locale);
        assertThat(formatter.pattern(), is(pattern));
        assertThat(formatter.locale(), is(locale));
    }

    public void testSeveralTimeFormats() {
        {
            String format = "year_month_day||ordinal_date";
            DateFormatter jodaFormatter = Joda.forPattern(format);
            DateFormatter javaFormatter = DateFormatter.forPattern(format);
            assertSameDate("2018-12-12", format, jodaFormatter, javaFormatter);
            assertSameDate("2018-128", format, jodaFormatter, javaFormatter);
        }
        {
            String format = "strict_date_optional_time||dd-MM-yyyy";
            DateFormatter jodaFormatter = Joda.forPattern(format);
            DateFormatter javaFormatter = DateFormatter.forPattern(format);
            assertSameDate("31-01-2014", format, jodaFormatter, javaFormatter);
        }
    }

    // the iso 8601 parser is available via Joda.forPattern(), so we have to test this slightly differently
    public void testIso8601Parsers() {
        String format = "iso8601";
        org.joda.time.format.DateTimeFormatter isoFormatter = ISODateTimeFormat.dateTimeParser().withZone(DateTimeZone.UTC);
        JodaDateFormatter jodaFormatter = new JodaDateFormatter(format, isoFormatter, isoFormatter);
        DateFormatter javaFormatter = DateFormatter.forPattern(format);

        assertSameDate("2018-10-10", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10+0430", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11-08:00", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11Z", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11:12", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11:12+0100", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11:12.123", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11:12.123Z", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11:12.123+0000", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11:12,123", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11:12,123Z", format, jodaFormatter, javaFormatter);
        assertSameDate("2018-10-10T10:11:12,123+05:30", format, jodaFormatter, javaFormatter);
    }

    public void testParsingLocalDateFromYearOfEra(){
        //with strict resolving, YearOfEra expect an era, otherwise it won't resolve to a date
        assertSameDate("2018363","yyyyDDD",Joda.forPattern("YYYYDDD"),DateFormatter.forPattern("uuuuDDD"));
    }
    public void testParsingMissingTimezone() {
        long millisJava = DateFormatter.forPattern("8yyyy-MM-dd HH:mm:ss").parseMillis("2018-02-18 17:47:17");
        long millisJoda = DateFormatter.forPattern("yyyy-MM-dd HH:mm:ss").parseMillis("2018-02-18 17:47:17");
        assertThat(millisJava, is(millisJoda));
    }

    private void assertSamePrinterOutput(String format, ZonedDateTime javaDate, DateTime jodaDate) {
        DateFormatter dateFormatter = DateFormatter.forPattern(format);
        JodaDateFormatter jodaDateFormatter = Joda.forPattern(format);

        assertSamePrinterOutput(format, javaDate, jodaDate, dateFormatter, jodaDateFormatter);
    }

    private void assertSamePrinterOutput(String format, ZonedDateTime javaDate, DateTime jodaDate, Locale locale) {
        DateFormatter dateFormatter = DateFormatter.forPattern(format).withLocale(locale);
        DateFormatter jodaDateFormatter = Joda.forPattern(format).withLocale(locale);

        assertSamePrinterOutput(format, javaDate, jodaDate, dateFormatter, jodaDateFormatter);
    }

    private void assertSamePrinterOutput(String format,
                                         ZonedDateTime javaDate,
                                         DateTime jodaDate,
                                         DateFormatter dateFormatter,
                                         DateFormatter jodaDateFormatter) {
        String javaTimeOut = dateFormatter.format(javaDate);
        String jodaTimeOut = jodaDateFormatter.formatJoda(jodaDate);

        assertThat(jodaDate.getMillis(), is(javaDate.toInstant().toEpochMilli()));

        if (JavaVersion.current().getVersion().get(0) == 8 && javaTimeOut.endsWith(".0")
            && (format.equals("epoch_second") || format.equals("epoch_millis"))) {
            // java 8 has a bug in DateTimeFormatter usage when printing dates that rely on isSupportedBy for fields, which is
            // what we use for epoch time. This change accounts for that bug. It should be removed when java 8 support is removed
            jodaTimeOut += ".0";
        }
        String message = String.format(Locale.ROOT, "expected string representation to be equal for format [%s]: joda [%s], java [%s]",
                format, jodaTimeOut, javaTimeOut);
        assertThat(message, javaTimeOut, is(jodaTimeOut));
    }

    private void assertSameDate(String input, String format) {
        DateFormatter jodaFormatter = Joda.forPattern(format);
        DateFormatter javaFormatter = DateFormatter.forPattern(format);
        assertSameDate(input, format, jodaFormatter, javaFormatter);
    }

    private void assertSameDate(String input, String format, Locale locale) {
        DateFormatter jodaFormatter = Joda.forPattern(format).withLocale(locale);
        DateFormatter javaFormatter = DateFormatter.forPattern(format).withLocale(locale);
        assertSameDate(input, format, jodaFormatter, javaFormatter);
    }

    private void assertSameDate(String input, String format, DateFormatter jodaFormatter, DateFormatter javaFormatter) {
        DateTime jodaDateTime = jodaFormatter.parseJoda(input);

        TemporalAccessor javaTimeAccessor = javaFormatter.parse(input);
        ZonedDateTime zonedDateTime = DateFormatters.from(javaTimeAccessor);

        String msg = String.format(Locale.ROOT, "Input [%s] Format [%s] Joda [%s], Java [%s]", input, format, jodaDateTime,
            DateTimeFormatter.ISO_INSTANT.format(zonedDateTime.toInstant()));

        assertThat(msg, jodaDateTime.getMillis(), is(zonedDateTime.toInstant().toEpochMilli()));
    }

    private void assertParseException(String input, String format) {
        assertJodaParseException(input, format, "Invalid format: \"" + input);
        assertJavaTimeParseException(input, format);
    }

    private void assertJodaParseException(String input, String format, String expectedMessage) {
        DateFormatter jodaFormatter = Joda.forPattern(format);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> jodaFormatter.parseJoda(input));
        assertThat(e.getMessage(), containsString(expectedMessage));
    }

    private void assertJavaTimeParseException(String input, String format) {
        DateFormatter javaTimeFormatter = DateFormatter.forPattern(format);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> javaTimeFormatter.parse(input));
        assertThat(e.getMessage(), containsString(input));
        assertThat(e.getMessage(), containsString(format));
    }

    private void assertDateMathEquals(String text, String pattern) {
        long gotMillisJava = dateMathToMillis(text, DateFormatter.forPattern(pattern));
        long gotMillisJoda = dateMathToMillis(text, Joda.forPattern(pattern));

        assertEquals(gotMillisJoda, gotMillisJava);
    }

    private void assertSameDateAs(String input, String javaPattern, String jodaPattern) {
        DateFormatter javaFormatter = DateFormatter.forPattern(javaPattern);
        DateFormatter jodaFormatter = Joda.forPattern(jodaPattern);
        assertSameDate(input, javaPattern, jodaFormatter, javaFormatter);
    }
}
