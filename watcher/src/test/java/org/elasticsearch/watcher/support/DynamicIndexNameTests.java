/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class DynamicIndexNameTests extends ElasticsearchTestCase {

    @Test
    public void testNormal() throws Exception {
        String indexName = randomAsciiOfLength(10);
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        DynamicIndexName indexNames = parser.parse(indexName);
        String name = indexNames.name(now);
        assertThat(name, equalTo(indexName));
    }

    @Test
    public void testExpression() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        DynamicIndexName indexNames = parser.parse("<.marvel-{now}>");
        String name = indexNames.name(now);
        assertThat(name, equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now)));
    }

    @Test
    public void testNullOrEmpty() throws Exception {
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        DynamicIndexName indexName = parser.parse((String) null);
        assertThat(indexName, nullValue());
        DynamicIndexName[] indexNames = parser.parse(Strings.EMPTY_ARRAY);
        assertThat(indexNames, nullValue());
    }

    @Test
    public void testExpression_Static() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        DynamicIndexName indexNames = parser.parse("<.marvel-test>");
        String name = indexNames.name(now);
        assertThat(name, equalTo(".marvel-test"));
    }

    @Test
    public void testExpression_MultiParts() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        DynamicIndexName indexNames = parser.parse("<.text1-{now/d}-text2-{now/M}>");
        String name = indexNames.name(now);
        assertThat(name, equalTo(".text1-"
                + DateTimeFormat.forPattern("YYYY.MM.dd").print(now)
                + "-text2-"
                + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.withDayOfMonth(1))));
    }

    @Test
    public void testExpression_CustomFormat() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        DynamicIndexName indexNames = parser.parse("<.marvel-{now/d{YYYY.MM.dd}}>");
        String name = indexNames.name(now);
        assertThat(name, equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now)));
    }

    @Test
    public void testExpression_CustomTimeZone() throws Exception {
        DateTimeZone timeZone;
        int hoursOffset;
        int minutesOffset = 0;
        if (randomBoolean()) {
            hoursOffset = randomIntBetween(-12, 14);
            timeZone = DateTimeZone.forOffsetHours(hoursOffset);
        } else {
            hoursOffset = randomIntBetween(-11, 13);
            minutesOffset = randomIntBetween(0, 59);
            timeZone = DateTimeZone.forOffsetHoursMinutes(hoursOffset, minutesOffset);
        }
        DateTime now;
        if (hoursOffset >= 0) {
            // rounding to next day 00:00
            now = DateTime.now(DateTimeZone.UTC).plusHours(hoursOffset).plusMinutes(minutesOffset).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        } else {
            // rounding to today 00:00
            now = DateTime.now(DateTimeZone.UTC).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        }
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", timeZone);
        DynamicIndexName indexNames = parser.parse("<.marvel-{now/d{YYYY.MM.dd}}>");
        String name = indexNames.name(now);
        logger.info("timezone: [{}], now [{}], name: [{}]", timeZone, now, name);
        assertThat(name, equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.withZone(timeZone))));
    }

    @Test
    public void testExpression_CustomTimeZone_OnParse() throws Exception {
        DateTimeZone timeZone;
        int hoursOffset;
        int minutesOffset = 0;
        if (randomBoolean()) {
            hoursOffset = randomIntBetween(-12, 14);
            timeZone = DateTimeZone.forOffsetHours(hoursOffset);
        } else {
            hoursOffset = randomIntBetween(-11, 13);
            minutesOffset = randomIntBetween(0, 59);
            timeZone = DateTimeZone.forOffsetHoursMinutes(hoursOffset, minutesOffset);
        }
        DateTime now;
        if (hoursOffset >= 0) {
            // rounding to next day 00:00
            now = DateTime.now(DateTimeZone.UTC).plusHours(hoursOffset).plusMinutes(minutesOffset).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        } else {
            // rounding to today 00:00
            now = DateTime.now(DateTimeZone.UTC).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        }
        Settings settings = Settings.builder()
                .put("watcher.dynamic_indices.default_date_format", "YYYY.MM.dd")
                .put("watcher.dynamic_indices.time_zone", "-12")
                .put("watcher.foo.dynamic_indices.time_zone", "-12")
                .build();

        DynamicIndexName.Parser parser = new DynamicIndexName.Parser(settings, "watcher.foo");
        DynamicIndexName indexNames = parser.parse("<.marvel-{now/d{YYYY.MM.dd}}>", timeZone);
        String name = indexNames.name(now);
        logger.info("timezone: [{}], now [{}], name: [{}]", timeZone, now, name);
        assertThat(name, equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.withZone(timeZone))));

    }

    @Test
    public void testExpression_EscapeStatic() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        DynamicIndexName indexNames = parser.parse("<.mar\\{v\\}el-{now/d}>");
        String name = indexNames.name(now);
        assertThat(name, equalTo(".mar{v}el-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now)));
    }

    @Test
    public void testExpression_EscapeDateFormat() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        DynamicIndexName indexNames = parser.parse("<.marvel-{now/d{'\\{year\\}'YYYY}}>");
        String name = indexNames.name(now);
        assertThat(name, equalTo(".marvel-" + DateTimeFormat.forPattern("'{year}'YYYY").print(now)));
    }

    @Test
    public void testExpression_MixedArray() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        DynamicIndexName[] indexNames = parser.parse(new String[] {
                "name1",
                "<.marvel-{now/d}>",
                "name2",
                "<.logstash-{now/M{YYYY.MM}}>"
        });
        String[] names = new String[indexNames.length];
        for (int i = 0; i < names.length; i++) {
            names[i] = indexNames[i].name(now);
        }
        assertThat(names.length, is(4));
        assertThat(names, arrayContaining(
                "name1",
                ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now),
                "name2",
                ".logstash-" + DateTimeFormat.forPattern("YYYY.MM").print(now.withDayOfMonth(1))));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testExpression_Invalid_Unescaped() throws Exception {
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        parser.parse("<.mar}vel-{now/d}>");
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testExpression_Invalid_DateMathFormat() throws Exception {
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        parser.parse("<.marvel-{now/d{}>");
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testExpression_Invalid_EmptyDateMathFormat() throws Exception {
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        parser.parse("<.marvel-{now/d{}}>");
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testExpression_Invalid_OpenEnded() throws Exception {
        DynamicIndexName.Parser parser = new DynamicIndexName.Parser("YYYY.MM.dd", DateTimeZone.UTC);
        parser.parse("<.marvel-{now/d>");
    }

    @Test
    public void testDefaultDateFormat_Default() throws Exception {
        String dateFormat = DynamicIndexName.defaultDateFormat(Settings.EMPTY);
        assertThat(dateFormat, is("YYYY.MM.dd"));
    }

    @Test
    public void testDefaultDateFormat() throws Exception {
        Settings settings = Settings.builder()
                .put("watcher.dynamic_indices.default_date_format", "YYYY.MM")
                .build();
        String dateFormat = randomBoolean() ?
                DynamicIndexName.defaultDateFormat(settings) :
                DynamicIndexName.defaultDateFormat(settings, null);
        assertThat(dateFormat, is("YYYY.MM"));
    }

    @Test
    public void testDefaultDateFormat_Component() throws Exception {
        Settings settings = Settings.builder()
                .put("watcher.dynamic_indices.default_date_format", "YYYY.MM")
                .put("watcher.foo.dynamic_indices.default_date_format", "YYY.MM")
                .build();
        String dateFormat = DynamicIndexName.defaultDateFormat(settings, "watcher.foo");
        assertThat(dateFormat, is("YYY.MM"));
    }

    @Test
    public void testTimeZone_Default() throws Exception {
        DateTimeZone timeZone = DynamicIndexName.timeZone(Settings.EMPTY);
        assertThat(timeZone, is(DateTimeZone.UTC));
    }

    @Test
    public void testTimeZone() throws Exception {
        DateTimeZone timeZone = DateTimeZone.forOffsetHours(randomIntBetween(-12, 14));
        Settings settings = Settings.builder()
                .put("watcher.dynamic_indices.time_zone", timeZone)
                .build();
        DateTimeZone resolvedTimeZone = randomBoolean() ?
                DynamicIndexName.timeZone(settings) :
                DynamicIndexName.timeZone(settings, null);
        assertThat(timeZone, is(resolvedTimeZone));
    }

    @Test
    public void testTimeZone_Component() throws Exception {
        DateTimeZone timeZone = DateTimeZone.forOffsetHours(randomIntBetween(-11, 14));
        Settings settings = Settings.builder()
                .put("watcher.dynamic_indices.time_zone", "-12")
                .put("watcher.foo.dynamic_indices.time_zone", timeZone)
                .build();
        DateTimeZone resolvedTimeZone = DynamicIndexName.timeZone(settings, "watcher.foo");
        assertThat(resolvedTimeZone, is(timeZone));
    }

}
