/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.DateMathExpressionResolver;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DateMathExpressionResolverTests extends ESTestCase {

    private final long now = randomMillisUpToYear9999();
    private final LongSupplier getTime = () -> now;

    public void testNoDateMathExpression() {
        String expression = randomAlphaOfLength(10);
        assertThat(DateMathExpressionResolver.resolveExpression(expression, getTime), equalTo(expression));

        expression = "*";
        assertThat(DateMathExpressionResolver.resolveExpression(expression, getTime), equalTo(expression));
    }

    public void testExpression() {
        String result = DateMathExpressionResolver.resolveExpression("<.marvel-{now}>", getTime);
        assertThat(result, equalTo(".marvel-" + formatDate("uuuu.MM.dd", dateFromMillis(now))));

        result = DateMathExpressionResolver.resolveExpression("<.watch_history-{now}>", getTime);
        assertThat(result, equalTo(".watch_history-" + formatDate("uuuu.MM.dd", dateFromMillis(now))));

        result = DateMathExpressionResolver.resolveExpression("<logstash-{now}>", getTime);
        assertThat(result, equalTo("logstash-" + formatDate("uuuu.MM.dd", dateFromMillis(now))));
    }

    public void testExpressionWithWildcardAndExclusions() {
        String result = DateMathExpressionResolver.resolveExpression("<-before-inner-{now}>", getTime);
        assertThat(result, equalTo("-before-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(now))));

        result = DateMathExpressionResolver.resolveExpression("<wild*card-{now}*>", getTime);
        assertThat(result, equalTo("wild*card-" + formatDate("uuuu.MM.dd", dateFromMillis(now)) + "*"));

        result = DateMathExpressionResolver.resolveExpression("<-after-inner-{now}>", getTime);
        assertThat(result, equalTo("-after-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(now))));

    }

    public void testExpression_Static() {
        String result = DateMathExpressionResolver.resolveExpression("<.marvel-test>", getTime);
        assertThat(result, equalTo(".marvel-test"));
    }

    public void testExpression_MultiParts() {
        String result = DateMathExpressionResolver.resolveExpression("<.text1-{now/d}-text2-{now/M}>", getTime);
        assertThat(
            result,
            equalTo(
                ".text1-"
                    + formatDate("uuuu.MM.dd", dateFromMillis(now))
                    + "-text2-"
                    + formatDate("uuuu.MM.dd", dateFromMillis(now).withDayOfMonth(1))
            )
        );
    }

    public void testExpression_CustomFormat() {
        String result = DateMathExpressionResolver.resolveExpression("<.marvel-{now/d{yyyy.MM.dd}}>", getTime);
        assertThat(result, equalTo(".marvel-" + formatDate("uuuu.MM.dd", dateFromMillis(now))));
    }

    public void testExpression_EscapeStatic() {
        String result = DateMathExpressionResolver.resolveExpression("<.mar\\{v\\}el-{now/d}>", getTime);
        assertThat(result, equalTo(".mar{v}el-" + formatDate("uuuu.MM.dd", dateFromMillis(now))));
    }

    public void testExpression_EscapeDateFormat() {
        String result = DateMathExpressionResolver.resolveExpression("<.marvel-{now/d{'\\{year\\}'yyyy}}>", getTime);
        assertThat(result, equalTo(".marvel-" + formatDate("'{year}'yyyy", dateFromMillis(now))));
    }

    public void testExpression_CustomTimeZoneInIndexName() {
        ZoneId timeZone;
        int hoursOffset;
        int minutesOffset = 0;
        if (randomBoolean()) {
            hoursOffset = randomIntBetween(-12, 14);
            timeZone = ZoneOffset.ofHours(hoursOffset);
        } else {
            hoursOffset = randomIntBetween(-11, 13);
            minutesOffset = randomIntBetween(0, 59);
            if (hoursOffset < 0) {
                minutesOffset = -minutesOffset;
            }
            timeZone = ZoneOffset.ofHoursMinutes(hoursOffset, minutesOffset);
        }
        ZonedDateTime now;
        if (hoursOffset >= 0) {
            // rounding to next day 00:00
            now = ZonedDateTime.now(ZoneOffset.UTC)
                .plusHours(hoursOffset)
                .plusMinutes(minutesOffset)
                .withHour(0)
                .withMinute(0)
                .withSecond(0);
        } else {
            // rounding to today 00:00
            now = ZonedDateTime.now(ZoneOffset.UTC).withHour(0).withMinute(0).withSecond(0);
        }

        String result = DateMathExpressionResolver.resolveExpression(
            "<.marvel-{now/d{yyyy.MM.dd|" + timeZone.getId() + "}}>",
            () -> now.toInstant().toEpochMilli()
        );
        logger.info("timezone: [{}], now [{}], name: [{}]", timeZone, now, result);
        assertThat(result, equalTo(".marvel-" + formatDate("uuuu.MM.dd", now.withZoneSameInstant(timeZone))));
    }

    public void testExpressionInvalidUnescaped() {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolveExpression("<.mar}vel-{now/d}>", getTime)
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("invalid character at position ["));
    }

    public void testExpressionInvalidDateMathFormat() {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolveExpression("<.marvel-{now/d{}>", getTime)
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

    public void testExpressionInvalidEmptyDateMathFormat() {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolveExpression("<.marvel-{now/d{}}>", getTime)
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("missing date format"));
    }

    public void testExpressionInvalidOpenEnded() {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolveExpression("<.marvel-{now/d>", getTime)
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

    static ZonedDateTime dateFromMillis(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    static String formatDate(String pattern, ZonedDateTime zonedDateTime) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);
        return dateFormatter.format(zonedDateTime);
    }
}
