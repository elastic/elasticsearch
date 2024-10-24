/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.DateMathExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;

public class DateMathExpressionResolverTests extends ESTestCase {

    private final Context context = new Context(
        ClusterState.builder(new ClusterName("_name")).build(),
        IndicesOptions.strictExpand(),
        SystemIndexAccessLevel.NONE
    );

    private static ZonedDateTime dateFromMillis(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    private static String formatDate(String pattern, ZonedDateTime zonedDateTime) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);
        return dateFormatter.format(zonedDateTime);
    }

    public void testNormal() throws Exception {
        int numIndexExpressions = randomIntBetween(1, 9);
        List<String> indexExpressions = new ArrayList<>(numIndexExpressions);
        for (int i = 0; i < numIndexExpressions; i++) {
            indexExpressions.add(randomAlphaOfLength(10));
        }
        String[] result = DateMathExpressionResolver.resolve(context, indexExpressions.toArray(Strings.EMPTY_ARRAY));
        assertThat(result.length, equalTo(indexExpressions.size()));
        for (int i = 0; i < indexExpressions.size(); i++) {
            assertThat(result[i], equalTo(indexExpressions.get(i)));
        }
    }

    public void testExpression() throws Exception {
        List<String> indexExpressions = Arrays.asList("<.marvel-{now}>", "<.watch_history-{now}>", "<logstash-{now}>");
        String[] result = DateMathExpressionResolver.resolve(context, indexExpressions.toArray(Strings.EMPTY_ARRAY));
        assertThat(result.length, equalTo(3));
        assertThat(result[0], equalTo(".marvel-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
        assertThat(result[1], equalTo(".watch_history-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
        assertThat(result[2], equalTo("logstash-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
    }

    public void testExpressionWithWildcardAndExclusions() {
        String[] indexExpressions = new String[] {
            "<-before-inner-{now}>",
            "-<before-outer-{now}>",
            "<wild*card-{now}*>",
            "<-after-inner-{now}>",
            "-<after-outer-{now}>" };
        String[] result = DateMathExpressionResolver.resolve(context, indexExpressions);
        assertArrayEquals(
            new String[] {
                "-before-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())),
                "-<before-outer-{now}>", // doesn't evaluate because it doesn't start with "<" and it is not an exclusion
                "wild*card-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())) + "*",
                "-after-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())),
                "-after-outer-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())) },
            result
        );
        Context noWildcardExpandContext = new Context(
            ClusterState.builder(new ClusterName("_name")).build(),
            IndicesOptions.strictSingleIndexNoExpandForbidClosed(),
            SystemIndexAccessLevel.NONE
        );
        result = DateMathExpressionResolver.resolve(noWildcardExpandContext, indexExpressions);
        assertArrayEquals(
            result,
            new String[] {
                "-before-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())),
                // doesn't evaluate because it doesn't start with "<" and there can't be exclusions without wildcard expansion
                "-<before-outer-{now}>",
                "wild*card-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())) + "*",
                "-after-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())),
                // doesn't evaluate because it doesn't start with "<" and there can't be exclusions without wildcard expansion
                "-<after-outer-{now}>" }
        );
    }

    public void testEmpty() throws Exception {
        String[] result = DateMathExpressionResolver.resolve(context, Strings.EMPTY_ARRAY);
        assertThat(result, emptyArray());
    }

    public void testExpression_Static() throws Exception {
        String[] result = DateMathExpressionResolver.resolve(context, "<.marvel-test>");
        assertThat(result.length, equalTo(1));
        assertThat(result[0], equalTo(".marvel-test"));
    }

    public void testExpression_MultiParts() throws Exception {
        String[] result = DateMathExpressionResolver.resolve(context, "<.text1-{now/d}-text2-{now/M}>");
        assertThat(result.length, equalTo(1));
        assertThat(
            result[0],
            equalTo(
                ".text1-"
                    + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))
                    + "-text2-"
                    + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()).withDayOfMonth(1))
            )
        );
    }

    public void testExpression_CustomFormat() throws Exception {
        String[] results = DateMathExpressionResolver.resolve(context, "<.marvel-{now/d{yyyy.MM.dd}}>");
        assertThat(results.length, equalTo(1));
        assertThat(results[0], equalTo(".marvel-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
    }

    public void testExpression_EscapeStatic() throws Exception {
        String[] result = DateMathExpressionResolver.resolve(context, "<.mar\\{v\\}el-{now/d}>");
        assertThat(result.length, equalTo(1));
        assertThat(result[0], equalTo(".mar{v}el-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
    }

    public void testExpression_EscapeDateFormat() throws Exception {
        String[] result = DateMathExpressionResolver.resolve(context, "<.marvel-{now/d{'\\{year\\}'yyyy}}>");
        assertThat(result.length, equalTo(1));
        assertThat(result[0], equalTo(".marvel-" + formatDate("'{year}'yyyy", dateFromMillis(context.getStartTime()))));
    }

    public void testExpression_MixedArray() throws Exception {
        String[] result = DateMathExpressionResolver.resolve(
            context,
            "name1",
            "<.marvel-{now/d}>",
            "name2",
            "<.logstash-{now/M{uuuu.MM}}>"
        );
        assertThat(result.length, equalTo(4));
        assertThat(result[0], equalTo("name1"));
        assertThat(result[1], equalTo(".marvel-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
        assertThat(result[2], equalTo("name2"));
        assertThat(result[3], equalTo(".logstash-" + formatDate("uuuu.MM", dateFromMillis(context.getStartTime()).withDayOfMonth(1))));
    }

    public void testExpression_CustomTimeZoneInIndexName() throws Exception {
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
        Context context = new Context(
            this.context.getState(),
            this.context.getOptions(),
            now.toInstant().toEpochMilli(),
            SystemIndexAccessLevel.NONE,
            name -> false,
            name -> false
        );
        String[] results = DateMathExpressionResolver.resolve(context, "<.marvel-{now/d{yyyy.MM.dd|" + timeZone.getId() + "}}>");
        assertThat(results.length, equalTo(1));
        logger.info("timezone: [{}], now [{}], name: [{}]", timeZone, now, results[0]);
        assertThat(results[0], equalTo(".marvel-" + formatDate("uuuu.MM.dd", now.withZoneSameInstant(timeZone))));
    }

    public void testExpressionInvalidUnescaped() throws Exception {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolve(context, "<.mar}vel-{now/d}>")
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("invalid character at position ["));
    }

    public void testExpressionInvalidDateMathFormat() throws Exception {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolve(context, "<.marvel-{now/d{}>")
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

    public void testExpressionInvalidEmptyDateMathFormat() throws Exception {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolve(context, "<.marvel-{now/d{}}>")
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("missing date format"));
    }

    public void testExpressionInvalidOpenEnded() throws Exception {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolve(context, "<.marvel-{now/d>")
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

}
