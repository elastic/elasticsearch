/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.DateMathExpressionResolver;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
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
        List<String> result = DateMathExpressionResolver.resolve(context, indexExpressions);
        assertThat(result.size(), equalTo(indexExpressions.size()));
        for (int i = 0; i < indexExpressions.size(); i++) {
            assertThat(result.get(i), equalTo(indexExpressions.get(i)));
        }
    }

    public void testExpression() throws Exception {
        List<String> indexExpressions = Arrays.asList("<.marvel-{now}>", "<.watch_history-{now}>", "<logstash-{now}>");
        List<String> result = DateMathExpressionResolver.resolve(context, indexExpressions);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(0), equalTo(".marvel-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
        assertThat(result.get(1), equalTo(".watch_history-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
        assertThat(result.get(2), equalTo("logstash-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
    }

    public void testExpressionWithWildcardAndExclusions() {
        List<String> indexExpressions = Arrays.asList(
            "<-before-inner-{now}>",
            "-<before-outer-{now}>",
            "<wild*card-{now}*>",
            "<-after-inner-{now}>",
            "-<after-outer-{now}>"
        );
        List<String> result = DateMathExpressionResolver.resolve(context, indexExpressions);
        assertThat(
            result,
            Matchers.contains(
                equalTo("-before-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))),
                equalTo("-<before-outer-{now}>"), // doesn't evaluate because it doesn't start with "<" and it is not an exclusion
                equalTo("wild*card-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())) + "*"),
                equalTo("-after-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))),
                equalTo("-after-outer-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())))
            )
        );
        Context noWildcardExpandContext = new Context(
            ClusterState.builder(new ClusterName("_name")).build(),
            IndicesOptions.strictSingleIndexNoExpandForbidClosed(),
            SystemIndexAccessLevel.NONE
        );
        result = DateMathExpressionResolver.resolve(noWildcardExpandContext, indexExpressions);
        assertThat(
            result,
            Matchers.contains(
                equalTo("-before-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))),
                // doesn't evaluate because it doesn't start with "<" and there can't be exclusions without wildcard expansion
                equalTo("-<before-outer-{now}>"),
                equalTo("wild*card-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime())) + "*"),
                equalTo("-after-inner-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))),
                // doesn't evaluate because it doesn't start with "<" and there can't be exclusions without wildcard expansion
                equalTo("-<after-outer-{now}>")
            )
        );
    }

    public void testEmpty() throws Exception {
        List<String> result = DateMathExpressionResolver.resolve(context, Collections.<String>emptyList());
        assertThat(result.size(), equalTo(0));
    }

    public void testExpression_Static() throws Exception {
        List<String> result = DateMathExpressionResolver.resolve(context, Arrays.asList("<.marvel-test>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), equalTo(".marvel-test"));
    }

    public void testExpression_MultiParts() throws Exception {
        List<String> result = DateMathExpressionResolver.resolve(context, Arrays.asList("<.text1-{now/d}-text2-{now/M}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(
            result.get(0),
            equalTo(
                ".text1-"
                    + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))
                    + "-text2-"
                    + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()).withDayOfMonth(1))
            )
        );
    }

    public void testExpression_CustomFormat() throws Exception {
        List<String> results = DateMathExpressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{yyyy.MM.dd}}>"));
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0), equalTo(".marvel-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
    }

    public void testExpression_EscapeStatic() throws Exception {
        List<String> result = DateMathExpressionResolver.resolve(context, Arrays.asList("<.mar\\{v\\}el-{now/d}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), equalTo(".mar{v}el-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
    }

    public void testExpression_EscapeDateFormat() throws Exception {
        List<String> result = DateMathExpressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{'\\{year\\}'yyyy}}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), equalTo(".marvel-" + formatDate("'{year}'yyyy", dateFromMillis(context.getStartTime()))));
    }

    public void testExpression_MixedArray() throws Exception {
        List<String> result = DateMathExpressionResolver.resolve(
            context,
            Arrays.asList("name1", "<.marvel-{now/d}>", "name2", "<.logstash-{now/M{uuuu.MM}}>")
        );
        assertThat(result.size(), equalTo(4));
        assertThat(result.get(0), equalTo("name1"));
        assertThat(result.get(1), equalTo(".marvel-" + formatDate("uuuu.MM.dd", dateFromMillis(context.getStartTime()))));
        assertThat(result.get(2), equalTo("name2"));
        assertThat(result.get(3), equalTo(".logstash-" + formatDate("uuuu.MM", dateFromMillis(context.getStartTime()).withDayOfMonth(1))));
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
        List<String> results = DateMathExpressionResolver.resolve(
            context,
            Arrays.asList("<.marvel-{now/d{yyyy.MM.dd|" + timeZone.getId() + "}}>")
        );
        assertThat(results.size(), equalTo(1));
        logger.info("timezone: [{}], now [{}], name: [{}]", timeZone, now, results.get(0));
        assertThat(results.get(0), equalTo(".marvel-" + formatDate("uuuu.MM.dd", now.withZoneSameInstant(timeZone))));
    }

    public void testExpressionInvalidUnescaped() throws Exception {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolve(context, Arrays.asList("<.mar}vel-{now/d}>"))
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("invalid character at position ["));
    }

    public void testExpressionInvalidDateMathFormat() throws Exception {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{}>"))
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

    public void testExpressionInvalidEmptyDateMathFormat() throws Exception {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{}}>"))
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("missing date format"));
    }

    public void testExpressionInvalidOpenEnded() throws Exception {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> DateMathExpressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d>"))
        );
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

}
