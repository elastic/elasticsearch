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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.joda.time.DateTimeZone.UTC;

public class DateMathExpressionResolverTests extends ESTestCase {

    private final DateMathExpressionResolver expressionResolver = new DateMathExpressionResolver();
    private final Context context = new Context(
        ClusterState.builder(new ClusterName("_name")).build(), IndicesOptions.strictExpand(),
        SystemIndexAccessLevel.NONE
    );

    public void testNormal() throws Exception {
        int numIndexExpressions = randomIntBetween(1, 9);
        List<String> indexExpressions = new ArrayList<>(numIndexExpressions);
        for (int i = 0; i < numIndexExpressions; i++) {
            indexExpressions.add(randomAlphaOfLength(10));
        }
        List<String> result = expressionResolver.resolve(context, indexExpressions);
        assertThat(result.size(), equalTo(indexExpressions.size()));
        for (int i = 0; i < indexExpressions.size(); i++) {
            assertThat(result.get(i), equalTo(indexExpressions.get(i)));
        }
    }

    public void testExpression() throws Exception {
        List<String> indexExpressions = Arrays.asList("<.marvel-{now}>", "<.watch_history-{now}>", "<logstash-{now}>");
        List<String> result = expressionResolver.resolve(context, indexExpressions);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(0),
            equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
        assertThat(result.get(1),
            equalTo(".watch_history-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
        assertThat(result.get(2),
            equalTo("logstash-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testEmpty() throws Exception {
        List<String> result = expressionResolver.resolve(context, Collections.<String>emptyList());
        assertThat(result.size(), equalTo(0));
    }

    public void testExpression_Static() throws Exception {
        List<String> result = expressionResolver.resolve(context, Arrays.asList("<.marvel-test>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), equalTo(".marvel-test"));
    }

    public void testExpression_MultiParts() throws Exception {
        List<String> result = expressionResolver.resolve(context, Arrays.asList("<.text1-{now/d}-text2-{now/M}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), equalTo(".text1-"
                + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))
                + "-text2-"
                + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC).withDayOfMonth(1))));
    }

    public void testExpression_CustomFormat() throws Exception {
        List<String> results = expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{yyyy.MM.dd}}>"));
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0),
            equalTo(".marvel-" + DateTimeFormat.forPattern("yyyy.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testExpression_EscapeStatic() throws Exception {
        List<String> result = expressionResolver.resolve(context, Arrays.asList("<.mar\\{v\\}el-{now/d}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0),
            equalTo(".mar{v}el-" + DateTimeFormat.forPattern("yyyy.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testExpression_EscapeDateFormat() throws Exception {
        List<String> result = expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{'\\{year\\}'yyyy}}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0),
            equalTo(".marvel-" + DateTimeFormat.forPattern("'{year}'yyyy").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testExpression_MixedArray() throws Exception {
        List<String> result = expressionResolver.resolve(context, Arrays.asList(
                "name1", "<.marvel-{now/d}>", "name2", "<.logstash-{now/M{uuuu.MM}}>"
        ));
        assertThat(result.size(), equalTo(4));
        assertThat(result.get(0), equalTo("name1"));
        assertThat(result.get(1),
            equalTo(".marvel-" + DateTimeFormat.forPattern("yyyy.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
        assertThat(result.get(2), equalTo("name2"));
        assertThat(result.get(3), equalTo(".logstash-" +
            DateTimeFormat.forPattern("yyyy.MM").print(new DateTime(context.getStartTime(), UTC).withDayOfMonth(1))));
    }

    public void testExpression_CustomTimeZoneInIndexName() throws Exception {
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
            now = DateTime.now(UTC).plusHours(hoursOffset).plusMinutes(minutesOffset)
                .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        } else {
            // rounding to today 00:00
            now = DateTime.now(UTC).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        }
        Context context = new Context(this.context.getState(), this.context.getOptions(), now.getMillis(),
            SystemIndexAccessLevel.NONE, name -> false, name -> false);
        List<String> results = expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{yyyy.MM.dd|" + timeZone.getID() + "}}>"));
        assertThat(results.size(), equalTo(1));
        logger.info("timezone: [{}], now [{}], name: [{}]", timeZone, now, results.get(0));
        assertThat(results.get(0), equalTo(".marvel-" + DateTimeFormat.forPattern("yyyy.MM.dd").print(now.withZone(timeZone))));
    }

    public void testExpressionInvalidUnescaped() throws Exception {
        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> expressionResolver.resolve(context, Arrays.asList("<.mar}vel-{now/d}>")));
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("invalid character at position ["));
    }

    public void testExpressionInvalidDateMathFormat() throws Exception {
        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{}>")));
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

    public void testExpressionInvalidEmptyDateMathFormat() throws Exception {
        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{}}>")));
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("missing date format"));
    }

    public void testExpressionInvalidOpenEnded() throws Exception {
        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d>")));
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

}
