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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.DateMathExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.joda.time.DateTimeZone.UTC;

public class DateMathExpressionResolverTests extends ESTestCase {

    private final DateMathExpressionResolver expressionResolver = new DateMathExpressionResolver();
    private final Context context = new Context(
            ClusterState.builder(new ClusterName("_name")).build(), IndicesOptions.strictExpand()
    );

    public void testNormal() throws Exception {
        int numIndexExpressions = randomIntBetween(1, 9);
        Set<String> indexExpressions = new LinkedHashSet<>(numIndexExpressions);
        for (int i = 0; i < numIndexExpressions; i++) {
            indexExpressions.add(randomAlphaOfLength(10));
        }
        Set<String> result = expressionResolver.resolve(context, indexExpressions);
        assertThat(result, equalTo(indexExpressions));
    }

    public void testExpression() throws Exception {
        Set<String> indexExpressions = new LinkedHashSet<>(Arrays.asList("<.marvel-{now}>", "<.watch_history-{now}>", "<logstash-{now}>"));
        Set<String> result = expressionResolver.resolve(context, indexExpressions);
        assertThat(result.size(), equalTo(3));
        Iterator<String> resultIter = result.iterator();
        assertThat(resultIter.next(),
            equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
        assertThat(resultIter.next(),
            equalTo(".watch_history-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
        assertThat(resultIter.next(),
            equalTo("logstash-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testEmpty() throws Exception {
        Set<String> result = expressionResolver.resolve(context, Collections.<String>emptySet());
        assertThat(result.size(), equalTo(0));
    }

    public void testExpression_Static() throws Exception {
        Set<String> result = expressionResolver.resolve(context, Collections.singleton("<.marvel-test>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.iterator().next(), equalTo(".marvel-test"));
    }

    public void testExpression_MultiParts() throws Exception {
        Set<String> result = expressionResolver.resolve(context, Collections.singleton("<.text1-{now/d}-text2-{now/M}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.iterator().next(), equalTo(".text1-"
                + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))
                + "-text2-"
                + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC).withDayOfMonth(1))));
    }

    public void testExpression_CustomFormat() throws Exception {
        Set<String> results = expressionResolver.resolve(context, Collections.singleton("<.marvel-{now/d{yyyy.MM.dd}}>"));
        assertThat(results.size(), equalTo(1));
        assertThat(results.iterator().next(),
            equalTo(".marvel-" + DateTimeFormat.forPattern("yyyy.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testExpression_EscapeStatic() throws Exception {
        Set<String> result = expressionResolver.resolve(context, Collections.singleton("<.mar\\{v\\}el-{now/d}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.iterator().next(),
            equalTo(".mar{v}el-" + DateTimeFormat.forPattern("yyyy.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testExpression_EscapeDateFormat() throws Exception {
        Set<String> result = expressionResolver.resolve(context, Collections.singleton("<.marvel-{now/d{'\\{year\\}'yyyy}}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.iterator().next(),
            equalTo(".marvel-" + DateTimeFormat.forPattern("'{year}'yyyy").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testExpression_MixedArray() throws Exception {
        Set<String> result = expressionResolver.resolve(context, new LinkedHashSet<>(Arrays.asList(
                "name1", "<.marvel-{now/d}>", "name2", "<.logstash-{now/M{YYYY.MM}}>"
        )));
        assertThat(result.size(), equalTo(4));
        Iterator<String> resultIter = result.iterator();
        assertThat(resultIter.next(), equalTo("name1"));
        assertThat(resultIter.next(),
            equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
        assertThat(resultIter.next(), equalTo("name2"));
        assertThat(resultIter.next(), equalTo(".logstash-" +
            DateTimeFormat.forPattern("YYYY.MM").print(new DateTime(context.getStartTime(), UTC).withDayOfMonth(1))));
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
        Context context = new Context(this.context.getState(), this.context.getOptions(), now.getMillis());
        Set<String> results = expressionResolver.resolve(context,
            Collections.singleton("<.marvel-{now/d{yyyy.MM.dd|" + timeZone.getID() + "}}>"));
        assertThat(results.size(), equalTo(1));
        logger.info("timezone: [{}], now [{}], name: [{}]", timeZone, now, results.iterator().next());
        assertThat(results.iterator().next(), equalTo(".marvel-" + DateTimeFormat.forPattern("yyyy.MM.dd").print(now.withZone(timeZone))));
    }

    public void testExpressionInvalidUnescaped() throws Exception {
        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> expressionResolver.resolve(context, Collections.singleton("<.mar}vel-{now/d}>")));
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("invalid character at position ["));
    }

    public void testExpressionInvalidDateMathFormat() throws Exception {
        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> expressionResolver.resolve(context, Collections.singleton("<.marvel-{now/d{}>")));
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

    public void testExpressionInvalidEmptyDateMathFormat() throws Exception {
        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> expressionResolver.resolve(context, Collections.singleton("<.marvel-{now/d{}}>")));
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("missing date format"));
    }

    public void testExpressionInvalidOpenEnded() throws Exception {
        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> expressionResolver.resolve(context, Collections.singleton("<.marvel-{now/d>")));
        assertThat(e.getMessage(), containsString("invalid dynamic name expression"));
        assertThat(e.getMessage(), containsString("date math placeholder is open ended"));
    }

}
