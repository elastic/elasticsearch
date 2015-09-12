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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.joda.time.DateTimeZone.UTC;

public class DateMathExpressionResolverTests extends ESTestCase {

    private final DateMathExpressionResolver expressionResolver = new DateMathExpressionResolver(Settings.EMPTY);
    private final Context context = new Context(
            ClusterState.builder(new ClusterName("_name")).build(), IndicesOptions.strictExpand()
    );

    public void testNormal() throws Exception {
        int numIndexExpressions = randomIntBetween(1, 9);
        List<String> indexExpressions = new ArrayList<>(numIndexExpressions);
        for (int i = 0; i < numIndexExpressions; i++) {
            indexExpressions.add(randomAsciiOfLength(10));
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
        assertThat(result.get(0), equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
        assertThat(result.get(1), equalTo(".watch_history-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
        assertThat(result.get(2), equalTo("logstash-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
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
        List<String> results = expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{YYYY.MM.dd}}>"));
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0), equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testExpression_EscapeStatic() throws Exception {
        List<String> result = expressionResolver.resolve(context, Arrays.asList("<.mar\\{v\\}el-{now/d}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), equalTo(".mar{v}el-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testExpression_EscapeDateFormat() throws Exception {
        List<String> result = expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{'\\{year\\}'YYYY}}>"));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), equalTo(".marvel-" + DateTimeFormat.forPattern("'{year}'YYYY").print(new DateTime(context.getStartTime(), UTC))));
    }

    public void testExpression_MixedArray() throws Exception {
        List<String> result = expressionResolver.resolve(context, Arrays.asList(
                "name1", "<.marvel-{now/d}>", "name2", "<.logstash-{now/M{YYYY.MM}}>"
        ));
        assertThat(result.size(), equalTo(4));
        assertThat(result.get(0), equalTo("name1"));
        assertThat(result.get(1), equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(new DateTime(context.getStartTime(), UTC))));
        assertThat(result.get(2), equalTo("name2"));
        assertThat(result.get(3), equalTo(".logstash-" + DateTimeFormat.forPattern("YYYY.MM").print(new DateTime(context.getStartTime(), UTC).withDayOfMonth(1))));
    }

    public void testExpression_CustomTimeZoneInSetting() throws Exception {
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
            now = DateTime.now(UTC).plusHours(hoursOffset).plusMinutes(minutesOffset).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        } else {
            // rounding to today 00:00
            now = DateTime.now(UTC).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        }
        Settings settings = Settings.builder()
                .put("date_math_expression_resolver.default_time_zone", timeZone.getID())
                .build();
        DateMathExpressionResolver expressionResolver = new DateMathExpressionResolver(settings);
        Context context = new Context(this.context.getState(), this.context.getOptions(), now.getMillis());
        List<String> results = expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{YYYY.MM.dd}}>"));
        assertThat(results.size(), equalTo(1));
        logger.info("timezone: [{}], now [{}], name: [{}]", timeZone, now, results.get(0));
        assertThat(results.get(0), equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.withZone(timeZone))));
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
            now = DateTime.now(UTC).plusHours(hoursOffset).plusMinutes(minutesOffset).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        } else {
            // rounding to today 00:00
            now = DateTime.now(UTC).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
        }
        Context context = new Context(this.context.getState(), this.context.getOptions(), now.getMillis());
        List<String> results = expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{YYYY.MM.dd|" + timeZone.getID() + "}}>"));
        assertThat(results.size(), equalTo(1));
        logger.info("timezone: [{}], now [{}], name: [{}]", timeZone, now, results.get(0));
        assertThat(results.get(0), equalTo(".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.withZone(timeZone))));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testExpression_Invalid_Unescaped() throws Exception {
        expressionResolver.resolve(context, Arrays.asList("<.mar}vel-{now/d}>"));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testExpression_Invalid_DateMathFormat() throws Exception {
        expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{}>"));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testExpression_Invalid_EmptyDateMathFormat() throws Exception {
        expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d{}}>"));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testExpression_Invalid_OpenEnded() throws Exception {
        expressionResolver.resolve(context, Arrays.asList("<.marvel-{now/d>"));
    }

}
