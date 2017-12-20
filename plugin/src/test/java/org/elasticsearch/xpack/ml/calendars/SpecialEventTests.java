/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.calendars;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.ml.job.config.Connective;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.Operator;
import org.elasticsearch.xpack.ml.job.config.RuleAction;
import org.elasticsearch.xpack.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.ml.job.config.RuleConditionType;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class SpecialEventTests extends AbstractSerializingTestCase<SpecialEvent> {

    public static SpecialEvent createSpecialEvent(String calendarId) {
        ZonedDateTime start = ZonedDateTime.ofInstant(Instant.ofEpochMilli(new DateTime(randomDateTimeZone()).getMillis()), ZoneOffset.UTC);
        return new SpecialEvent(randomAlphaOfLength(10), start, start.plusSeconds(randomIntBetween(1, 10000)),
                calendarId);
    }

    @Override
    protected SpecialEvent createTestInstance() {
        return createSpecialEvent(randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected Writeable.Reader<SpecialEvent> instanceReader() {
        return SpecialEvent::new;
    }

    @Override
    protected SpecialEvent doParseInstance(XContentParser parser) throws IOException {
        return SpecialEvent.PARSER.apply(parser, null).build();
    }

    public void testToDetectionRule() {
        long bucketSpanSecs = 300;
        SpecialEvent event = createTestInstance();
        DetectionRule rule = event.toDetectionRule(TimeValue.timeValueSeconds(bucketSpanSecs));

        assertEquals(Connective.AND, rule.getConditionsConnective());
        assertEquals(rule.getActions(), EnumSet.of(RuleAction.FILTER_RESULTS, RuleAction.SKIP_SAMPLING));
        assertNull(rule.getTargetFieldName());
        assertNull(rule.getTargetFieldValue());

        List<RuleCondition> conditions = rule.getConditions();
        assertEquals(2, conditions.size());
        assertEquals(RuleConditionType.TIME, conditions.get(0).getType());
        assertEquals(RuleConditionType.TIME, conditions.get(1).getType());
        assertEquals(Operator.GTE, conditions.get(0).getCondition().getOperator());
        assertEquals(Operator.LT, conditions.get(1).getCondition().getOperator());

        // Check times are aligned with the bucket
        long conditionStartTime = Long.parseLong(conditions.get(0).getCondition().getValue());
        assertEquals(0, conditionStartTime % bucketSpanSecs);
        long bucketCount = conditionStartTime / bucketSpanSecs;
        assertEquals(bucketSpanSecs * bucketCount, conditionStartTime);

        long conditionEndTime = Long.parseLong(conditions.get(1).getCondition().getValue());
        assertEquals(0, conditionEndTime % bucketSpanSecs);

        long eventTime = event.getEndTime().toEpochSecond() - conditionStartTime;
        long numbBucketsInEvent = (eventTime + bucketSpanSecs -1) / bucketSpanSecs;
        assertEquals(bucketSpanSecs * (bucketCount + numbBucketsInEvent), conditionEndTime);
    }

    public void testBuild() {
        SpecialEvent.Builder builder = new SpecialEvent.Builder();

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [description] cannot be null", e.getMessage());
        builder.description("foo");
        e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [start_time] cannot be null", e.getMessage());
        ZonedDateTime now = ZonedDateTime.now();
        builder.startTime(now);
        e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [end_time] cannot be null", e.getMessage());
        builder.endTime(now.plusHours(1));
        e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [calendar_id] cannot be null", e.getMessage());
        builder.calendarId("foo");
        builder.build();


        builder = new SpecialEvent.Builder().description("f").calendarId("c");
        builder.startTime(now);
        builder.endTime(now.minusHours(2));

        e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertThat(e.getMessage(), containsString("must come before end time"));
    }
}