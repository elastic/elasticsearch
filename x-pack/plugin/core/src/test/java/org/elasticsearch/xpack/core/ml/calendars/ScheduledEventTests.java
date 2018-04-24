/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.calendars;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Connective;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleAction;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.config.RuleConditionType;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ScheduledEventTests extends AbstractSerializingTestCase<ScheduledEvent> {

    public static ScheduledEvent createScheduledEvent(String calendarId) {
        ZonedDateTime start = ZonedDateTime.ofInstant(Instant.ofEpochMilli(new DateTime(randomDateTimeZone()).getMillis()), ZoneOffset.UTC);
        return new ScheduledEvent(randomAlphaOfLength(10), start, start.plusSeconds(randomIntBetween(1, 10000)),
                calendarId, null);
    }

    @Override
    protected ScheduledEvent createTestInstance() {
        return createScheduledEvent(randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected Writeable.Reader<ScheduledEvent> instanceReader() {
        return ScheduledEvent::new;
    }

    @Override
    protected ScheduledEvent doParseInstance(XContentParser parser) throws IOException {
        return ScheduledEvent.STRICT_PARSER.apply(parser, null).build();
    }

    public void testToDetectionRule() {
        long bucketSpanSecs = 300;
        ScheduledEvent event = createTestInstance();
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
        ScheduledEvent.Builder builder = new ScheduledEvent.Builder();

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


        builder = new ScheduledEvent.Builder().description("f").calendarId("c");
        builder.startTime(now);
        builder.endTime(now.minusHours(2));

        e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertThat(e.getMessage(), containsString("must come before end time"));
    }

    public void testStrictParser() throws IOException {
        String json = "{\"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> ScheduledEvent.STRICT_PARSER.apply(parser, null));

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = "{\"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            ScheduledEvent.LENIENT_PARSER.apply(parser, null);
        }
    }
}