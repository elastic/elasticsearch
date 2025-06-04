/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.calendars;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleAction;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.config.RuleParams;
import org.elasticsearch.xpack.core.ml.job.config.RuleParamsForForceTimeShift;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;

public class ScheduledEventTests extends AbstractXContentSerializingTestCase<ScheduledEvent> {

    private static final long BUCKET_SPAN_SECS = 300;

    public static ScheduledEvent createScheduledEvent(String calendarId) {
        Instant start = Instant.now();
        return new ScheduledEvent(
            randomAlphaOfLength(10),
            start,
            start.plusSeconds(randomIntBetween(1, 10000)),
            randomBoolean(),
            randomBoolean(),
            randomBoolean() ? null : randomInt(),
            calendarId,
            null
        );
    }

    public static ScheduledEvent createScheduledEvent(
        String calendarId,
        Boolean skipResult,
        Boolean skipModelUpdate,
        Integer forceTimeShift
    ) {
        Instant start = Instant.now();
        return new ScheduledEvent(
            randomAlphaOfLength(10),
            start,
            start.plusSeconds(randomIntBetween(1, 10000)),
            skipResult,
            skipModelUpdate,
            forceTimeShift,
            calendarId,
            null
        );
    }

    @Override
    protected ScheduledEvent createTestInstance() {
        return createScheduledEvent(randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected ScheduledEvent mutateInstance(ScheduledEvent instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<ScheduledEvent> instanceReader() {
        return ScheduledEvent::new;
    }

    @Override
    protected ScheduledEvent doParseInstance(XContentParser parser) throws IOException {
        return ScheduledEvent.STRICT_PARSER.apply(parser, null).build();
    }

    public void testToDetectionRule_SetsSkipResultActionProperly() {
        List<Boolean> validValues = List.of(true, false);
        validValues.forEach((skipResult) -> {
            Boolean skipModelUpdate = randomBoolean();
            Integer forceTimeShift = randomInt();
            ScheduledEvent event = createScheduledEvent(randomAlphaOfLength(10), skipResult, skipModelUpdate, forceTimeShift);
            DetectionRule rule = event.toDetectionRule(TimeValue.timeValueSeconds(BUCKET_SPAN_SECS));
            validateDetectionRule(event, rule, skipResult, skipModelUpdate, forceTimeShift);
        });
    }

    public void testToDetectionRule_SetsSkipModelUpdateActionProperly() {
        List<Boolean> validValues = List.of(true, false);
        validValues.forEach((skipModelUpdate) -> {
            Boolean skipResult = randomBoolean();
            Integer forceTimeShift = randomInt();
            ScheduledEvent event = createScheduledEvent(randomAlphaOfLength(10), skipResult, skipModelUpdate, forceTimeShift);
            DetectionRule rule = event.toDetectionRule(TimeValue.timeValueSeconds(BUCKET_SPAN_SECS));
            validateDetectionRule(event, rule, skipResult, skipModelUpdate, forceTimeShift);
        });
    }

    public void testToDetectionRule_SetsForceTimeShiftActionProperly() {
        List<Optional<Integer>> validValues = List.of(Optional.of(randomInt()), Optional.empty());
        validValues.forEach((forceTimeShift) -> {
            Boolean skipResult = randomBoolean();
            Boolean skipModelUpdate = randomBoolean();
            ScheduledEvent event = createScheduledEvent(randomAlphaOfLength(10), skipResult, skipModelUpdate, forceTimeShift.orElse(null));
            DetectionRule rule = event.toDetectionRule(TimeValue.timeValueSeconds(BUCKET_SPAN_SECS));
            validateDetectionRule(event, rule, skipResult, skipModelUpdate, forceTimeShift.orElse(null));
        });
    }

    private void validateDetectionRule(
        ScheduledEvent event,
        DetectionRule rule,
        Boolean skipResult,
        Boolean skipModelUpdate,
        Integer forceTimeShift
    ) {
        assertEquals(skipResult, rule.getActions().contains(RuleAction.SKIP_RESULT));
        assertEquals(skipModelUpdate, rule.getActions().contains(RuleAction.SKIP_MODEL_UPDATE));
        if (forceTimeShift != null) {
            RuleParams expectedRuleParams = new RuleParams(new RuleParamsForForceTimeShift(forceTimeShift));
            assertEquals(expectedRuleParams, rule.getParams());
        }

        List<RuleCondition> conditions = rule.getConditions();
        assertEquals(2, conditions.size());
        assertEquals(RuleCondition.AppliesTo.TIME, conditions.get(0).getAppliesTo());
        assertEquals(RuleCondition.AppliesTo.TIME, conditions.get(1).getAppliesTo());
        assertEquals(Operator.GTE, conditions.get(0).getOperator());
        assertEquals(Operator.LT, conditions.get(1).getOperator());

        // Check times are aligned with the bucket
        long conditionStartTime = (long) conditions.get(0).getValue();
        assertEquals(0, conditionStartTime % BUCKET_SPAN_SECS);
        long bucketCount = conditionStartTime / BUCKET_SPAN_SECS;
        assertEquals(BUCKET_SPAN_SECS * bucketCount, conditionStartTime);

        long conditionEndTime = (long) conditions.get(1).getValue();
        assertEquals(0, conditionEndTime % BUCKET_SPAN_SECS);

        long eventTime = event.getEndTime().getEpochSecond() - conditionStartTime;
        long numbBucketsInEvent = (eventTime + BUCKET_SPAN_SECS - 1) / BUCKET_SPAN_SECS;
        assertEquals(BUCKET_SPAN_SECS * (bucketCount + numbBucketsInEvent), conditionEndTime);
    }

    public void testBuild_DescriptionNull() {
        ScheduledEvent.Builder builder = new ScheduledEvent.Builder();
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [description] cannot be null", e.getMessage());
    }

    public void testBuild_StartTimeNull() {
        ScheduledEvent.Builder builder = new ScheduledEvent.Builder().description("foo");
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [start_time] cannot be null", e.getMessage());
    }

    public void testBuild_EndTimeNull() {
        ScheduledEvent.Builder builder = new ScheduledEvent.Builder().description("foo").startTime(Instant.now());
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [end_time] cannot be null", e.getMessage());
    }

    public void testBuild_CalendarIdNull() {
        ScheduledEvent.Builder builder = new ScheduledEvent.Builder().description("foo")
            .startTime(Instant.now())
            .endTime(Instant.now().plusSeconds(1 * 60 * 60));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [calendar_id] cannot be null", e.getMessage());
    }

    public void testBuild_StartTimeAfterEndTime() {
        Instant now = Instant.now();
        ScheduledEvent.Builder builder = new ScheduledEvent.Builder().description("f")
            .calendarId("c")
            .startTime(now)
            .endTime(now.minusSeconds(2 * 60 * 60));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertThat(e.getMessage(), containsString("must come before end time"));
    }

    public void testBuild_SucceedsWithDefaultSkipResultAndSkipModelUpdatesValues() {
        validateScheduledEventSuccessfulBuild(Optional.empty(), Optional.empty(), Optional.empty());
    }

    public void testBuild_SucceedsWithProvidedSkipResultAndSkipModelUpdatesValues() {
        Boolean skipResult = randomBoolean();
        Boolean skipModelUpdate = randomBoolean();
        Integer forceTimeShift = randomBoolean() ? null : randomInt();

        validateScheduledEventSuccessfulBuild(Optional.of(skipResult), Optional.of(skipModelUpdate), Optional.ofNullable(forceTimeShift));
    }

    private void validateScheduledEventSuccessfulBuild(
        Optional<Boolean> skipResult,
        Optional<Boolean> skipModelUpdate,
        Optional<Integer> forceTimeShift
    ) {
        String description = randomAlphaOfLength(10);
        String calendarId = randomAlphaOfLength(10);
        Instant startTime = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        Instant endTime = startTime.plusSeconds(randomIntBetween(1, 3600));

        ScheduledEvent.Builder builder = new ScheduledEvent.Builder().description(description)
            .calendarId(calendarId)
            .startTime(startTime)
            .endTime(endTime);
        skipResult.ifPresent(builder::skipResult);
        skipModelUpdate.ifPresent(builder::skipModelUpdate);
        forceTimeShift.ifPresent(builder::forceTimeShift);

        ScheduledEvent event = builder.build();
        assertEquals(description, event.getDescription());
        assertEquals(calendarId, event.getCalendarId());
        assertEquals(startTime, event.getStartTime());
        assertEquals(endTime, event.getEndTime());
        assertEquals(skipResult.orElse(true), event.getSkipResult());
        assertEquals(skipModelUpdate.orElse(true), event.getSkipModelUpdate());
        assertEquals(forceTimeShift.orElse(null), event.getForceTimeShift());
    }

    public void testBuild() {
        ScheduledEvent.Builder builder = new ScheduledEvent.Builder();

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [description] cannot be null", e.getMessage());
        builder.description("foo");
        e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [start_time] cannot be null", e.getMessage());
        Instant now = Instant.now();
        builder.startTime(now);
        e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [end_time] cannot be null", e.getMessage());
        builder.endTime(now.plusSeconds(1 * 60 * 60));
        e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertEquals("Field [calendar_id] cannot be null", e.getMessage());
        builder.calendarId("foo");
        builder.build();

        builder = new ScheduledEvent.Builder().description("f").calendarId("c");
        builder.startTime(now);
        builder.endTime(now.minusSeconds(2 * 60 * 60));

        e = expectThrows(ElasticsearchStatusException.class, builder::build);
        assertThat(e.getMessage(), containsString("must come before end time"));
    }

    public void testStrictParser() throws IOException {
        String json = "{\"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ScheduledEvent.STRICT_PARSER.apply(parser, null)
            );

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
