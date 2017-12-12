/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.calendars;

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
import java.util.ArrayList;
import java.util.List;

public class SpecialEventTests extends AbstractSerializingTestCase<SpecialEvent> {

    public static SpecialEvent createSpecialEvent() {
        int size = randomInt(10);
        List<String> jobIds = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            jobIds.add(randomAlphaOfLengthBetween(1, 20));
        }

        return new SpecialEvent(randomAlphaOfLength(10), randomAlphaOfLength(10),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(new DateTime(randomDateTimeZone()).getMillis()), ZoneOffset.UTC),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(new DateTime(randomDateTimeZone()).getMillis()), ZoneOffset.UTC),
                jobIds);
    }

    @Override
    protected SpecialEvent createTestInstance() {
        return createSpecialEvent();
    }

    @Override
    protected Writeable.Reader<SpecialEvent> instanceReader() {
        return SpecialEvent::new;
    }

    @Override
    protected SpecialEvent doParseInstance(XContentParser parser) throws IOException {
        return SpecialEvent.PARSER.apply(parser, null);
    }

    public void testToDetectionRule() {
        long bucketSpanSecs = 300;
        SpecialEvent event = createTestInstance();
        DetectionRule rule = event.toDetectionRule(TimeValue.timeValueSeconds(bucketSpanSecs));

        assertEquals(Connective.AND, rule.getConditionsConnective());
        assertEquals(RuleAction.SKIP_SAMPLING_AND_FILTER_RESULTS, rule.getRuleAction());
        assertNull(rule.getTargetFieldName());
        assertNull(rule.getTargetFieldValue());

        List<RuleCondition> conditions = rule.getRuleConditions();
        assertEquals(2, conditions.size());
        assertEquals(RuleConditionType.TIME, conditions.get(0).getConditionType());
        assertEquals(RuleConditionType.TIME, conditions.get(1).getConditionType());
        assertEquals(Operator.GTE, conditions.get(0).getCondition().getOperator());
        assertEquals(Operator.LT, conditions.get(1).getCondition().getOperator());

        // Check times are aligned with the bucket
        long conditionStartTime = Long.parseLong(conditions.get(0).getCondition().getValue());
        assertEquals(0, conditionStartTime % bucketSpanSecs);
        long bucketCount = conditionStartTime / bucketSpanSecs;
        assertEquals(bucketSpanSecs * bucketCount, conditionStartTime);

        long conditionEndTime = Long.parseLong(conditions.get(1).getCondition().getValue());
        assertEquals(0, conditionEndTime % bucketSpanSecs);
        assertEquals(bucketSpanSecs * (bucketCount + 1), conditionEndTime);
    }
}