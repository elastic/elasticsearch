/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

public class DetectorTests extends AbstractXContentTestCase<Detector> {

    public void testEquals_GivenEqual() {
        Detector.Builder builder = new Detector.Builder("mean", "field");
        builder.setByFieldName("by_field");
        builder.setOverFieldName("over_field");
        builder.setPartitionFieldName("partition");
        builder.setUseNull(false);
        Detector detector1 = builder.build();

        builder = new Detector.Builder("mean", "field");
        builder.setByFieldName("by_field");
        builder.setOverFieldName("over_field");
        builder.setPartitionFieldName("partition");
        builder.setUseNull(false);
        Detector detector2 = builder.build();

        assertTrue(detector1.equals(detector2));
        assertTrue(detector2.equals(detector1));
        assertEquals(detector1.hashCode(), detector2.hashCode());
    }

    public void testEquals_GivenDifferentDetectorDescription() {
        Detector detector1 = createDetector().build();
        Detector.Builder builder = createDetector();
        builder.setDetectorDescription("bar");
        Detector detector2 = builder.build();

        assertFalse(detector1.equals(detector2));
    }

    public void testEquals_GivenDifferentByFieldName() {
        Detector detector1 = createDetector().build();
        Detector detector2 = createDetector().build();

        assertEquals(detector1, detector2);

        Detector.Builder builder = new Detector.Builder(detector2);
        builder.setByFieldName("by2");
        detector2 = builder.build();
        assertFalse(detector1.equals(detector2));
    }

    private Detector.Builder createDetector() {
        Detector.Builder detector = new Detector.Builder("mean", "field");
        detector.setByFieldName("by_field");
        detector.setOverFieldName("over_field");
        detector.setPartitionFieldName("partition");
        detector.setUseNull(true);
        DetectionRule rule = new DetectionRule.Builder(RuleScope.builder().exclude("partition", "partition_filter"))
                .setActions(RuleAction.SKIP_RESULT)
                .build();
        detector.setRules(Collections.singletonList(rule));
        return detector;
    }

    @Override
    protected Detector createTestInstance() {
        DetectorFunction function = randomFrom(EnumSet.allOf(DetectorFunction.class));
        Detector.Builder detector = new Detector.Builder(function, randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            detector.setDetectorDescription(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            detector.setPartitionFieldName(randomAlphaOfLengthBetween(6, 20));
        } else if (randomBoolean()) {
            detector.setOverFieldName(randomAlphaOfLengthBetween(6, 20));
        } else if (randomBoolean()) {
            detector.setByFieldName(randomAlphaOfLengthBetween(6, 20));
        }
        if (randomBoolean()) {
            detector.setExcludeFrequent(randomFrom(Detector.ExcludeFrequent.values()));
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<DetectionRule> rules = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                // no need for random DetectionRule (it is already tested)
                rules.add(new DetectionRule.Builder(Collections.singletonList(RuleConditionTests.createRandom())).build());
            }
            detector.setRules(rules);
        }
        if (randomBoolean()) {
            detector.setUseNull(randomBoolean());
        }
        return detector.build();
    }

    @Override
    protected Detector doParseInstance(XContentParser parser) {
        return Detector.PARSER.apply(parser, null).build();
    }

    public void testExcludeFrequentForString() {
        assertEquals(Detector.ExcludeFrequent.ALL, Detector.ExcludeFrequent.forString("all"));
        assertEquals(Detector.ExcludeFrequent.ALL, Detector.ExcludeFrequent.forString("ALL"));
        assertEquals(Detector.ExcludeFrequent.NONE, Detector.ExcludeFrequent.forString("none"));
        assertEquals(Detector.ExcludeFrequent.NONE, Detector.ExcludeFrequent.forString("NONE"));
        assertEquals(Detector.ExcludeFrequent.BY, Detector.ExcludeFrequent.forString("by"));
        assertEquals(Detector.ExcludeFrequent.BY, Detector.ExcludeFrequent.forString("BY"));
        assertEquals(Detector.ExcludeFrequent.OVER, Detector.ExcludeFrequent.forString("over"));
        assertEquals(Detector.ExcludeFrequent.OVER, Detector.ExcludeFrequent.forString("OVER"));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
