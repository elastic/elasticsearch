/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class DetectionRuleTests extends AbstractSerializingTestCase<DetectionRule> {

    public void testExtractReferoencedLists() {
        RuleCondition numericalCondition =
                new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "field", "value", new Condition(Operator.GT, "5"), null);
        List<RuleCondition> conditions = Arrays.asList(
                numericalCondition,
                RuleCondition.createCategorical("foo", "list1"),
                RuleCondition.createCategorical("bar", "list2"));
        DetectionRule rule = new DetectionRule(null, null, Connective.OR, conditions);

        assertEquals(new HashSet<>(Arrays.asList("list1", "list2")), rule.extractReferencedLists());
    }

    public void testEqualsGivenSameObject() {
        DetectionRule rule = createFullyPopulated();
        assertTrue(rule.equals(rule));
    }

    public void testEqualsGivenString() {
        assertFalse(createFullyPopulated().equals("a string"));
    }

    public void testEqualsGivenDifferentTargetFieldName() {
        DetectionRule rule1 = createFullyPopulated();
        DetectionRule rule2 = new DetectionRule("targetField2", "targetValue", Connective.AND, createRule("5"));
        assertFalse(rule1.equals(rule2));
        assertFalse(rule2.equals(rule1));
    }

    public void testEqualsGivenDifferentTargetFieldValue() {
        DetectionRule rule1 = createFullyPopulated();
        DetectionRule rule2 = new DetectionRule("targetField", "targetValue2", Connective.AND, createRule("5"));
        assertFalse(rule1.equals(rule2));
        assertFalse(rule2.equals(rule1));
    }

    public void testEqualsGivenDifferentConjunction() {
        DetectionRule rule1 = createFullyPopulated();
        DetectionRule rule2 = new DetectionRule("targetField", "targetValue", Connective.OR, createRule("5"));
        assertFalse(rule1.equals(rule2));
        assertFalse(rule2.equals(rule1));
    }

    public void testEqualsGivenRules() {
        DetectionRule rule1 = createFullyPopulated();
        DetectionRule rule2 = new DetectionRule("targetField", "targetValue", Connective.AND, createRule("10"));
        assertFalse(rule1.equals(rule2));
        assertFalse(rule2.equals(rule1));
    }

    public void testEqualsGivenEqual() {
        DetectionRule rule1 = createFullyPopulated();
        DetectionRule rule2 = createFullyPopulated();
        assertTrue(rule1.equals(rule2));
        assertTrue(rule2.equals(rule1));
        assertEquals(rule1.hashCode(), rule2.hashCode());
    }

    private static DetectionRule createFullyPopulated() {
        return new DetectionRule("targetField", "targetValue", Connective.AND, createRule("5"));
    }

    private static List<RuleCondition> createRule(String value) {
        Condition condition = new Condition(Operator.GT, value);
        return Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition, null));
    }

    @Override
    protected DetectionRule createTestInstance() {
        String targetFieldName = null;
        String targetFieldValue = null;
        Connective connective = randomFrom(Connective.values());
        if (randomBoolean()) {
            targetFieldName = randomAsciiOfLengthBetween(1, 20);
            targetFieldValue = randomAsciiOfLengthBetween(1, 20);
        }
        int size = 1 + randomInt(20);
        List<RuleCondition> ruleConditions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // no need for random condition (it is already tested)
            ruleConditions.addAll(createRule(Double.toString(randomDouble())));
        }
        return new DetectionRule(targetFieldName, targetFieldValue, connective, ruleConditions);
    }

    @Override
    protected Reader<DetectionRule> instanceReader() {
        return DetectionRule::new;
    }

    @Override
    protected DetectionRule parseInstance(XContentParser parser) {
        return DetectionRule.PARSER.apply(parser, null);
    }
}
