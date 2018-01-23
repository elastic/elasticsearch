/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;

public class DetectionRuleTests extends AbstractSerializingTestCase<DetectionRule> {

    public void testExtractReferencedLists() {
        RuleCondition numericalCondition =
                new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "field", "value", new Condition(Operator.GT, "5"), null);
        List<RuleCondition> conditions = Arrays.asList(
                numericalCondition,
                RuleCondition.createCategorical("foo", "filter1"),
                RuleCondition.createCategorical("bar", "filter2"));

        DetectionRule rule = new DetectionRule.Builder(conditions).build();

        assertEquals(new HashSet<>(Arrays.asList("filter1", "filter2")), rule.extractReferencedFilters());
    }

    public void testEqualsGivenSameObject() {
        DetectionRule rule = createFullyPopulated().build();
        assertTrue(rule.equals(rule));
    }

    public void testEqualsGivenString() {
        assertFalse(createFullyPopulated().build().equals("a string"));
    }

    public void testEqualsGivenDifferentTargetFieldName() {
        DetectionRule rule1 = createFullyPopulated().build();
        DetectionRule rule2 = createFullyPopulated().setTargetFieldName("targetField2").build();
        assertFalse(rule1.equals(rule2));
        assertFalse(rule2.equals(rule1));
    }

    public void testEqualsGivenDifferentTargetFieldValue() {
        DetectionRule rule1 = createFullyPopulated().build();
        DetectionRule rule2 = createFullyPopulated().setTargetFieldValue("targetValue2").build();
        assertFalse(rule1.equals(rule2));
        assertFalse(rule2.equals(rule1));
    }

    public void testEqualsGivenDifferentConnective() {
        DetectionRule rule1 = createFullyPopulated().build();
        DetectionRule rule2 = createFullyPopulated().setConditionsConnective(Connective.OR).build();
        assertFalse(rule1.equals(rule2));
        assertFalse(rule2.equals(rule1));
    }

    public void testEqualsGivenRules() {
        DetectionRule rule1 = createFullyPopulated().build();
        DetectionRule rule2 = createFullyPopulated().setConditions(createRule("10")).build();
        assertFalse(rule1.equals(rule2));
        assertFalse(rule2.equals(rule1));
    }

    public void testEqualsGivenEqual() {
        DetectionRule rule1 = createFullyPopulated().build();
        DetectionRule rule2 = createFullyPopulated().build();
        assertTrue(rule1.equals(rule2));
        assertTrue(rule2.equals(rule1));
        assertEquals(rule1.hashCode(), rule2.hashCode());
    }

    private static DetectionRule.Builder createFullyPopulated() {
        return new DetectionRule.Builder(createRule("5"))
                .setActions(EnumSet.of(RuleAction.FILTER_RESULTS, RuleAction.SKIP_SAMPLING))
                .setTargetFieldName("targetField")
                .setTargetFieldValue("targetValue")
                .setConditionsConnective(Connective.AND);
    }

    private static List<RuleCondition> createRule(String value) {
        Condition condition = new Condition(Operator.GT, value);
        return Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition, null));
    }

    @Override
    protected DetectionRule createTestInstance() {
        int size = 1 + randomInt(20);
        List<RuleCondition> ruleConditions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // no need for random condition (it is already tested)
            ruleConditions.addAll(createRule(Double.toString(randomDouble())));
        }
        DetectionRule.Builder builder = new DetectionRule.Builder(ruleConditions);

        if (randomBoolean()) {
            EnumSet<RuleAction> actions = EnumSet.noneOf(RuleAction.class);
            int actionsCount = randomIntBetween(1, RuleAction.values().length);
            for (int i = 0; i < actionsCount; ++i) {
                actions.add(randomFrom(RuleAction.values()));
            }
            builder.setActions(actions);
        }

        if (randomBoolean()) {
            builder.setConditionsConnective(randomFrom(Connective.values()));
        }

        if (randomBoolean()) {
            builder.setTargetFieldName(randomAlphaOfLengthBetween(1, 20));
            builder.setTargetFieldValue(randomAlphaOfLengthBetween(1, 20));
        }

        return builder.build();
    }

    @Override
    protected Reader<DetectionRule> instanceReader() {
        return DetectionRule::new;
    }

    @Override
    protected DetectionRule doParseInstance(XContentParser parser) {
        return DetectionRule.CONFIG_PARSER.apply(parser, null).build();
    }

    @Override
    protected DetectionRule mutateInstance(DetectionRule instance) throws IOException {
        List<RuleCondition> conditions = instance.getConditions();
        EnumSet<RuleAction> actions = instance.getActions();
        String targetFieldName = instance.getTargetFieldName();
        String targetFieldValue = instance.getTargetFieldValue();
        Connective connective = instance.getConditionsConnective();

        switch (between(0, 3)) {
        case 0:
            conditions = new ArrayList<>(conditions);
            conditions.addAll(createRule(Double.toString(randomDouble())));
            break;
        case 1:
            targetFieldName = randomAlphaOfLengthBetween(5, 10);
            break;
        case 2:
            targetFieldValue = randomAlphaOfLengthBetween(5, 10);
            if (targetFieldName == null) {
                targetFieldName = randomAlphaOfLengthBetween(5, 10);
            }
            break;
        case 3:
            if (connective == Connective.AND) {
                connective = Connective.OR;
            } else {
                connective = Connective.AND;
            }
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new DetectionRule.Builder(conditions).setActions(actions).setTargetFieldName(targetFieldName)
                .setTargetFieldValue(targetFieldValue).setConditionsConnective(connective).build();
    }
}
