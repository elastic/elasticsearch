/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

public class RuleConditionTests extends AbstractXContentSerializingTestCase<RuleCondition> {

    @Override
    protected RuleCondition createTestInstance() {
        return createRandom();
    }

    @Override
    protected RuleCondition mutateInstance(RuleCondition instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static RuleCondition createRandom() {
        RuleCondition.AppliesTo appliesTo = randomFrom(RuleCondition.AppliesTo.values());
        Operator operator = randomFrom(Operator.LT, Operator.LTE, Operator.GT, Operator.GTE);
        return new RuleCondition(appliesTo, operator, randomDouble());
    }

    @Override
    protected Reader<RuleCondition> instanceReader() {
        return RuleCondition::new;
    }

    @Override
    protected RuleCondition doParseInstance(XContentParser parser) {
        return RuleCondition.STRICT_PARSER.apply(parser, null);
    }

    public void testEqualsGivenSameObject() {
        RuleCondition condition = createRandom();
        assertTrue(condition.equals(condition));
    }

    public void testEqualsGivenString() {
        assertFalse(createRandom().equals("a string"));
    }

    public void testVerify_GivenValidActual() {
        // no validation error:
        new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 5.0);
    }

    public void testVerify_GivenValidTypical() {
        // no validation error:
        new RuleCondition(RuleCondition.AppliesTo.TYPICAL, Operator.GTE, 5.0);
    }

    public void testVerify_GivenValidDiffFromTypical() {
        // no validation error:
        new RuleCondition(RuleCondition.AppliesTo.DIFF_FROM_TYPICAL, Operator.LT, 5.0);
    }

    public void testCreateTimeBased() {
        RuleCondition timeBased = RuleCondition.createTime(Operator.GTE, 100L);
        assertEquals(RuleCondition.AppliesTo.TIME, timeBased.getAppliesTo());
        assertEquals(Operator.GTE, timeBased.getOperator());
        assertEquals(100.0, timeBased.getValue(), 0.000001);
    }

    public void testAppliesToFromString() {
        assertEquals(RuleCondition.AppliesTo.ACTUAL, RuleCondition.AppliesTo.fromString("actual"));
        assertEquals(RuleCondition.AppliesTo.ACTUAL, RuleCondition.AppliesTo.fromString("ACTUAL"));
        assertEquals(RuleCondition.AppliesTo.TYPICAL, RuleCondition.AppliesTo.fromString("typical"));
        assertEquals(RuleCondition.AppliesTo.TYPICAL, RuleCondition.AppliesTo.fromString("TYPICAL"));
        assertEquals(RuleCondition.AppliesTo.DIFF_FROM_TYPICAL, RuleCondition.AppliesTo.fromString("diff_from_typical"));
        assertEquals(RuleCondition.AppliesTo.DIFF_FROM_TYPICAL, RuleCondition.AppliesTo.fromString("DIFF_FROM_TYPICAL"));
        assertEquals(RuleCondition.AppliesTo.TIME, RuleCondition.AppliesTo.fromString("time"));
        assertEquals(RuleCondition.AppliesTo.TIME, RuleCondition.AppliesTo.fromString("TIME"));
    }

    public void testAppliesToToString() {
        assertEquals("actual", RuleCondition.AppliesTo.ACTUAL.toString());
        assertEquals("typical", RuleCondition.AppliesTo.TYPICAL.toString());
        assertEquals("diff_from_typical", RuleCondition.AppliesTo.DIFF_FROM_TYPICAL.toString());
        assertEquals("time", RuleCondition.AppliesTo.TIME.toString());
    }
}
