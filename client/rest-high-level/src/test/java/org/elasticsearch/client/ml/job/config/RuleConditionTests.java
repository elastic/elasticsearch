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
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

public class RuleConditionTests extends AbstractXContentTestCase<RuleCondition> {

    @Override
    protected RuleCondition createTestInstance() {
        return createRandom();
    }

    public static RuleCondition createRandom() {
        RuleCondition.AppliesTo appliesTo = randomFrom(RuleCondition.AppliesTo.values());
        Operator operator = randomFrom(Operator.LT, Operator.LTE, Operator.GT, Operator.GTE);
        return new RuleCondition(appliesTo, operator, randomDouble());
    }

    @Override
    protected RuleCondition doParseInstance(XContentParser parser) {
        return RuleCondition.PARSER.apply(parser, null);
    }

    public void testEqualsGivenSameObject() {
        RuleCondition condition = createRandom();
        assertTrue(condition.equals(condition));
    }

    public void testEqualsGivenString() {
        assertFalse(createRandom().equals("a string"));
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

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
