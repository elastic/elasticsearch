/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Condition;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;

public class ConditionTests extends AbstractSerializingTestCase<Condition> {

    public void testSetValues() {
        Condition cond = new Condition(Operator.EQ, "5");
        assertEquals(Operator.EQ, cond.getOperator());
        assertEquals("5", cond.getValue());
    }

    public void testHashCodeAndEquals() {
        Condition cond1 = new Condition(Operator.MATCH, "regex");
        Condition cond2 = new Condition(Operator.MATCH, "regex");

        assertEquals(cond1, cond2);
        assertEquals(cond1.hashCode(), cond2.hashCode());

        Condition cond3 = new Condition(Operator.EQ, "5");
        assertFalse(cond1.equals(cond3));
        assertFalse(cond1.hashCode() == cond3.hashCode());
    }

    @Override
    protected Condition createTestInstance() {
        Operator op = randomFrom(Operator.values());
        Condition condition;
        switch (op) {
        case EQ:
        case GT:
        case GTE:
        case LT:
        case LTE:
            condition = new Condition(op, Double.toString(randomDouble()));
            break;
        case MATCH:
            condition = new Condition(op, randomAlphaOfLengthBetween(1, 20));
            break;
        default:
            throw new AssertionError("Unknown operator selected: " + op);
        }
        return condition;
    }

    @Override
    protected Reader<Condition> instanceReader() {
        return Condition::new;
    }

    @Override
    protected Condition doParseInstance(XContentParser parser) {
        return Condition.PARSER.apply(parser, null);
    }

    public void testVerifyArgsNumericArgs() {
        new Condition(Operator.LTE, "100");
        new Condition(Operator.GT, "10.0");
    }

    public void testVerify_GivenEmptyValue() {
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, () -> new Condition(Operator.LT, ""));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_CONDITION_INVALID_VALUE_NUMBER, ""), e.getMessage());
    }

    public void testVerify_GivenInvalidRegex() {
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, () -> new Condition(Operator.MATCH, "[*"));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_CONDITION_INVALID_VALUE_REGEX, "[*"), e.getMessage());
    }

    public void testVerify_GivenNullRegex() {
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class,
                () -> new Condition(Operator.MATCH, null));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_CONDITION_INVALID_VALUE_NULL, "[*"), e.getMessage());
    }

    @Override
    protected Condition mutateInstance(Condition instance) throws IOException {
        Operator op = instance.getOperator();
        String value = instance.getValue();
        switch (between(0, 1)) {
        case 0:
            Operator newOp = op;
            while (newOp == op) {
                newOp = randomFrom(Operator.values());
            }
            if (op == Operator.MATCH && newOp != Operator.MATCH) {
                value = Double.toString(randomDouble());
            }
            op = newOp;
            break;
        case 1:
            value = Double.toString(randomDouble());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new Condition(op, value);
    }
}
