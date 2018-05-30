/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Condition;
import org.elasticsearch.xpack.core.ml.job.config.Operator;

import java.io.IOException;

public class ConditionTests extends AbstractSerializingTestCase<Condition> {

    @Override
    protected Condition createTestInstance() {
        Operator op = randomFrom(Operator.values());
        Condition condition;
        switch (op) {
        case GT:
        case GTE:
        case LT:
        case LTE:
            condition = new Condition(op, randomDouble());
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

    @Override
    protected Condition mutateInstance(Condition instance) throws IOException {
        Operator op = instance.getOperator();
        double value = instance.getValue();
        switch (between(0, 1)) {
        case 0:
            Operator newOp = op;
            while (newOp == op) {
                newOp = randomFrom(Operator.values());
            }
            op = newOp;
            break;
        case 1:
            value = randomDouble();
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new Condition(op, value);
    }
}
