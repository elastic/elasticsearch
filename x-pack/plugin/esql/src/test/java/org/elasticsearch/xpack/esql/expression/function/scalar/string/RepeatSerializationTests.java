/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class RepeatSerializationTests extends AbstractExpressionSerializationTests<Repeat> {
    @Override
    protected Repeat createTestInstance() {
        Source source = randomSource();
        Expression str = randomChild();
        Expression number = randomChild();
        return new Repeat(source, str, number);
    }

    @Override
    protected Repeat mutateInstance(Repeat instance) throws IOException {
        Source source = instance.source();
        Expression str = instance.str();
        Expression number = instance.number();
        if (randomBoolean()) {
            str = randomValueOtherThan(str, AbstractExpressionSerializationTests::randomChild);
        } else {
            number = randomValueOtherThan(number, AbstractExpressionSerializationTests::randomChild);
        }
        return new Repeat(source, str, number);
    }
}
