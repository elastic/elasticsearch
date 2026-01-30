/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class RoundSerializationTests extends AbstractExpressionSerializationTests<Round> {
    @Override
    protected Round createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression decimals = randomBoolean() ? null : randomChild();
        return new Round(source, field, decimals);
    }

    @Override
    protected Round mutateInstance(Round instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression decimals = instance.decimals();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            decimals = randomValueOtherThan(decimals, () -> randomBoolean() ? null : randomChild());
        }
        return new Round(source, field, decimals);
    }
}
