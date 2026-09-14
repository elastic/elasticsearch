/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class LimitKSerializationTests extends AbstractExpressionSerializationTests<LimitK> {
    @Override
    protected LimitK createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression limit = randomChild();
        return new LimitK(source, field, limit);
    }

    @Override
    protected LimitK mutateInstance(LimitK instance) throws IOException {
        Source source = randomSource();
        Expression field = instance.field();
        Expression limit = instance.limitField();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            limit = randomValueOtherThan(limit, AbstractExpressionSerializationTests::randomChild);
        }
        return new LimitK(source, field, limit);
    }
}
