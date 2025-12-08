/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class AllLastSerializationTests extends AbstractExpressionSerializationTests<AllLast> {
    @Override
    protected AllLast createTestInstance() {
        return new AllLast(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected AllLast mutateInstance(AllLast instance) throws IOException {
        Expression field = instance.field();
        Expression sort = instance.sort();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            sort = randomValueOtherThan(sort, AbstractExpressionSerializationTests::randomChild);
        }
        return new AllLast(instance.source(), field, sort);
    }
}
