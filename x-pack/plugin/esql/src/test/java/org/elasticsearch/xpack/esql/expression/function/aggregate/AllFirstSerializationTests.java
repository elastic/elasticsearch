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

public class AllFirstSerializationTests extends AbstractExpressionSerializationTests<AllFirst> {
    @Override
    protected AllFirst createTestInstance() {
        return new AllFirst(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected AllFirst mutateInstance(AllFirst instance) throws IOException {
        Expression field = instance.field();
        Expression sort = instance.sort();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            sort = randomValueOtherThan(sort, AbstractExpressionSerializationTests::randomChild);
        }
        return new AllFirst(instance.source(), field, sort);
    }
}
