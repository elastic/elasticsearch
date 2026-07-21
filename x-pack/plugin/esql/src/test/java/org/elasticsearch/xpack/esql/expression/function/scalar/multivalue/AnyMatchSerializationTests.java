/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Lambda;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class AnyMatchSerializationTests extends AbstractExpressionSerializationTests<AnyMatch> {
    @Override
    protected AnyMatch createTestInstance() {
        return new AnyMatch(randomSource(), randomChild(), randomLambda());
    }

    @Override
    protected AnyMatch mutateInstance(AnyMatch instance) throws IOException {
        Expression field = instance.field();
        Expression lambda = instance.children().get(1);
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            lambda = randomValueOtherThan(lambda, AnyMatchSerializationTests::randomLambda);
        }
        return new AnyMatch(instance.source(), field, lambda);
    }

    static Lambda randomLambda() {
        ReferenceAttribute param = new ReferenceAttribute(
            Source.EMPTY,
            randomAlphaOfLength(5),
            randomFrom(DataType.KEYWORD, DataType.INTEGER, DataType.LONG, DataType.BOOLEAN)
        );
        return new Lambda(Source.EMPTY, List.of(param, randomChild()));
    }
}
