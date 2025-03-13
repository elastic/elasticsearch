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

public class EndsWithSerializationTests extends AbstractExpressionSerializationTests<EndsWith> {
    @Override
    protected EndsWith createTestInstance() {
        Source source = randomSource();
        Expression str = randomChild();
        Expression suffix = randomChild();
        return new EndsWith(source, str, suffix);
    }

    @Override
    protected EndsWith mutateInstance(EndsWith instance) throws IOException {
        Source source = instance.source();
        Expression str = instance.str();
        Expression suffix = instance.suffix();
        if (randomBoolean()) {
            str = randomValueOtherThan(str, AbstractExpressionSerializationTests::randomChild);
        } else {
            suffix = randomValueOtherThan(suffix, AbstractExpressionSerializationTests::randomChild);
        }
        return new EndsWith(source, str, suffix);
    }
}
