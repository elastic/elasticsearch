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
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

import java.io.IOException;

public class ContainsSerializationTests extends AbstractExpressionSerializationTests<Contains> {
    @Override
    protected Contains createTestInstance() {
        Source source = randomSource();
        Expression str = randomChild();
        Expression substr = randomChild();
        return new Contains(source, str, substr);
    }

    @Override
    protected Contains mutateInstance(Contains instance) throws IOException {
        Source source = instance.source();
        Expression str = instance.str();
        Expression substr = instance.substr();
        if (randomBoolean()) {
            str = randomValueOtherThan(str, AbstractUnaryScalarSerializationTests::randomChild);
        } else {
            substr = randomValueOtherThan(substr, AbstractUnaryScalarSerializationTests::randomChild);
        }
        return new Contains(source, str, substr);
    }
}
