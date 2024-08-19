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

public class LeftSerializationTests extends AbstractExpressionSerializationTests<Left> {
    @Override
    protected Left createTestInstance() {
        Source source = randomSource();
        Expression str = randomChild();
        Expression length = randomChild();
        return new Left(source, str, length);
    }

    @Override
    protected Left mutateInstance(Left instance) throws IOException {
        Source source = instance.source();
        Expression str = instance.str();
        Expression length = instance.length();
        if (randomBoolean()) {
            str = randomValueOtherThan(str, AbstractExpressionSerializationTests::randomChild);
        } else {
            length = randomValueOtherThan(length, AbstractExpressionSerializationTests::randomChild);
        }
        return new Left(source, str, length);
    }
}
