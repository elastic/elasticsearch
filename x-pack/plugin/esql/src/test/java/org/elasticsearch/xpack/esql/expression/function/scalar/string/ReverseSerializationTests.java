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

public class ReverseSerializationTests extends AbstractExpressionSerializationTests<Reverse> {
    @Override
    protected Reverse createTestInstance() {
        return new Reverse(randomSource(), randomChild());
    }

    @Override
    protected Reverse mutateInstance(Reverse instance) throws IOException {
        Source source = instance.source();
        Expression child = randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild);
        return new Reverse(source, child);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
