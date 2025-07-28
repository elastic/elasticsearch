/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nulls;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;

import java.io.IOException;

public class IsNotNullSerializationTests extends AbstractExpressionSerializationTests<IsNotNull> {
    @Override
    protected IsNotNull createTestInstance() {
        return new IsNotNull(randomSource(), randomChild());
    }

    @Override
    protected IsNotNull mutateInstance(IsNotNull instance) throws IOException {
        Source source = instance.source();
        Expression child = randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild);
        return new IsNotNull(source, child);
    }
}
