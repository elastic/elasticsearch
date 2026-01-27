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

public class ToLowerSerializationTests extends AbstractExpressionSerializationTests<ToLower> {
    @Override
    protected ToLower createTestInstance() {
        return new ToLower(randomSource(), randomChild(), configuration());
    }

    @Override
    protected ToLower mutateInstance(ToLower instance) throws IOException {
        Source source = instance.source();
        Expression child = randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild);
        return new ToLower(source, child, configuration());
    }
}
