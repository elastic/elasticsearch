/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class ToRangeSerializationTests extends AbstractExpressionSerializationTests<ToRange> {
    @Override
    protected ToRange createTestInstance() {
        Source source = randomSource();
        Expression from = randomChild();
        Expression to = randomChild();
        return new ToRange(source, from, to);
    }

    @Override
    protected ToRange mutateInstance(ToRange instance) throws IOException {
        Source source = instance.source();
        Expression from = instance.from();
        Expression to = instance.to();
        if (randomBoolean()) {
            from = randomValueOtherThan(from, AbstractExpressionSerializationTests::randomChild);
        } else {
            to = randomValueOtherThan(to, AbstractExpressionSerializationTests::randomChild);
        }
        return new ToRange(source, from, to);
    }
}
