/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class LogSerializationTests extends AbstractExpressionSerializationTests<Log> {
    @Override
    protected Log createTestInstance() {
        Source source = randomSource();
        Expression value = randomChild();
        Expression base = randomBoolean() ? null : randomChild();
        return new Log(source, value, base);
    }

    @Override
    protected Log mutateInstance(Log instance) throws IOException {
        Source source = instance.source();
        Expression value = instance.value();
        Expression base = instance.base();
        if (randomBoolean()) {
            value = randomValueOtherThan(value, AbstractExpressionSerializationTests::randomChild);
        } else {
            base = randomValueOtherThan(base, () -> randomBoolean() ? null : randomChild());
        }
        return new Log(source, value, base);
    }
}
