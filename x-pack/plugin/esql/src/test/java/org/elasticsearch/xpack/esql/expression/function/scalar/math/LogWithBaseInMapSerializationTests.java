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
import org.elasticsearch.xpack.esql.expression.function.scalar.map.LogWithBaseInMap;

import java.io.IOException;

public class LogWithBaseInMapSerializationTests extends AbstractExpressionSerializationTests<LogWithBaseInMap> {
    @Override
    protected LogWithBaseInMap createTestInstance() {
        Source source = randomSource();
        Expression number = randomChild();
        Expression base = randomBoolean() ? null : randomChild();
        return new LogWithBaseInMap(source, number, base);
    }

    @Override
    protected LogWithBaseInMap mutateInstance(LogWithBaseInMap instance) throws IOException {
        Source source = instance.source();
        Expression number = instance.number();
        Expression base = instance.base();
        if (randomBoolean()) {
            number = randomValueOtherThan(number, AbstractExpressionSerializationTests::randomChild);
        } else {
            base = randomValueOtherThan(base, () -> randomBoolean() ? null : randomChild());
        }
        return new LogWithBaseInMap(source, number, base);
    }
}
