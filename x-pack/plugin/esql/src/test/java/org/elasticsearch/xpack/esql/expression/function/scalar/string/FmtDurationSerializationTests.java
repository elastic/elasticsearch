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

public class FmtDurationSerializationTests extends AbstractExpressionSerializationTests<FmtDuration> {
    @Override
    protected FmtDuration createTestInstance() {
        Source source = randomSource();
        Expression nanoseconds = randomChild();
        Expression unit = randomBoolean() ? null : randomChild();
        return new FmtDuration(source, nanoseconds, unit);
    }

    @Override
    protected FmtDuration mutateInstance(FmtDuration instance) throws IOException {
        Source source = instance.source();
        Expression nanoseconds = instance.nanoseconds();
        Expression unit = instance.unit();
        if (randomBoolean()) {
            nanoseconds = randomValueOtherThan(nanoseconds, AbstractExpressionSerializationTests::randomChild);
        } else {
            unit = randomValueOtherThan(unit, () -> randomBoolean() ? null : randomChild());
        }
        return new FmtDuration(source, nanoseconds, unit);
    }
}
