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

public class FmtBytesSerializationTests extends AbstractExpressionSerializationTests<FmtBytes> {
    @Override
    protected FmtBytes createTestInstance() {
        Source source = randomSource();
        Expression bytes = randomChild();
        Expression unit = randomBoolean() ? null : randomChild();
        return new FmtBytes(source, bytes, unit);
    }

    @Override
    protected FmtBytes mutateInstance(FmtBytes instance) throws IOException {
        Source source = instance.source();
        Expression bytes = instance.bytes();
        Expression unit = instance.unit();
        if (randomBoolean()) {
            bytes = randomValueOtherThan(bytes, AbstractExpressionSerializationTests::randomChild);
        } else {
            unit = randomValueOtherThan(unit, () -> randomBoolean() ? null : randomChild());
        }
        return new FmtBytes(source, bytes, unit);
    }
}
