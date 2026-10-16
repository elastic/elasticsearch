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

public class FmtPercentSerializationTests extends AbstractExpressionSerializationTests<FmtPercent> {
    @Override
    protected FmtPercent createTestInstance() {
        Source source = randomSource();
        Expression value = randomChild();
        return new FmtPercent(source, value);
    }

    @Override
    protected FmtPercent mutateInstance(FmtPercent instance) throws IOException {
        Source source = instance.source();
        Expression value = instance.field();
        value = randomValueOtherThan(value, AbstractExpressionSerializationTests::randomChild);
        return new FmtPercent(source, value);
    }
}
