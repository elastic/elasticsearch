/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MvSliceSerializationTests extends AbstractExpressionSerializationTests<MvSlice> {
    @Override
    protected MvSlice createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression start = randomChild();
        Expression end = randomBoolean() ? null : randomChild();
        return new MvSlice(source, field, start, end);
    }

    @Override
    protected MvSlice mutateInstance(MvSlice instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression start = instance.start();
        Expression end = instance.end();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> start = randomValueOtherThan(start, AbstractExpressionSerializationTests::randomChild);
            case 2 -> end = randomValueOtherThan(end, () -> randomBoolean() ? null : randomChild());
        }
        return new MvSlice(source, field, start, end);
    }
}
