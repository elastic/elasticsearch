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

public class SpaceSerializationTests extends AbstractExpressionSerializationTests<Space> {
    @Override
    protected Space createTestInstance() {
        Source source = randomSource();
        Expression number = randomChild();
        return new Space(source, number);
    }

    @Override
    protected Space mutateInstance(Space instance) throws IOException {
        Source source = instance.source();
        Expression number = instance.field();
        number = randomValueOtherThan(number, AbstractExpressionSerializationTests::randomChild);
        return new Space(source, number);
    }
}
