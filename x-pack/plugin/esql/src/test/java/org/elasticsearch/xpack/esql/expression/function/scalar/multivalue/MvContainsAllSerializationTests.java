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

public class MvContainsAllSerializationTests extends AbstractExpressionSerializationTests<MvContainsAll> {
    @Override
    protected MvContainsAll createTestInstance() {
        Source source = randomSource();
        Expression field1 = randomChild();
        Expression field2 = randomChild();
        return new MvContainsAll(source, field1, field2);
    }

    @Override
    protected MvContainsAll mutateInstance(MvContainsAll instance) throws IOException {
        Source source = randomSource();
        Expression field1 = randomChild();
        Expression field2 = randomChild();
        if (randomBoolean()) {
            field1 = randomValueOtherThan(field1, AbstractExpressionSerializationTests::randomChild);
        } else {
            field2 = randomValueOtherThan(field2, AbstractExpressionSerializationTests::randomChild);
        }
        return new MvContainsAll(source, field1, field2);
    }
}
