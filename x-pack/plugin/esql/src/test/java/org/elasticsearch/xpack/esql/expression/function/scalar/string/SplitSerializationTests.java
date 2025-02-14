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

public class SplitSerializationTests extends AbstractExpressionSerializationTests<Split> {
    @Override
    protected Split createTestInstance() {
        Source source = randomSource();
        Expression str = randomChild();
        Expression delim = randomChild();
        return new Split(source, str, delim);
    }

    @Override
    protected Split mutateInstance(Split instance) throws IOException {
        Source source = instance.source();
        Expression str = instance.str();
        Expression delim = instance.delim();
        if (randomBoolean()) {
            str = randomValueOtherThan(str, AbstractExpressionSerializationTests::randomChild);
        } else {
            delim = randomValueOtherThan(delim, AbstractExpressionSerializationTests::randomChild);
        }
        return new Split(source, str, delim);
    }
}
