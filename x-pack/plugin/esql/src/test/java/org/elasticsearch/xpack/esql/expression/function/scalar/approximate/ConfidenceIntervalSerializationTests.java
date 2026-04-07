/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConfidenceIntervalSerializationTests extends AbstractExpressionSerializationTests<ConfidenceInterval> {

    @Override
    protected ConfidenceInterval createTestInstance() {
        return new ConfidenceInterval(randomSource(), randomChild(), randomChild(), randomChild(), randomChild(), randomChild());
    }

    @Override
    protected ConfidenceInterval mutateInstance(ConfidenceInterval instance) throws IOException {
        Source source = instance.source();
        List<Expression> children = new ArrayList<>(instance.children());
        int i = randomIntBetween(0, children.size() - 1);
        children.set(i, randomValueOtherThan(children.get(i), AbstractExpressionSerializationTests::randomChild));
        return new ConfidenceInterval(source, children.get(0), children.get(1), children.get(2), children.get(3), children.get(4));
    }
}
