/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RerankFunctionSerializationTests extends AbstractExpressionSerializationTests<RerankFunction> {
    @Override
    protected RerankFunction createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression query = randomChild();
        Expression options = randomChild();
        return new RerankFunction(source, field, query, options);
    }

    @Override
    protected RerankFunction mutateInstance(RerankFunction instance) throws IOException {
        List<Expression> newChildren = new ArrayList<>(instance.children());
        newChildren.set(randomInt(newChildren.size() - 1), randomChild());
        return instance.replaceChildren(newChildren);
    }
}
