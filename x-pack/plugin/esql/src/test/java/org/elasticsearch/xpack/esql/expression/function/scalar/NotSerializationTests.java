/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;

import java.io.IOException;

public class NotSerializationTests extends AbstractExpressionSerializationTests<Not> {
    @Override
    protected Not createTestInstance() {
        return new Not(randomSource(), randomChild());
    }

    @Override
    protected Not mutateInstance(Not instance) throws IOException {
        Source source = instance.source();
        Expression child = randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild);
        return new Not(source, child);
    }
}
