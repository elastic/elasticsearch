/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.SerializationTestUtils.ignoreIds;

public abstract class AbstractExpressionSerializationTests<T extends Expression> extends AbstractNodeSerializationTests<T> {

    public static Expression randomChild() {
        return ReferenceAttributeTests.randomReferenceAttribute(false);
    }

    public static Expression mutateExpression(Expression expression) {
        return randomValueOtherThan(expression, AbstractExpressionSerializationTests::randomChild);
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ExpressionWritables.getNamedWriteables());
    }

    @Override
    protected Class<? extends Node<?>> categoryClass() {
        return Expression.class;
    }

    @Override
    protected final T createTestInstance() {
        return ignoreIds(innerCreateTestInstance());
    }

    protected abstract T innerCreateTestInstance();

    @Override
    protected final T mutateInstance(T instance) throws IOException {
        return ignoreIds(innerMutateInstance(instance));
    }

    protected abstract T innerMutateInstance(T instance) throws IOException;

    @Override
    protected T copyInstance(T instance, TransportVersion version) throws IOException {
        return ignoreIds(super.copyInstance(instance, version));
    }
}
