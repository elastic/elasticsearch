/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests;

public abstract class AbstractExpressionSerializationTests<T extends Expression> extends AbstractNodeSerializationTests<T> {
    public static Expression randomChild() {
        return ReferenceAttributeTests.randomReferenceAttribute(false);
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ExpressionWritables.getNamedWriteables());
    }

    @Override
    protected Class<? extends Node<?>> categoryClass() {
        return Expression.class;
    }
}
