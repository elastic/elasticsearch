/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractExpressionSerializationTests<T extends Expression> extends AbstractNodeSerializationTests<T> {
    public static Expression randomChild() {
        return ReferenceAttributeTests.randomReferenceAttribute();
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(NamedExpression.getNamedWriteables());
        entries.addAll(Expression.getNamedWriteables());
        entries.addAll(Attribute.getNamedWriteables());
        entries.addAll(EsqlScalarFunction.getNamedWriteables());
        entries.addAll(AggregateFunction.getNamedWriteables());
        entries.add(UnsupportedAttribute.ENTRY);
        entries.add(UnsupportedAttribute.NAMED_EXPRESSION_ENTRY);
        entries.add(UnsupportedAttribute.EXPRESSION_ENTRY);
        entries.add(org.elasticsearch.xpack.esql.expression.Order.ENTRY);
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected Class<? extends Node<?>> categoryClass() {
        return Expression.class;
    }
}
