/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.pipeline;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * An input that must first be rewritten against the rest of the query
 * before it can be further processed.
 */
public class AttributeInput extends NonExecutableInput<Attribute> {
    public AttributeInput(Source source, Expression expression, Attribute context) {
        super(source, expression, context);
    }

    @Override
    protected NodeInfo<AttributeInput> info() {
        return NodeInfo.create(this, AttributeInput::new, expression(), context());
    }

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return true;
    }

    @Override
    public Pipe resolveAttributes(AttributeResolver resolver) {
        return new ReferenceInput(source(), expression(), resolver.resolve(context()));
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        // Nothing to extract
    }
}
