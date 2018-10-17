/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

/**
 * An input that must first be rewritten against the rest of the query
 * before it can be further processed.
 */
public class AttributeInput extends NonExecutableInput<Attribute> {
    public AttributeInput(Location location, Expression expression, Attribute context) {
        super(location, expression, context);
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
        return new ReferenceInput(location(), expression(), resolver.resolve(context()));
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        // Nothing to extract
    }
}
