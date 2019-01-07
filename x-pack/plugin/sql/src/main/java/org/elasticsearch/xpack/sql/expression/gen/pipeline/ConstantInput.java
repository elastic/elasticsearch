/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class ConstantInput extends LeafInput<Object> {

    public ConstantInput(Source source, Expression expression, Object context) {
        super(source, expression, context);
    }

    @Override
    protected NodeInfo<ConstantInput> info() {
        return NodeInfo.create(this, ConstantInput::new, expression(), context());
    }

    @Override
    public Processor asProcessor() {
        return new ConstantProcessor(context());
    }

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return false;
    }

    @Override
    public Pipe resolveAttributes(AttributeResolver resolver) {
        return this;
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        // Nothing to collect
    }
}
