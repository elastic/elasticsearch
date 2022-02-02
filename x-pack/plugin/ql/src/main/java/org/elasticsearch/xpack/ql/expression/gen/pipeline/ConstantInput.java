/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.pipeline;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

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
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        // Nothing to collect
    }
}
