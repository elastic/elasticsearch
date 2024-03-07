/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.qlcore.expression.gen.pipeline;

import org.elasticsearch.xpack.qlcore.execution.search.FieldExtraction;
import org.elasticsearch.xpack.qlcore.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;

public class ReferenceInput extends NonExecutableInput<FieldExtraction> {
    public ReferenceInput(Source source, Expression expression, FieldExtraction context) {
        super(source, expression, context);
    }

    @Override
    protected NodeInfo<? extends Pipe> info() {
        return NodeInfo.create(this, ReferenceInput::new, expression(), context());
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
        context().collectFields(sourceBuilder);
    }
}
