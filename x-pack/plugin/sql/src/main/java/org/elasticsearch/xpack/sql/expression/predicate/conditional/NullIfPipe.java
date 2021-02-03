/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class NullIfPipe extends BinaryPipe {

    public NullIfPipe(Source source, Expression expression, Pipe left, Pipe right) {
        super(source, expression, left, right);
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new NullIfPipe(source(), expression(), left, right);
    }

    @Override
    protected NodeInfo<NullIfPipe> info() {
        return NodeInfo.create(this, NullIfPipe::new, expression(), children().get(0), children().get(1));
    }

    @Override
    public Processor asProcessor() {
        return new NullIfProcessor(left().asProcessor(), right().asProcessor());
    }
}
