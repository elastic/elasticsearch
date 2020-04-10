/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.MultiPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class CasePipe extends MultiPipe {

    public CasePipe(Source source, Expression expression, List<Pipe> children) {
        super(source, expression, children);
    }

    @Override
    protected NodeInfo<CasePipe> info() {
        return NodeInfo.create(this, CasePipe::new, expression(), children());
    }

    @Override
    public Pipe replaceChildren(List<Pipe> newChildren) {
        return new CasePipe(source(), expression(), newChildren);
    }

    @Override
    public Processor asProcessor(List<Processor> procs) {
        return new CaseProcessor(procs);
    }
}
