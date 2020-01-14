/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.MultiPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class InPipe extends MultiPipe {

    public InPipe(Source source, Expression expression, List<Pipe> pipes) {
        super(source, expression, pipes);
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new InPipe(source(), expression(), newChildren);
    }

    @Override
    protected NodeInfo<InPipe> info() {
        return NodeInfo.create(this, InPipe::new, expression(), children());
    }

    @Override
    public int hashCode() {
        return Objects.hash(children());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        InPipe other = (InPipe) obj;
        return Objects.equals(children(), other.children());
    }

    @Override
    public InProcessor asProcessor(List<Processor> processors) {
        return new InProcessor(processors);
    }
}
