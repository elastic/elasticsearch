/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ConcatFunctionPipe extends Pipe {

    private final List<Pipe> values;

    public ConcatFunctionPipe(Source source, Expression expression, List<Pipe> values) {
        super(source, expression, values);
        this.values = values;
    }

    @Override
    public final ConcatFunctionPipe replaceChildren(List<Pipe> newChildren) {
        return new ConcatFunctionPipe(source(), expression(), newChildren);
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        List<Pipe> newValues = new ArrayList<>(values.size());
        for (Pipe v : values) {
            newValues.add(v.resolveAttributes(resolver));
        }

        if (newValues == values) {
            return this;
        }

        return replaceChildrenSameSize(newValues);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        for (Pipe p : values) {
            if (p.supportedByAggsOnlyQuery() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean resolved() {
        for (Pipe p : values) {
            if (p.resolved() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        for (Pipe v : values) {
            v.collectFields(sourceBuilder);
        }
    }

    @Override
    protected NodeInfo<ConcatFunctionPipe> info() {
        return NodeInfo.create(this, ConcatFunctionPipe::new, expression(), values);
    }

    @Override
    public ConcatFunctionProcessor asProcessor() {
        List<Processor> processors = new ArrayList<>(values.size());
        for (Pipe p : values) {
            processors.add(p.asProcessor());
        }
        return new ConcatFunctionProcessor(processors);
    }

    List<Pipe> values() {
        return values;
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return Objects.equals(values(), ((ConcatFunctionPipe) obj).values());
    }
}
