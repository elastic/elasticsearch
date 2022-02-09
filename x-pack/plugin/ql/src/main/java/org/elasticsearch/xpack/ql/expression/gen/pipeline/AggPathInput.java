/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.pipeline;

import org.elasticsearch.xpack.ql.execution.search.AggRef;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class AggPathInput extends CommonNonExecutableInput<AggRef> {

    // used in case the agg itself is not returned in a suitable format (like date aggs)
    private final Processor action;

    public AggPathInput(Expression expression, AggRef context) {
        this(Source.EMPTY, expression, context, null);
    }

    /**
     *
     * Constructs a new <code>AggPathInput</code> instance.
     * The action is used for handling corner-case results such as date histogram which returns
     * a full date object for year which requires additional extraction.
     */
    public AggPathInput(Source source, Expression expression, AggRef context, Processor action) {
        super(source, expression, context);
        this.action = action;
    }

    @Override
    protected NodeInfo<AggPathInput> info() {
        return NodeInfo.create(this, AggPathInput::new, expression(), context(), action);
    }

    public Processor action() {
        return action;
    }

    @Override
    public boolean resolved() {
        return true;
    }

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(context(), action);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        AggPathInput other = (AggPathInput) obj;
        return Objects.equals(context(), other.context()) && Objects.equals(action, other.action);
    }
}
