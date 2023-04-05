/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

@Experimental
public class ExchangeExec extends UnaryExec {
    private final Mode mode;

    public ExchangeExec(Source source, PhysicalPlan child, Mode mode) {
        super(source, child);
        this.mode = mode;
    }

    @Override
    public boolean singleNode() {
        return true;
    }

    public Mode mode() {
        return mode;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new ExchangeExec(source(), newChild, mode);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ExchangeExec::new, child(), mode);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        ExchangeExec that = (ExchangeExec) o;
        return mode == that.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), mode);
    }

    public enum Mode {
        LOCAL,
        REMOTE_SINK,
        REMOTE_SOURCE
    }
}
