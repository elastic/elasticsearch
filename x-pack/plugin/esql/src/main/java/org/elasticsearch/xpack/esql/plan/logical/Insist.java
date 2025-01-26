/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Insist extends UnaryPlan {
    private final UnresolvedAttribute insistIdentifier;

    public Insist(Source source, UnresolvedAttribute insistIdentifier, LogicalPlan child) {
        super(source, child);
        this.insistIdentifier = insistIdentifier;
    }

    private @Nullable List<Attribute> lazyOutput = null;

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = computeOutput();
        }
        return lazyOutput;
    }

    private List<Attribute> computeOutput() {
        var result = new ArrayList<>(child().output());
        result.add(insistIdentifier);
        return result;
    }

    public UnresolvedAttribute getInsistIdentifier() {
        return insistIdentifier;
    }

    @Override
    public Insist replaceChild(LogicalPlan newChild) {
        return new Insist(source(), insistIdentifier, newChild);
    }

    @Override
    public String commandName() {
        return "INSIST";
    }

    @Override
    public boolean expressionsResolved() {
        return computeOutput().stream().allMatch(Attribute::resolved);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Insist::new, insistIdentifier, child());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Objects.hashCode(insistIdentifier));
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && ((Insist) obj).insistIdentifier.equals(insistIdentifier);
    }
}
