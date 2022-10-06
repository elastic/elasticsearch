/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

// TODO not have it unary
public class Exchange extends UnaryPlan {

    public enum Type {
        GATHER, // gathering results from various sources (1:n)
        REPARTITION, // repartitioning results from various sources (n:m)
        // REPLICATE, TODO: implement
    }

    public enum Partitioning {
        SINGLE_DISTRIBUTION, // single exchange source, no partitioning
        FIXED_ARBITRARY_DISTRIBUTION, // multiple exchange sources, random partitioning
        FIXED_BROADCAST_DISTRIBUTION, // multiple exchange sources, broadcasting
        FIXED_PASSTHROUGH_DISTRIBUTION, // n:n forwarding
        // FIXED_HASH_DISTRIBUTION, TODO: implement hash partitioning
    }

    private final Type type;
    private final Partitioning partitioning;

    public Exchange(Source source, LogicalPlan child, Type type, Partitioning partitioning) {
        super(source, child);
        this.type = type;
        this.partitioning = partitioning;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    public Type getType() {
        return type;
    }

    public Partitioning getPartitioning() {
        return partitioning;
    }

    @Override
    public boolean singleNode() {
        if (partitioning == Partitioning.SINGLE_DISTRIBUTION && type == Type.GATHER) {
            return true;
        }
        return child().singleNode();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Exchange(source(), newChild, type, partitioning);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Exchange::new, child(), type, partitioning);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, partitioning, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Exchange other = (Exchange) obj;
        return Objects.equals(type, other.type)
            && Objects.equals(partitioning, other.partitioning)
            && Objects.equals(child(), other.child());
    }
}
