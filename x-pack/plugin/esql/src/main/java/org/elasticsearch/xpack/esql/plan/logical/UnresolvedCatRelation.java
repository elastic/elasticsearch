/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

/**
 * Represents an unresolved {@code CAT <endpoint>} source command. Created by the parser and
 * resolved during analysis using data pre-fetched from the CAT transport actions.
 */
public class UnresolvedCatRelation extends LeafPlan implements Unresolvable {

    private final String endpoint;

    public UnresolvedCatRelation(Source source, String endpoint) {
        super(source);
        this.endpoint = endpoint;
    }

    public String endpoint() {
        return endpoint;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<UnresolvedCatRelation> info() {
        return NodeInfo.create(this, UnresolvedCatRelation::new, endpoint);
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    @Override
    public String unresolvedMessage() {
        return "Unknown CAT endpoint [" + endpoint + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), endpoint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UnresolvedCatRelation other = (UnresolvedCatRelation) obj;
        return Objects.equals(endpoint, other.endpoint);
    }

    @Override
    public List<Object> nodeProperties() {
        return singletonList(endpoint);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + "CAT[" + endpoint + "]";
    }
}
