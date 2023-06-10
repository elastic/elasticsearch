/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class FragmentExec extends LeafExec {

    private final LogicalPlan fragment;
    private final QueryBuilder esFilter;

    public FragmentExec(LogicalPlan fragment) {
        this(fragment.source(), fragment, null);
    }

    public FragmentExec(Source source, LogicalPlan fragment, QueryBuilder esFilter) {
        super(fragment.source());
        this.fragment = fragment;
        this.esFilter = esFilter;
    }

    public LogicalPlan fragment() {
        return fragment;
    }

    public QueryBuilder esFilter() {
        return esFilter;
    }

    @Override
    protected NodeInfo<FragmentExec> info() {
        return NodeInfo.create(this, FragmentExec::new, fragment, esFilter);
    }

    @Override
    public List<Attribute> output() {
        return fragment.output();
    }

    @Override
    public int hashCode() {
        return Objects.hash(fragment, esFilter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FragmentExec other = (FragmentExec) obj;
        return Objects.equals(fragment, other.fragment) && Objects.equals(esFilter, other.esFilter);
    }

    @Override
    public String nodeString() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        sb.append("[filter=");
        sb.append(esFilter);
        sb.append("[<>");
        sb.append(fragment.toString());
        sb.append("<>]");
        return sb.toString();
    }
}
