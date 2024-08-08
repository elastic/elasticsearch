/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;

import java.util.List;
import java.util.Objects;

public class EsSourceExec extends LeafExec {

    private final EsIndex index;
    private final List<Attribute> attributes;
    private final QueryBuilder query;
    private final IndexMode indexMode;

    public EsSourceExec(EsRelation relation) {
        this(relation.source(), relation.index(), relation.output(), null, relation.indexMode());
    }

    public EsSourceExec(Source source, EsIndex index, List<Attribute> attributes, QueryBuilder query, IndexMode indexMode) {
        super(source);
        this.index = index;
        this.attributes = attributes;
        this.query = query;
        this.indexMode = indexMode;
    }

    public EsIndex index() {
        return index;
    }

    public QueryBuilder query() {
        return query;
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EsSourceExec::new, index, attributes, query, indexMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsSourceExec other = (EsSourceExec) obj;
        return Objects.equals(index, other.index) && Objects.equals(query, other.query);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + index + "]" + NodeUtils.limitedToString(attributes);
    }
}
