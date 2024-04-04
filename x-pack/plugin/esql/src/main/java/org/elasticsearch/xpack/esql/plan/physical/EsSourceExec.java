/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelationWithFilter;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer.TRANSLATOR_HANDLER;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;

public class EsSourceExec extends LeafExec {

    private final EsIndex index;
    private final List<Attribute> attributes;
    private QueryBuilder query;

    public EsSourceExec(EsRelation relation) {
        this(relation.source(), relation.index(), relation.output(), null);

        if (relation instanceof EsRelationWithFilter) {
            String fieldName = ((EsRelationWithFilter) relation).getFieldName();
            String queryString = ((EsRelationWithFilter) relation).getQueryString();

            if (fieldName != null && queryString != null) {
                FieldAttribute fa = new FieldAttribute(EMPTY, fieldName, new EsField(fieldName, TEXT, emptyMap(), true));
                MatchQueryPredicate mmqp = new MatchQueryPredicate(relation.source(), fa, queryString, "");
                this.query = TRANSLATOR_HANDLER.asQuery(mmqp).asBuilder();
            }
        }
    }

    public EsSourceExec(Source source, EsIndex index, List<Attribute> attributes, QueryBuilder query) {
        super(source);
        this.index = index;
        this.attributes = attributes;
        this.query = query;
    }

    public EsIndex index() {
        return index;
    }

    public QueryBuilder query() {
        return query;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EsSourceExec::new, index, attributes, query);
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
