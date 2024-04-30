/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.options.EsSourceOptions;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

/**
 * Unresolved logical plan from METRICS command
 */
public final class UnresolvedMetrics extends EsqlUnresolvedRelation {
    // TODO: add _tsid and @timestamp field
    private final List<Expression> groupings;
    private final List<? extends NamedExpression> aggregates;

    public UnresolvedMetrics(
        Source source,
        TableIdentifier table,
        List<Attribute> metadataFields,
        EsSourceOptions esSourceOptions,
        String unresolvedMessage,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates
    ) {
        super(source, table, metadataFields, esSourceOptions, unresolvedMessage);
        this.groupings = groupings;
        this.aggregates = aggregates;
    }

    @Override
    protected NodeInfo<UnresolvedRelation> info() {
        return NodeInfo.create(
            this,
            UnresolvedMetrics::new,
            table(),
            metadataFields(),
            esSourceOptions(),
            unresolvedMessage(),
            groupings,
            aggregates
        );
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && Resolvables.resolved(groupings) && Resolvables.resolved(aggregates);
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        UnresolvedMetrics that = (UnresolvedMetrics) o;
        return Objects.equals(groupings, that.groupings) && Objects.equals(aggregates, that.aggregates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), groupings, aggregates);
    }
}
