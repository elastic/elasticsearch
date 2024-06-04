/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.plan.TableIdentifier;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

public class UnresolvedMetrics extends EsqlUnresolvedRelation {
    private final Attribute timestamp;
    private final List<? extends NamedExpression> aggregates;
    private final List<Expression> groupings;

    public UnresolvedMetrics(
        Source source,
        TableIdentifier table,
        String unresolvedMessage,
        Attribute timestamp,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates
    ) {
        super(source, table, List.of(), IndexMode.TIME_SERIES, unresolvedMessage);
        this.timestamp = timestamp;
        this.groupings = groupings;
        this.aggregates = aggregates;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    public Attribute timestamp() {
        return timestamp;
    }

    @Override
    protected NodeInfo<UnresolvedRelation> info() {
        return NodeInfo.create(this, UnresolvedMetrics::new, table(), unresolvedMessage(), timestamp, groupings, aggregates);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        UnresolvedMetrics that = (UnresolvedMetrics) o;
        return Objects.equals(timestamp, that.timestamp)
            && Objects.equals(groupings, that.groupings)
            && Objects.equals(aggregates, that.aggregates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), timestamp, groupings, aggregates);
    }
}
