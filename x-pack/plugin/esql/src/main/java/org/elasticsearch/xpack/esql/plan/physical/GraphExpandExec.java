/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.GraphExpand;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan for {@code GRAPH EXPAND}. Mirrors {@link MMRExec}'s "not serialized"
 * stance. The visited-set walk runs in {@code EsqlSession} via subplan phases; this node
 * is the mapped form of {@link GraphExpand} and should not reach local planning with the
 * walk still unresolved.
 */
public class GraphExpandExec extends UnaryExec {
    private final LogicalPlan edgeRelation;
    private final Attribute seedColumn;
    private final Attribute matchField;
    private final List<Attribute> targetFields;
    private final Expression documentFilter;
    private final List<? extends NamedExpression> aggregates;
    private final List<Expression> groupings;
    private final Expression aggregateFilter;
    private final List<Order> sorts;
    private final Expression until;
    private final MapExpression options;
    private final List<Attribute> resultAttributes;

    public GraphExpandExec(
        Source source,
        PhysicalPlan child,
        LogicalPlan edgeRelation,
        Attribute seedColumn,
        Attribute matchField,
        List<Attribute> targetFields,
        @Nullable Expression documentFilter,
        @Nullable List<? extends NamedExpression> aggregates,
        @Nullable List<Expression> groupings,
        @Nullable Expression aggregateFilter,
        @Nullable List<Order> sorts,
        @Nullable Expression until,
        @Nullable MapExpression options,
        @Nullable List<Attribute> resultAttributes
    ) {
        super(source, child);
        this.edgeRelation = edgeRelation;
        this.seedColumn = seedColumn;
        this.matchField = matchField;
        this.targetFields = targetFields;
        this.documentFilter = documentFilter;
        this.aggregates = aggregates;
        this.groupings = groupings;
        this.aggregateFilter = aggregateFilter;
        this.sorts = sorts;
        this.until = until;
        this.options = options;
        this.resultAttributes = resultAttributes;
    }

    public static GraphExpandExec fromLogical(GraphExpand ge, PhysicalPlan child) {
        return new GraphExpandExec(
            ge.source(),
            child,
            ge.edgeRelation(),
            ge.seedColumn(),
            ge.matchField(),
            ge.targetFields(),
            ge.documentFilter(),
            ge.aggregates(),
            ge.groupings(),
            ge.aggregateFilter(),
            ge.sorts(),
            ge.until(),
            ge.options(),
            ge.resultAttributes()
        );
    }

    public LogicalPlan edgeRelation() {
        return edgeRelation;
    }

    public Attribute seedColumn() {
        return seedColumn;
    }

    public Attribute matchField() {
        return matchField;
    }

    public List<Attribute> targetFields() {
        return targetFields;
    }

    public Expression documentFilter() {
        return documentFilter;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Expression aggregateFilter() {
        return aggregateFilter;
    }

    public List<Order> sorts() {
        return sorts;
    }

    public Expression until() {
        return until;
    }

    public MapExpression options() {
        return options;
    }

    public List<Attribute> resultAttributes() {
        return resultAttributes;
    }

    @Override
    public List<Attribute> output() {
        return resultAttributes != null ? resultAttributes : child().output();
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new GraphExpandExec(
            source(),
            newChild,
            edgeRelation,
            seedColumn,
            matchField,
            targetFields,
            documentFilter,
            aggregates,
            groupings,
            aggregateFilter,
            sorts,
            until,
            options,
            resultAttributes
        );
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(
            this,
            GraphExpandExec::new,
            child(),
            edgeRelation,
            seedColumn,
            matchField,
            targetFields,
            documentFilter,
            aggregates,
            groupings,
            aggregateFilter,
            sorts,
            until,
            options,
            resultAttributes
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        GraphExpandExec that = (GraphExpandExec) o;
        return Objects.equals(edgeRelation, that.edgeRelation)
            && Objects.equals(seedColumn, that.seedColumn)
            && Objects.equals(matchField, that.matchField)
            && Objects.equals(targetFields, that.targetFields)
            && Objects.equals(documentFilter, that.documentFilter)
            && Objects.equals(aggregates, that.aggregates)
            && Objects.equals(groupings, that.groupings)
            && Objects.equals(aggregateFilter, that.aggregateFilter)
            && Objects.equals(sorts, that.sorts)
            && Objects.equals(until, that.until)
            && Objects.equals(options, that.options)
            && Objects.equals(resultAttributes, that.resultAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            edgeRelation,
            seedColumn,
            matchField,
            targetFields,
            documentFilter,
            aggregates,
            groupings,
            aggregateFilter,
            sorts,
            until,
            options,
            resultAttributes
        );
    }
}
