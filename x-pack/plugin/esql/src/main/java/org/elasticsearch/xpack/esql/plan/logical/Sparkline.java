/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.SupportsObservabilityTier;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.SupportsObservabilityTier.ObservabilityTier.COMPLETE;

@SupportsObservabilityTier(tier = COMPLETE)
public class Sparkline extends UnaryPlan
    implements
        SurrogateLogicalPlan,
        PostAnalysisVerificationAware,
        ExecutesOn.Coordinator {

    private final Attribute key;
    private final Attribute value;
    private final Attribute group;
    private final Attribute trend;
    private final Attribute timestamps;

    private List<Attribute> output;

    public Sparkline(Source source, LogicalPlan child, Attribute key, Attribute value, Attribute group, Attribute trend, Attribute timestamps) {
        super(source, child);
        this.key = key;
        this.value = value;
        this.group = group;
        this.trend = trend;
        this.timestamps = timestamps;
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
    protected NodeInfo<Sparkline> info() {
        return NodeInfo.create(this, Sparkline::new, child(), key, value, group, trend, timestamps);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Sparkline(source(), newChild, key, value, group, trend, timestamps);
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = NamedExpressions.mergeOutputAttributes(List.of(trend, timestamps), child().output());
        }
        return output;
    }

    public NamedExpression key() {
        return key;
    }

    public NamedExpression value() {
        return value;
    }

    public Attribute group() {
        return group;
    }

    @Override
    protected AttributeSet computeReferences() {
        return Expressions.references(List.of(key, value, group));
    }

    @Override
    public boolean expressionsResolved() {
        return key.resolved() && value.resolved() && (group == null || group.resolved());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key, value, group);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
            && Objects.equals(key, ((Sparkline) other).key)
            && Objects.equals(value, ((Sparkline) other).value)
            && Objects.equals(group, ((Sparkline) other).group);
    }

    @Override
    public LogicalPlan surrogate() {
        List<Expression> groupings = group() == null ? List.of() : List.of(group());
        Literal limit = new Literal(Source.EMPTY, 50, DataType.INTEGER);
        Literal order = new Literal(Source.EMPTY, new BytesRef("asc"), DataType.KEYWORD);
        Top topKeysAgg = new Top(Source.EMPTY, key(), limit, order, null);
        Top topValuesAgg = new Top(Source.EMPTY, key(), limit, order, value());
        Attribute topKeysAlias = new ReferenceAttribute(topKeysAgg.source(), key().name(), DataType.LONG);
        Attribute topValuesAlias = new ReferenceAttribute(topValuesAgg.source(), value().name(), DataType.INTEGER);
        List<Attribute> aggregates = List.of(topKeysAlias, topValuesAlias);
        return new Aggregate(source(), child(), groupings, aggregates);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
    }
}
