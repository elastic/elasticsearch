/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Retains a ratio of series using Prometheus-compatible hash sampling: each series is kept or
 * dropped by hashing its series identity, so the kept subset is stable across steps, runs, and
 * shards. The keep/drop decision is per-series stateless, unlike a count-based limit.
 * <p>
 * Like the other reductions ({@code TopNBy}) this is a {@link PipelineBreaker}: it runs on the
 * coordinator after the per-series rows are collected. Pushing the stateless filter itself down
 * to data nodes is a possible follow-up; it is not needed for PromQL compliance since the
 * hashed subset is identical wherever it is computed.
 */
public class LimitRatioBy extends UnaryPlan implements PipelineBreaker {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "LimitRatioBy",
        LimitRatioBy::new
    );

    private final Expression ratio;
    private final List<Expression> groupings;
    /**
     * The per-row series identity the hash filter samples on: the {@code _timeseries} attribute
     * when the input is at series grain, otherwise a translator-synthesized key (for example over
     * an aggregated input whose rows are groups, not series).
     */
    private final Expression seriesKey;

    public LimitRatioBy(Source source, LogicalPlan child, Expression ratio, List<Expression> groupings, Expression seriesKey) {
        super(source, child);
        this.ratio = ratio;
        this.groupings = groupings;
        this.seriesKey = seriesKey;
    }

    private LimitRatioBy(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(ratio());
        out.writeNamedWriteableCollection(groupings());
        out.writeNamedWriteable(seriesKey());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<LimitRatioBy> info() {
        return NodeInfo.create(this, LimitRatioBy::new, child(), ratio, groupings, seriesKey);
    }

    @Override
    public LimitRatioBy replaceChild(LogicalPlan newChild) {
        return new LimitRatioBy(source(), newChild, ratio, groupings, seriesKey);
    }

    public Expression ratio() {
        return ratio;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Expression seriesKey() {
        return seriesKey;
    }

    @Override
    public boolean expressionsResolved() {
        return ratio.resolved() && seriesKey.resolved() && Resolvables.resolved(groupings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ratio, child(), groupings, seriesKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LimitRatioBy other = (LimitRatioBy) obj;
        return Objects.equals(ratio, other.ratio)
            && Objects.equals(child(), other.child())
            && Objects.equals(groupings, other.groupings)
            && Objects.equals(seriesKey, other.seriesKey);
    }
}
