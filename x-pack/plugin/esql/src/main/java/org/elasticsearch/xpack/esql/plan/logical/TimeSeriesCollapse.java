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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node that stream-collapses label-contiguous expanded rows into one multi-valued
 * row per series. The {@code timestamp} column and every column in {@code values} become
 * multi-valued; all other columns remain single-valued (dimension/label columns).
 * <p>
 * This node is injected by the PromQL translation rules at the top of the translated plan when
 * {@link org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand#isCollapsed()} is true.
 * That flag is set either when the user writes {@code TS_COLLAPSE} directly after a {@code PROMQL}
 * command, or when the Prometheus {@code query_range} plan builder sets it automatically,
 * enabling {@code PrometheusQueryResponseListener} to read one MV row per series.
 */
public class TimeSeriesCollapse extends UnaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "TimeSeriesCollapse",
        TimeSeriesCollapse::new
    );

    private final Attribute timestamp;
    private final List<Attribute> values;

    public TimeSeriesCollapse(Source source, LogicalPlan child, Attribute timestamp, List<Attribute> values) {
        super(source, child);
        this.timestamp = timestamp;
        this.values = values;
    }

    private TimeSeriesCollapse(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(timestamp);
        out.writeNamedWriteableCollection(values);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Attribute timestamp() {
        return timestamp;
    }

    public List<Attribute> values() {
        return values;
    }

    @Override
    protected AttributeSet computeReferences() {
        return child().outputSet();
    }

    @Override
    public boolean expressionsResolved() {
        return timestamp.resolved() && values.stream().allMatch(Attribute::resolved);
    }

    @Override
    public TimeSeriesCollapse replaceChild(LogicalPlan newChild) {
        return new TimeSeriesCollapse(source(), newChild, timestamp, values);
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, TimeSeriesCollapse::new, child(), timestamp, values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), timestamp, values);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        TimeSeriesCollapse other = (TimeSeriesCollapse) obj;
        return Objects.equals(timestamp, other.timestamp) && Objects.equals(values, other.values);
    }
}
