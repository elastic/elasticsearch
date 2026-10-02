/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node corresponding to {@link org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse}.
 * Produces one sparse multi-valued row per series from expanded PromQL input.
 */
public class TimeSeriesCollapseExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "TimeSeriesCollapseExec",
        TimeSeriesCollapseExec::new
    );

    private final Attribute value;
    private final Attribute step;
    private final List<Attribute> dimensions;
    private final long start;
    private final long end;
    private final long stepMillis;

    public TimeSeriesCollapseExec(
        Source source,
        PhysicalPlan child,
        Attribute value,
        Attribute step,
        List<Attribute> dimensions,
        long start,
        long end,
        long stepMillis
    ) {
        super(source, child);
        this.value = value;
        this.step = step;
        this.dimensions = dimensions;
        this.start = start;
        this.end = end;
        this.stepMillis = stepMillis;
    }

    private TimeSeriesCollapseExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readLong(),
            in.readLong(),
            in.readLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(value);
        out.writeNamedWriteable(step);
        out.writeNamedWriteableCollection(dimensions);
        out.writeLong(start);
        out.writeLong(end);
        out.writeLong(stepMillis);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Attribute value() {
        return value;
    }

    public Attribute step() {
        return step;
    }

    public List<Attribute> dimensions() {
        return dimensions;
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    public long stepMillis() {
        return stepMillis;
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    protected NodeInfo<TimeSeriesCollapseExec> info() {
        return NodeInfo.create(this, TimeSeriesCollapseExec::new, child(), value, step, dimensions, start, end, stepMillis);
    }

    @Override
    public TimeSeriesCollapseExec replaceChild(PhysicalPlan newChild) {
        return new TimeSeriesCollapseExec(source(), newChild, value, step, dimensions, start, end, stepMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), value, step, dimensions, start, end, stepMillis);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimeSeriesCollapseExec other = (TimeSeriesCollapseExec) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(value, other.value)
            && Objects.equals(step, other.step)
            && Objects.equals(dimensions, other.dimensions)
            && start == other.start
            && end == other.end
            && stepMillis == other.stepMillis;
    }
}
