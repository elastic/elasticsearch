/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Logical plan node that collapses expanded rows into one multi-valued row per output series.
 * The {@code step} and {@code value} columns become sparse, positionally-aligned multi-valued
 * columns; the {@code dimensions} stay single-valued grouping keys.
 * <p>
 * The parser stacks this node directly on top of a {@link PromqlCommand} when the user writes
 * {@code TS_COLLAPSE} (and the Prometheus {@code query_range} plan builder does the same so
 * {@code PrometheusQueryResponseListener} can read one MV row per series). The {@code start},
 * {@code end} and {@code stepBucketSize} bounds are {@code null} at parse time and are filled in
 * by {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslateTimeSeriesCollapse}
 * reading from the source {@link PromqlCommand} child; this avoids the divergence that would arise
 * from resolving them independently from the PROMQL evaluation. They stay as ESQL expressions all
 * the way down to the Mapper, which folds them to {@code long} when building the physical
 * {@code TimeSeriesCollapseExec}.
 */
public class TimeSeriesCollapse extends UnaryPlan implements PostAnalysisVerificationAware, PostOptimizationVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "TimeSeriesCollapse",
        TimeSeriesCollapse::new
    );

    /** Minimum transport version that knows how to deserialize this plan node. */
    public static final TransportVersion TS_COLLAPSE = TransportVersion.fromName("ts_collapse");

    private final Attribute value;
    private final Attribute step;
    private final List<Attribute> dimensions;
    /**
     * Null until {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslateTimeSeriesCollapse} populates it from
     * the source {@link PromqlCommand}.
     */
    private final Literal start;
    private final Literal end;
    private final Expression stepBucketSize;

    /** Parse-time constructor; bounds are populated later by {@code TranslateTimeSeriesCollapse}. */
    public TimeSeriesCollapse(Source source, LogicalPlan child, Attribute value, Attribute step, List<Attribute> dimensions) {
        this(source, child, value, step, dimensions, null, null, null);
    }

    public TimeSeriesCollapse(
        Source source,
        LogicalPlan child,
        Attribute value,
        Attribute step,
        List<Attribute> dimensions,
        Literal start,
        Literal end,
        Expression stepBucketSize
    ) {
        super(source, child);
        this.value = value;
        this.step = step;
        this.dimensions = dimensions;
        this.start = start;
        this.end = end;
        this.stepBucketSize = stepBucketSize;
    }

    private TimeSeriesCollapse(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            (Literal) in.readNamedWriteable(Expression.class),
            (Literal) in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(value);
        out.writeNamedWriteable(step);
        out.writeNamedWriteableCollection(dimensions);
        out.writeNamedWriteable(Objects.requireNonNull(start, "TimeSeriesCollapse start not resolved"));
        out.writeNamedWriteable(Objects.requireNonNull(end, "TimeSeriesCollapse end not resolved"));
        out.writeNamedWriteable(Objects.requireNonNull(stepBucketSize, "TimeSeriesCollapse stepBucketSize not resolved"));
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

    public Literal startLiteral() {
        return start;
    }

    public Literal endLiteral() {
        return end;
    }

    public Expression stepBucketSize() {
        return stepBucketSize;
    }

    public long start() {
        Literal lit = Objects.requireNonNull(
            start,
            "TimeSeriesCollapse start not yet populated; TranslateTimeSeriesCollapse must run first"
        );
        if (lit.value() instanceof Number n) {
            return n.longValue();
        }
        throw new QlIllegalArgumentException("TimeSeriesCollapse start must fold to a number, got [{}]", lit);
    }

    public long end() {
        Literal lit = Objects.requireNonNull(end, "TimeSeriesCollapse end not yet populated; TranslateTimeSeriesCollapse must run first");
        if (lit.value() instanceof Number n) {
            return n.longValue();
        }
        throw new QlIllegalArgumentException("TimeSeriesCollapse end must fold to a number, got [{}]", lit);
    }

    public long stepMillis() {
        Expression expr = Objects.requireNonNull(
            stepBucketSize,
            "TimeSeriesCollapse stepBucketSize not yet populated; TranslateTimeSeriesCollapse must run first"
        );
        if (expr.foldable() && expr.fold(FoldContext.small()) instanceof Duration d) {
            return d.toMillis();
        }
        throw new QlIllegalArgumentException("TimeSeriesCollapse stepBucketSize must fold to a duration, got [{}]", expr);
    }

    @Override
    protected AttributeSet computeReferences() {
        return child().outputSet();
    }

    @Override
    public boolean expressionsResolved() {
        return value.resolved() && step.resolved() && dimensions.stream().allMatch(Attribute::resolved);
    }

    @Override
    public TimeSeriesCollapse replaceChild(LogicalPlan newChild) {
        return new TimeSeriesCollapse(source(), newChild, value, step, dimensions, start, end, stepBucketSize);
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, TimeSeriesCollapse::new, child(), value, step, dimensions, start, end, stepBucketSize);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        PromqlCommand source = findSourcePromqlCommand(child());
        if (source == null) {
            failures.add(fail(this, "TS_COLLAPSE must follow a PROMQL command"));
            return;
        }
        if (source.hasTimeRange() == false) {
            failures.add(
                fail(this, "TS_COLLAPSE requires concrete [{}] and [{}] parameters [{}]", PromqlCommand.START, PromqlCommand.END, "")
            );
        }
    }

    /** Walks down through unary wrappers to find the source {@link PromqlCommand}, if any. */
    private static PromqlCommand findSourcePromqlCommand(LogicalPlan plan) {
        while (plan != null) {
            if (plan instanceof PromqlCommand pc) {
                return pc;
            }
            if (plan instanceof UnaryPlan unary) {
                plan = unary.child();
            } else {
                return null;
            }
        }
        return null;
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        validateChildAttribute(value, "value", failures);
        validateChildAttribute(step, "step", failures);
        if (value.dataType() != DataType.DOUBLE) {
            failures.add(fail(value, "TS_COLLAPSE value column must be [double], found [{}]", value.dataType().typeName()));
        }
        if (step.dataType() != DataType.DATETIME && step.dataType() != DataType.LONG) {
            failures.add(fail(step, "TS_COLLAPSE step column must be [datetime] or [long], found [{}]", step.dataType().typeName()));
        }
        if (end() < start()) {
            failures.add(fail(this, "TS_COLLAPSE end [{}] must be greater than or equal to start [{}]", end(), start()));
        }
        if (stepMillis() <= 0) {
            failures.add(fail(this, "TS_COLLAPSE step must be greater than [0ms], found [{}ms]", stepMillis()));
        }

        Set<NameId> seen = new HashSet<>();
        for (Attribute dimension : dimensions) {
            validateChildAttribute(dimension, "dimension", failures);
            if (dimension.id().equals(value.id()) || dimension.id().equals(step.id())) {
                failures.add(fail(dimension, "TS_COLLAPSE dimension [{}] cannot be the value or step column", dimension.name()));
            }
            if (seen.add(dimension.id()) == false) {
                failures.add(fail(dimension, "TS_COLLAPSE dimension [{}] appears more than once", dimension.name()));
            }
        }
    }

    private void validateChildAttribute(Attribute attribute, String role, Failures failures) {
        if (child().output().stream().noneMatch(a -> a.id().equals(attribute.id()))) {
            failures.add(fail(attribute, "TS_COLLAPSE {} column [{}] is not produced by its child", role, attribute.name()));
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value, step, dimensions, start, end, stepBucketSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        TimeSeriesCollapse other = (TimeSeriesCollapse) obj;
        return Objects.equals(value, other.value)
            && Objects.equals(step, other.step)
            && Objects.equals(dimensions, other.dimensions)
            && Objects.equals(start, other.start)
            && Objects.equals(end, other.end)
            && Objects.equals(stepBucketSize, other.stepBucketSize);
    }
}
