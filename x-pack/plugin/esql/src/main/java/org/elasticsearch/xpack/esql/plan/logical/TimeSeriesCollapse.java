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
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
 * <p>
 * The grouping columns ({@link #dimensions()}) are derived from the child's output at call time
 * rather than stored. {@link #computeReferences()} returns {@code child().outputSet()}, which is
 * what stops {@code PruneColumns} from pruning below this node; that invariant guarantees the
 * derived list stays consistent with whatever the child actually produces.
 */
public class TimeSeriesCollapse extends UnaryPlan implements PostOptimizationVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "TimeSeriesCollapse",
        TimeSeriesCollapse::new
    );

    /** Minimum transport version that knows how to deserialize this plan node. */
    public static final TransportVersion TS_COLLAPSE = TransportVersion.fromName("ts_collapse");

    private final Attribute value;
    private final Attribute step;
    /**
     * Null until {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslateTimeSeriesCollapse} populates it from
     * the source {@link PromqlCommand}.
     */
    private final Literal start;
    private final Literal end;
    private final Expression stepBucketSize;

    /** Parse-time constructor; bounds are populated later by {@code TranslateTimeSeriesCollapse}. */
    public TimeSeriesCollapse(Source source, LogicalPlan child, Attribute value, Attribute step) {
        this(source, child, value, step, null, null, null);
    }

    public TimeSeriesCollapse(
        Source source,
        LogicalPlan child,
        Attribute value,
        Attribute step,
        Literal start,
        Literal end,
        Expression stepBucketSize
    ) {
        super(source, child);
        this.value = value;
        this.step = step;
        this.start = start;
        this.end = end;
        this.stepBucketSize = stepBucketSize;
    }

    private TimeSeriesCollapse(StreamInput in) throws IOException {
        // Call super(source, child) first, then set the remaining final fields directly so we can
        // read and discard the formerly-stored dimension list for wire compatibility.
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(LogicalPlan.class));
        this.value = in.readNamedWriteable(Attribute.class);
        this.step = in.readNamedWriteable(Attribute.class);
        // Dimensions were stored on the wire in older versions; read and discard -- they are now
        // derived from child.output() at runtime (see dimensions()).
        in.readNamedWriteableCollectionAsList(Attribute.class);
        this.start = (Literal) in.readNamedWriteable(Expression.class);
        this.end = (Literal) in.readNamedWriteable(Expression.class);
        this.stepBucketSize = in.readNamedWriteable(Expression.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(value);
        out.writeNamedWriteable(step);
        // Write an empty list in place of the formerly-stored dimensions; the receiver discards
        // whatever list arrives (see StreamInput constructor). Writing an empty list is safe
        // because TimeSeriesCollapse is only serialized when every cluster node supports the
        // ts_collapse transport version, so no node receiving this will be older than this change.
        out.writeNamedWriteableCollection(List.of());
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

    /**
     * The grouping keys for the collapse: every column the child exposes except {@link #value()}
     * and {@link #step()}.
     * <p>
     * This is derived rather than stored. {@link #computeReferences()} returns
     * {@code child().outputSet()}, which is what stops {@code PruneColumns} from pruning below this
     * node; that invariant guarantees the derived set is consistent with what the child actually
     * produces.
     */
    public List<Attribute> dimensions() {
        return child().output().stream().filter(a -> a.id().equals(value.id()) == false && a.id().equals(step.id()) == false).toList();
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
        return value.resolved() && step.resolved();
    }

    @Override
    public TimeSeriesCollapse replaceChild(LogicalPlan newChild) {
        return new TimeSeriesCollapse(source(), newChild, value, step, start, end, stepBucketSize);
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, TimeSeriesCollapse::new, child(), value, step, start, end, stepBucketSize);
    }

    public void verify(Failures failures) {
        Optional<PromqlCommand> source = sourcePromqlCommand();
        if (source.isEmpty()) {
            // Missing Prometheus indices resolve the child to LIMIT 0 before this verifier runs.
            if (isZeroLimit(child()) == false) {
                failures.add(fail(this, "TS_COLLAPSE must follow a PROMQL command"));
            }
        } else if (source.get().hasTimeRange() == false) {
            failures.add(
                fail(this, "TS_COLLAPSE requires concrete [{}] and [{}] parameters [{}]", PromqlCommand.START, PromqlCommand.END, "")
            );
        }
    }

    private Optional<PromqlCommand> sourcePromqlCommand() {
        return child().collectFirstChildren(p -> p instanceof PromqlCommand || p instanceof UnaryPlan == false)
            .stream()
            .filter(PromqlCommand.class::isInstance)
            .map(PromqlCommand.class::cast)
            .findFirst();
    }

    public static boolean isZeroLimit(LogicalPlan plan) {
        return plan instanceof Limit limit
            && limit.limit().foldable()
            && limit.limit().fold(FoldContext.small()) instanceof Number n
            && n.longValue() == 0;
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

    }

    private void validateChildAttribute(Attribute attribute, String role, Failures failures) {
        if (child().output().stream().noneMatch(a -> a.id().equals(attribute.id()))) {
            failures.add(fail(attribute, "TS_COLLAPSE {} column [{}] is not produced by its child", role, attribute.name()));
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value, step, start, end, stepBucketSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        TimeSeriesCollapse other = (TimeSeriesCollapse) obj;
        return Objects.equals(value, other.value)
            && Objects.equals(step, other.step)
            && Objects.equals(start, other.start)
            && Objects.equals(end, other.end)
            && Objects.equals(stepBucketSize, other.stepBucketSize);
    }
}
