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
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for {@code LIMIT N BY expr1, expr2, ...}.
 * Retains at most N rows per group defined by the grouping expressions.
 */
public class LimitByExec extends UnaryExec implements EstimatesRowSize {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "LimitByExec",
        LimitByExec::readFrom
    );

    private final Expression limitPerGroup;
    private final List<Expression> groupings;
    private final Integer estimatedRowSize;

    /**
     * Local-only execution mode for CATEGORIZE groupings. Not serialized.
     * Defaults to {@link AggregatorMode#SINGLE}.
     * <ul>
     *   <li>{@link AggregatorMode#SINGLE} — default; local-only execution (no exchange, or no CATEGORIZE in the groupings).
     *   <li>{@link AggregatorMode#INITIAL} — data-node driver; appends serialized categorizer state to each output page.
     *   <li>{@link AggregatorMode#INTERMEDIATE} — node-reduce driver; merges shard-level states, re-emits node-level state.
     *   <li>{@link AggregatorMode#FINAL} — coordinator; merges node-level states, produces final output without a state channel.
     * </ul>
     */
    private final AggregatorMode mode;

    /**
     * Snapshot of the logical output taken at INITIAL-mode creation time.
     * <p>
     * In {@link AggregatorMode#INITIAL} mode, the data node's local physical optimizer
     * may add technical fields (e.g. {@code _doc}) to the child plan's output for late
     * materialization. Because {@link #output()} delegates to {@code child().output()}, those
     * extra fields would propagate upward, causing {@code LocalPhysicalPlanOptimizer.verify} to
     * fail. Storing the output at creation time and returning it from {@link #output()} makes the
     * declared output stable across local optimization.
     * </p>
     * Not serialized — only used on the data node.
     */
    private final List<Attribute> initialCategorizeOutput;

    public LimitByExec(Source source, PhysicalPlan child, Expression limitPerGroup, List<Expression> groupings, Integer estimatedRowSize) {
        this(source, child, limitPerGroup, groupings, estimatedRowSize, AggregatorMode.SINGLE);
    }

    public LimitByExec(
        Source source,
        PhysicalPlan child,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize,
        AggregatorMode mode
    ) {
        this(source, child, limitPerGroup, groupings, estimatedRowSize, mode, null);
    }

    private LimitByExec(
        Source source,
        PhysicalPlan child,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize,
        AggregatorMode mode,
        List<Attribute> initialCategorizeOutput
    ) {
        super(source, child);
        this.limitPerGroup = limitPerGroup;
        this.groupings = groupings;
        this.estimatedRowSize = estimatedRowSize;
        this.mode = mode;
        this.initialCategorizeOutput = initialCategorizeOutput;
    }

    /**
     * Sets INITIAL mode and records the logical output as the stable declared output.
     * The logical output must match {@code ExchangeExec.output()} so that the coordinator-side
     * {@code channelsBefore} computation is consistent with the data-node channel layout.
     */
    public LimitByExec withInitialCategorizeMode(List<Attribute> logicalOutput) {
        return new LimitByExec(source(), child(), limitPerGroup, groupings, estimatedRowSize, AggregatorMode.INITIAL, logicalOutput);
    }

    /**
     * Creates a node-reduce (INTERMEDIATE) copy of this node. The reduce plan is never locally
     * optimized, so {@code super.output()} (= child output = {@code ExchangeSourceExec.output()})
     * is already stable; no snapshot is required.
     */
    public LimitByExec withIntermediateCategorizeMode() {
        return new LimitByExec(source(), child(), limitPerGroup, groupings, estimatedRowSize, AggregatorMode.INTERMEDIATE, null);
    }

    public LimitByExec withFinalCategorizeMode() {
        return new LimitByExec(source(), child(), limitPerGroup, groupings, estimatedRowSize, AggregatorMode.FINAL, null);
    }

    @Override
    public List<Attribute> output() {
        // In INITIAL mode the local physical optimizer may add _doc to the child plan's output
        // for late materialization. Return the snapshot taken at INITIAL-mode creation time so
        // LocalPhysicalPlanOptimizer.verify sees a stable output.
        if (mode == AggregatorMode.INITIAL && initialCategorizeOutput != null) {
            return initialCategorizeOutput;
        }
        return super.output();
    }

    public AggregatorMode mode() {
        return mode;
    }

    private static LimitByExec readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        PhysicalPlan child = in.readNamedWriteable(PhysicalPlan.class);
        Expression limit = in.readNamedWriteable(Expression.class);
        Integer estimatedRowSize = in.readOptionalVInt();
        List<Expression> groupings = in.readNamedWriteableCollectionAsList(Expression.class);
        return new LimitByExec(source, child, limit, groupings, estimatedRowSize, AggregatorMode.SINGLE, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(limitPerGroup());
        out.writeOptionalVInt(estimatedRowSize);
        out.writeNamedWriteableCollection(groupings());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends LimitByExec> info() {
        // Capture initialCategorizeOutput so that plan-transformation rules that rebuild this
        // node via the NodeInfo factory (instead of replaceChild) preserve the output snapshot
        // required by INITIAL-mode CATEGORIZE execution.
        List<Attribute> capturedOutput = initialCategorizeOutput;
        return NodeInfo.create(
            this,
            (src, child, lim, grp, size, m) -> new LimitByExec(src, child, lim, grp, size, m, capturedOutput),
            child(),
            limitPerGroup,
            groupings,
            estimatedRowSize,
            mode
        );
    }

    @Override
    public LimitByExec replaceChild(PhysicalPlan newChild) {
        return new LimitByExec(source(), newChild, limitPerGroup, groupings, estimatedRowSize, mode, initialCategorizeOutput);
    }

    public Expression limitPerGroup() {
        return limitPerGroup;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public PhysicalPlan estimateRowSize(State unused) {
        final List<Attribute> output = output();
        EstimatesRowSize.State state = new EstimatesRowSize.State();
        final boolean needsSortedDocIds = output.stream().anyMatch(a -> a.dataType() == DataType.DOC_DATA_TYPE);
        state.add(needsSortedDocIds, output);
        int size = state.consumeAllFields(true);
        size = Math.max(size, 1);
        return Objects.equals(this.estimatedRowSize, size)
            ? this
            : new LimitByExec(source(), child(), limitPerGroup, groupings, size, mode, initialCategorizeOutput);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limitPerGroup, groupings, estimatedRowSize, mode, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LimitByExec other = (LimitByExec) obj;
        return Objects.equals(limitPerGroup, other.limitPerGroup)
            && Objects.equals(groupings, other.groupings)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && mode == other.mode
            && Objects.equals(child(), other.child());
    }
}
