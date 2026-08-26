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

    /**
     * Execution mode for CATEGORIZE groupings in a distributed plan.
     * {@link #SINGLE} is the default and means local-only execution (no exchange involved, or
     * no CATEGORIZE in the groupings). {@link #INITIAL} runs on data nodes and emits serialized
     * categorizer state alongside the filtered rows. {@link #FINAL} runs on the coordinator,
     * merges states from all data nodes, and applies the global limit.
     *
     * <p>Not serialized — determined locally by {@link
     * org.elasticsearch.xpack.esql.planner.mapper.Mapper} (coordinator) and {@link
     * org.elasticsearch.xpack.esql.planner.mapper.LocalMapper} (data nodes), mirroring the
     * pattern used by {@code TopNByExec.outputOrdering}.
     */
    public enum CategorizeGroupingMode {
        SINGLE,
        INITIAL,
        FINAL
    }

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
     * Defaults to {@link CategorizeGroupingMode#SINGLE}.
     */
    private final CategorizeGroupingMode mode;

    /**
     * Snapshot of the logical output taken at INITIAL-mode creation time.
     * <p>
     * In {@link CategorizeGroupingMode#INITIAL} mode, the data node's local physical optimizer
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
        this(source, child, limitPerGroup, groupings, estimatedRowSize, CategorizeGroupingMode.SINGLE);
    }

    public LimitByExec(
        Source source,
        PhysicalPlan child,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize,
        CategorizeGroupingMode mode
    ) {
        this(source, child, limitPerGroup, groupings, estimatedRowSize, mode, null);
    }

    private LimitByExec(
        Source source,
        PhysicalPlan child,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize,
        CategorizeGroupingMode mode,
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
        return new LimitByExec(
            source(),
            child(),
            limitPerGroup,
            groupings,
            estimatedRowSize,
            CategorizeGroupingMode.INITIAL,
            logicalOutput
        );
    }

    public LimitByExec withFinalMode() {
        return new LimitByExec(source(), child(), limitPerGroup, groupings, estimatedRowSize, CategorizeGroupingMode.FINAL, null);
    }

    @Override
    public List<Attribute> output() {
        // In INITIAL mode the local physical optimizer may add _doc to the child plan's output
        // for late materialization. Return the snapshot taken at INITIAL-mode creation time so
        // LocalPhysicalPlanOptimizer.verify sees a stable output.
        if (mode == CategorizeGroupingMode.INITIAL && initialCategorizeOutput != null) {
            return initialCategorizeOutput;
        }
        return super.output();
    }

    public CategorizeGroupingMode mode() {
        return mode;
    }

    private static LimitByExec readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        PhysicalPlan child = in.readNamedWriteable(PhysicalPlan.class);
        Expression limit = in.readNamedWriteable(Expression.class);
        Integer estimatedRowSize = in.readOptionalVInt();
        List<Expression> groupings = in.readNamedWriteableCollectionAsList(Expression.class);
        return new LimitByExec(source, child, limit, groupings, estimatedRowSize, CategorizeGroupingMode.SINGLE, null);
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
        return Objects.hash(limitPerGroup, groupings, estimatedRowSize, child());
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
            && Objects.equals(child(), other.child());
    }
}
