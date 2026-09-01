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
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
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
     * Base logical output snapshot, taken at INITIAL-mode creation time and carried through
     * INTERMEDIATE and FINAL modes. In INITIAL/INTERMEDIATE mode, {@link #output()} appends
     * {@link #catIdAttr} and {@link #stateAttr} to this list. In FINAL mode, only this list is
     * returned. Not serialized.
     */
    private final List<Attribute> baseCategorizeOutput;

    /**
     * Synthetic attribute for the categorizer local-category-ID channel appended by
     * {@link org.elasticsearch.compute.operator.CategorizeEvalOperator} in INITIAL mode.
     * Shared between INITIAL and INTERMEDIATE instances; set on the coordinator via
     * {@link #withFinalMode(List, Attribute, Attribute)}. Not serialized.
     */
    private final Attribute catIdAttr;

    /**
     * Synthetic attribute for the serialized categorizer-state channel. Same lifecycle as
     * {@link #catIdAttr}. Not serialized.
     */
    private final Attribute stateAttr;

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
        this(source, child, limitPerGroup, groupings, estimatedRowSize, mode, null, null, null);
    }

    private LimitByExec(
        Source source,
        PhysicalPlan child,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize,
        AggregatorMode mode,
        List<Attribute> baseCategorizeOutput,
        Attribute catIdAttr,
        Attribute stateAttr
    ) {
        super(source, child);
        this.limitPerGroup = limitPerGroup;
        this.groupings = groupings;
        this.estimatedRowSize = estimatedRowSize;
        this.mode = mode;
        this.baseCategorizeOutput = baseCategorizeOutput;
        this.catIdAttr = catIdAttr;
        this.stateAttr = stateAttr;
    }

    /**
     * Sets INITIAL mode. Generates synthetic {@link #catIdAttr} and {@link #stateAttr} with
     * stable {@link org.elasticsearch.xpack.esql.core.expression.NameId}s so the planner can
     * look them up in the layout by ID rather than by positional offset.
     */
    public LimitByExec withInitialMode(List<Attribute> logicalOutput) {
        Attribute catId = new ReferenceAttribute(Source.EMPTY, null, "__categorize_catId", DataType.INTEGER, Nullability.FALSE, null, true);
        Attribute state = new ReferenceAttribute(Source.EMPTY, null, "__categorize_state", DataType.KEYWORD, Nullability.FALSE, null, true);
        return new LimitByExec(
            source(),
            child(),
            limitPerGroup,
            groupings,
            estimatedRowSize,
            AggregatorMode.INITIAL,
            logicalOutput,
            catId,
            state
        );
    }

    /**
     * Creates a node-reduce (INTERMEDIATE) copy of this node, carrying over the synthetic
     * catId/state attributes so that {@code planLimitByMerge} can look them up by ID.
     */
    public LimitByExec withIntermediateMode() {
        return new LimitByExec(
            source(),
            child(),
            limitPerGroup,
            groupings,
            estimatedRowSize,
            AggregatorMode.INTERMEDIATE,
            baseCategorizeOutput,
            catIdAttr,
            stateAttr
        );
    }

    /**
     * Creates a coordinator (FINAL) copy without catId/state in the declared output.
     * Used for non-CATEGORIZE paths only; for CATEGORIZE use
     * {@link #withFinalMode(List, Attribute, Attribute)}.
     */
    public LimitByExec withFinalMode() {
        return new LimitByExec(source(), child(), limitPerGroup, groupings, estimatedRowSize, AggregatorMode.FINAL, null, null, null);
    }

    /**
     * Creates a coordinator (FINAL) copy with the coordinator-side catId/state attributes.
     * These attributes must match the ones declared in the coordinator's {@link ExchangeExec} output
     * so that {@code planLimitByMerge} can look them up by ID in the exchange-source layout.
     */
    public LimitByExec withFinalMode(List<Attribute> base, Attribute catId, Attribute state) {
        return new LimitByExec(source(), child(), limitPerGroup, groupings, estimatedRowSize, AggregatorMode.FINAL, base, catId, state);
    }

    @Override
    public List<Attribute> output() {
        if (baseCategorizeOutput != null) {
            return switch (mode) {
                case INITIAL, INTERMEDIATE -> {
                    List<Attribute> out = new ArrayList<>(baseCategorizeOutput.size() + 2);
                    out.addAll(baseCategorizeOutput);
                    out.add(catIdAttr);
                    out.add(stateAttr);
                    yield out;
                }
                case FINAL -> baseCategorizeOutput;
                case SINGLE -> super.output();
            };
        }
        return super.output();
    }

    public AggregatorMode mode() {
        return mode;
    }

    /** Base logical output (without synthetic catId/state). Non-null in INITIAL/INTERMEDIATE/FINAL-CATEGORIZE modes. */
    public List<Attribute> baseCategorizeOutput() {
        return baseCategorizeOutput;
    }

    /** Synthetic attribute for the category-ID channel; non-null in INITIAL/INTERMEDIATE/FINAL-CATEGORIZE modes. */
    public Attribute catIdAttr() {
        return catIdAttr;
    }

    /** Synthetic attribute for the serialized-state channel; non-null in INITIAL/INTERMEDIATE/FINAL-CATEGORIZE modes. */
    public Attribute stateAttr() {
        return stateAttr;
    }

    private static LimitByExec readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        PhysicalPlan child = in.readNamedWriteable(PhysicalPlan.class);
        Expression limit = in.readNamedWriteable(Expression.class);
        Integer estimatedRowSize = in.readOptionalVInt();
        List<Expression> groupings = in.readNamedWriteableCollectionAsList(Expression.class);
        return new LimitByExec(source, child, limit, groupings, estimatedRowSize, AggregatorMode.SINGLE, null, null, null);
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
        List<Attribute> capturedBase = baseCategorizeOutput;
        Attribute capturedCatId = catIdAttr;
        Attribute capturedState = stateAttr;
        return NodeInfo.create(
            this,
            (src, child, lim, grp, size, m) -> new LimitByExec(src, child, lim, grp, size, m, capturedBase, capturedCatId, capturedState),
            child(),
            limitPerGroup,
            groupings,
            estimatedRowSize,
            mode
        );
    }

    @Override
    public LimitByExec replaceChild(PhysicalPlan newChild) {
        return new LimitByExec(
            source(),
            newChild,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            mode,
            baseCategorizeOutput,
            catIdAttr,
            stateAttr
        );
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
            : new LimitByExec(source(), child(), limitPerGroup, groupings, size, mode, baseCategorizeOutput, catIdAttr, stateAttr);
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
