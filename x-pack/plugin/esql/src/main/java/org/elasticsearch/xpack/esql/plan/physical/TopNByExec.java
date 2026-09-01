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
import org.elasticsearch.compute.operator.topn.GroupedTopNOperator.OutputOrdering;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Physical plan node for {@code SORT order1, order2 | LIMIT N BY grouping1, grouping2, ...}.
 * Sorts the input rows retaining at most N rows per group defined by the grouping expressions.
 */
public class TopNByExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "TopNByExec",
        TopNByExec::new
    );

    private final Expression limitPerGroup;
    private final List<Order> order;
    private final List<Expression> groupings;

    /**
     * Attributes that may be extracted as doc values even if that makes them
     * less accurate. This is mostly used for geo fields which lose a lot of
     * precision in their doc values, but in some cases doc values provides
     * <strong>enough</strong> precision to do the job.
     * <p>
     * This is never serialized between nodes and only used locally.
     * </p>
     */
    private final Set<Attribute> docValuesAttributes;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final Integer estimatedRowSize;

    /**
     * Whether {@link org.elasticsearch.compute.operator.topn.GroupedQueue#popAll} should sort rows
     * by sort key when building the output. Only the coordinator final reduce needs sorted output;
     * data nodes can skip this sort since their partial results are merged again upstream.
     * <p>
     * This is never serialized between nodes and only used locally.
     * </p>
     */
    private final OutputOrdering outputOrdering;

    /**
     * Local-only execution mode for CATEGORIZE groupings. Not serialized.
     * Defaults to {@link AggregatorMode#SINGLE}.
     * See {@link AggregatorMode} for semantics.
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
     * Synthetic attribute for the categorizer local-category-ID channel. Same lifecycle as
     * {@link LimitByExec#catIdAttr()}. Not serialized.
     */
    private final Attribute catIdAttr;

    /**
     * Synthetic attribute for the serialized categorizer-state channel. Same lifecycle as
     * {@link LimitByExec#stateAttr()}. Not serialized.
     */
    private final Attribute stateAttr;

    public TopNByExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize
    ) {
        this(source, child, order, limitPerGroup, groupings, estimatedRowSize, AggregatorMode.SINGLE);
    }

    public TopNByExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize,
        AggregatorMode mode
    ) {
        this(source, child, order, limitPerGroup, groupings, estimatedRowSize, Set.of(), OutputOrdering.SORTED, mode, null, null, null);
    }

    private TopNByExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize,
        Set<Attribute> docValuesAttributes,
        OutputOrdering outputOrdering,
        AggregatorMode mode,
        List<Attribute> baseCategorizeOutput,
        Attribute catIdAttr,
        Attribute stateAttr
    ) {
        super(source, child);
        this.order = order;
        this.limitPerGroup = limitPerGroup;
        this.groupings = groupings;
        this.estimatedRowSize = estimatedRowSize;
        this.docValuesAttributes = docValuesAttributes;
        this.outputOrdering = outputOrdering;
        this.mode = mode;
        this.baseCategorizeOutput = baseCategorizeOutput;
        this.catIdAttr = catIdAttr;
        this.stateAttr = stateAttr;
    }

    private TopNByExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readCollectionAsList(Order::new),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readOptionalVInt()
        );
        // docValueAttributes, outputOrdering, mode, baseCategorizeOutput, catIdAttr, and stateAttr
        // are only used on the local node and never serialized.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(order());
        out.writeNamedWriteable(limitPerGroup());
        out.writeNamedWriteableCollection(groupings());
        out.writeOptionalVInt(estimatedRowSize());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<TopNByExec> info() {
        Set<Attribute> capturedDocValuesAttributes = docValuesAttributes;
        OutputOrdering capturedOutputOrdering = outputOrdering;
        List<Attribute> capturedBase = baseCategorizeOutput;
        Attribute capturedCatId = catIdAttr;
        Attribute capturedState = stateAttr;
        return NodeInfo.create(
            this,
            (src, child, ord, lim, grp, size, m) -> new TopNByExec(
                src,
                child,
                ord,
                lim,
                grp,
                size,
                capturedDocValuesAttributes,
                capturedOutputOrdering,
                m,
                capturedBase,
                capturedCatId,
                capturedState
            ),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            mode
        );
    }

    @Override
    public TopNByExec replaceChild(PhysicalPlan newChild) {
        return new TopNByExec(
            source(),
            newChild,
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            mode,
            baseCategorizeOutput,
            catIdAttr,
            stateAttr
        );
    }

    public TopNByExec withDocValuesAttributes(Set<Attribute> docValuesAttributes) {
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            mode,
            baseCategorizeOutput,
            catIdAttr,
            stateAttr
        );
    }

    public TopNByExec withSortedOutput() {
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            OutputOrdering.SORTED,
            mode,
            baseCategorizeOutput,
            catIdAttr,
            stateAttr
        );
    }

    public TopNByExec withNonSortedOutput() {
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            OutputOrdering.NOT_SORTED,
            mode,
            baseCategorizeOutput,
            catIdAttr,
            stateAttr
        );
    }

    /**
     * Sets INITIAL mode. Generates synthetic catId/state attributes; see
     * {@link LimitByExec#withInitialMode(List)} for full documentation.
     */
    public TopNByExec withInitialMode(List<Attribute> logicalOutput) {
        Attribute catId = new ReferenceAttribute(Source.EMPTY, null, "__categorize_catId", DataType.INTEGER, Nullability.FALSE, null, true);
        Attribute state = new ReferenceAttribute(Source.EMPTY, null, "__categorize_state", DataType.KEYWORD, Nullability.FALSE, null, true);
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            AggregatorMode.INITIAL,
            logicalOutput,
            catId,
            state
        );
    }

    /**
     * Creates a node-reduce (INTERMEDIATE) copy, carrying over catId/state attributes.
     * See {@link LimitByExec#withIntermediateMode()} for full documentation.
     */
    public TopNByExec withIntermediateMode() {
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            AggregatorMode.INTERMEDIATE,
            baseCategorizeOutput,
            catIdAttr,
            stateAttr
        );
    }

    /**
     * Creates a coordinator (FINAL) copy without catId/state in the declared output.
     * For CATEGORIZE use {@link #withFinalMode(List, Attribute, Attribute)}.
     */
    public TopNByExec withFinalMode() {
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            AggregatorMode.FINAL,
            null,
            null,
            null
        );
    }

    /**
     * Creates a coordinator (FINAL) copy with coordinator-side catId/state attributes.
     * See {@link LimitByExec#withFinalMode(List, Attribute, Attribute)} for full documentation.
     */
    public TopNByExec withFinalMode(List<Attribute> base, Attribute catId, Attribute state) {
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            AggregatorMode.FINAL,
            base,
            catId,
            state
        );
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

    public OutputOrdering outputOrdering() {
        return outputOrdering;
    }

    public Expression limitPerGroup() {
        return limitPerGroup;
    }

    public List<Order> order() {
        return order;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Set<Attribute> docValuesAttributes() {
        return docValuesAttributes;
    }

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        final List<Attribute> output = output();
        final boolean needsSortedDocIds = output.stream().anyMatch(a -> a.dataType() == DataType.DOC_DATA_TYPE);
        state.add(needsSortedDocIds, output);
        int size = state.consumeAllFields(true);
        size = Math.max(size, 1);
        return Objects.equals(this.estimatedRowSize, size)
            ? this
            : new TopNByExec(
                source(),
                child(),
                order,
                limitPerGroup,
                groupings,
                size,
                docValuesAttributes,
                outputOrdering,
                mode,
                baseCategorizeOutput,
                catIdAttr,
                stateAttr
            );
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limitPerGroup, groupings, estimatedRowSize, docValuesAttributes, outputOrdering, mode);
    }

    @Override
    public boolean equals(Object obj) {
        boolean equals = super.equals(obj);
        if (equals) {
            var other = (TopNByExec) obj;
            equals = Objects.equals(order, other.order)
                && Objects.equals(limitPerGroup, other.limitPerGroup)
                && Objects.equals(groupings, other.groupings)
                && Objects.equals(estimatedRowSize, other.estimatedRowSize)
                && Objects.equals(docValuesAttributes, other.docValuesAttributes)
                && outputOrdering == other.outputOrdering
                && mode == other.mode;
        }
        return equals;
    }
}
