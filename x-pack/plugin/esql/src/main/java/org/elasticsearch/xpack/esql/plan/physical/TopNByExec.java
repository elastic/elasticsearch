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
import org.elasticsearch.compute.operator.topn.GroupedTopNOperator.OutputOrdering;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyList;

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
     * Defaults to {@link LimitByExec.CategorizeGroupingMode#SINGLE}.
     * See {@link LimitByExec.CategorizeGroupingMode} for semantics.
     */
    private final LimitByExec.CategorizeGroupingMode categorizeMode;

    /**
     * For {@link LimitByExec.CategorizeGroupingMode#FINAL}: the extra intermediate attributes
     * on the exchange (category-ID + serialized-state attributes per CATEGORIZE grouping).
     * Not serialized.
     */
    private final List<Attribute> intermediateAttributes;

    /**
     * Snapshot of {@code child().output()} taken at INITIAL-mode creation time.
     * <p>
     * In {@link LimitByExec.CategorizeGroupingMode#INITIAL} mode, the data node's local physical
     * optimizer may add technical fields (e.g. {@code _doc}) to the child plan's output for late
     * materialization. Because the default {@link #output()} delegates to {@code child().output()},
     * those extra fields would propagate upward, causing {@code LocalPhysicalPlanOptimizer.verify}
     * to fail. Storing the output at creation time and returning it from {@link #output()} makes
     * the declared output stable across local optimization.
     * </p>
     * Not serialized — only used on the data node.
     */
    private final List<Attribute> initialCategorizeOutput;

    public TopNByExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize
    ) {
        this(
            source,
            child,
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            Set.of(),
            OutputOrdering.SORTED,
            LimitByExec.CategorizeGroupingMode.SINGLE,
            emptyList(),
            null
        );
    }

    private TopNByExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize,
        Set<Attribute> docValuesAttributes,
        OutputOrdering outputOrdering
    ) {
        this(
            source,
            child,
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            LimitByExec.CategorizeGroupingMode.SINGLE,
            emptyList(),
            null
        );
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
        LimitByExec.CategorizeGroupingMode categorizeMode,
        List<Attribute> intermediateAttributes,
        List<Attribute> initialCategorizeOutput
    ) {
        super(source, child);
        this.order = order;
        this.limitPerGroup = limitPerGroup;
        this.groupings = groupings;
        this.estimatedRowSize = estimatedRowSize;
        this.docValuesAttributes = docValuesAttributes;
        this.outputOrdering = outputOrdering;
        this.categorizeMode = categorizeMode;
        this.intermediateAttributes = intermediateAttributes;
        this.initialCategorizeOutput = initialCategorizeOutput;
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
        // docValueAttributes, outputOrdering, categorizeMode, and intermediateAttributes are only
        // used on the local node and never serialized.
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
        return NodeInfo.create(this, TopNByExec::new, child(), order, limitPerGroup, groupings, estimatedRowSize);
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
            categorizeMode,
            intermediateAttributes,
            initialCategorizeOutput
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
            categorizeMode,
            intermediateAttributes,
            initialCategorizeOutput
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
            categorizeMode,
            intermediateAttributes,
            initialCategorizeOutput
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
            categorizeMode,
            intermediateAttributes,
            initialCategorizeOutput
        );
    }

    public TopNByExec withCategorizeMode(LimitByExec.CategorizeGroupingMode newMode) {
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            newMode,
            emptyList(),
            null
        );
    }

    /**
     * Sets INITIAL mode and records the logical output as the stable declared output.
     * The logical output must match {@code ExchangeExec.output()} so that the coordinator-side
     * {@code channelsBefore} computation is consistent with the data-node channel layout.
     */
    public TopNByExec withInitialCategorizeMode(List<Attribute> logicalOutput) {
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            LimitByExec.CategorizeGroupingMode.INITIAL,
            emptyList(),
            logicalOutput
        );
    }

    public TopNByExec withFinalCategorizeMode(List<Attribute> newIntermediateAttributes) {
        return new TopNByExec(
            source(),
            child(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            LimitByExec.CategorizeGroupingMode.FINAL,
            newIntermediateAttributes,
            null
        );
    }

    @Override
    public List<Attribute> output() {
        // In INITIAL mode the local physical optimizer may add _doc to the child plan's output
        // for late materialization. Return the snapshot taken at INITIAL-mode creation time so
        // LocalPhysicalPlanOptimizer.verify sees a stable output.
        if (categorizeMode == LimitByExec.CategorizeGroupingMode.INITIAL && initialCategorizeOutput != null) {
            return initialCategorizeOutput;
        }
        return super.output();
    }

    public LimitByExec.CategorizeGroupingMode categorizeMode() {
        return categorizeMode;
    }

    public List<Attribute> intermediateAttributes() {
        return intermediateAttributes;
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
                categorizeMode,
                intermediateAttributes,
                initialCategorizeOutput
            );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            order,
            limitPerGroup,
            groupings,
            estimatedRowSize,
            docValuesAttributes,
            outputOrdering,
            categorizeMode,
            intermediateAttributes
        );
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
                && categorizeMode == other.categorizeMode
                && Objects.equals(intermediateAttributes, other.intermediateAttributes);
        }
        return equals;
    }
}
