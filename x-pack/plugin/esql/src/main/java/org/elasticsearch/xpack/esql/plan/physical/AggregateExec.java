/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class AggregateExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "AggregateExec",
        AggregateExec::new
    );

    private final List<? extends Expression> groupings;
    private final List<? extends NamedExpression> aggregates;
    /**
     * The output attributes of {@link AggregatorMode#INITIAL} and {@link AggregatorMode#INTERMEDIATE} aggregations, resp.
     * the input attributes of {@link AggregatorMode#FINAL} and {@link AggregatorMode#INTERMEDIATE} aggregations.
     */
    private final List<Attribute> intermediateAttributes;

    private final AggregatorMode mode;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final Integer estimatedRowSize;

    public AggregateExec(
        Source source,
        PhysicalPlan child,
        List<? extends Expression> groupings,
        List<? extends NamedExpression> aggregates,
        AggregatorMode mode,
        List<Attribute> intermediateAttributes,
        Integer estimatedRowSize
    ) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
        this.mode = mode;
        this.intermediateAttributes = intermediateAttributes;
        this.estimatedRowSize = estimatedRowSize;
    }

    protected AggregateExec(StreamInput in) throws IOException {
        // This is only deserialized as part of node level reduction, which is turned off until at least 8.16.
        // So, we do not have to consider previous transport versions here, because old nodes will not send AggregateExecs to new nodes.
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            in.readEnum(AggregatorMode.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readOptionalVInt()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(groupings());
        out.writeNamedWriteableCollection(aggregates());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeEnum(getMode());
            out.writeNamedWriteableCollection(intermediateAttributes());
        } else {
            out.writeEnum(AggregateExec.Mode.fromAggregatorMode(getMode()));
        }
        out.writeOptionalVInt(estimatedRowSize());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<AggregateExec> info() {
        return NodeInfo.create(this, AggregateExec::new, child(), groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
    }

    @Override
    public AggregateExec replaceChild(PhysicalPlan newChild) {
        return new AggregateExec(source(), newChild, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
    }

    public List<? extends Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    public AggregateExec withAggregates(List<? extends NamedExpression> newAggregates) {
        return new AggregateExec(source(), child(), groupings, newAggregates, mode, intermediateAttributes, estimatedRowSize);
    }

    public AggregateExec withMode(AggregatorMode newMode) {
        return new AggregateExec(source(), child(), groupings, aggregates, newMode, intermediateAttributes, estimatedRowSize);
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
        state.add(false, aggregates);  // The groupings are contained within the aggregates
        int size = state.consumeAllFields(true);
        size = Math.max(size, 1);
        return Objects.equals(this.estimatedRowSize, size) ? this : withEstimatedSize(size);
    }

    protected AggregateExec withEstimatedSize(int estimatedRowSize) {
        return new AggregateExec(source(), child(), groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
    }

    public AggregatorMode getMode() {
        return mode;
    }

    /**
     * Used only for bwc when de-/serializing.
     */
    @Deprecated
    private enum Mode {
        SINGLE,
        PARTIAL, // maps raw inputs to intermediate outputs
        FINAL; // maps intermediate inputs to final outputs

        static Mode fromAggregatorMode(AggregatorMode aggregatorMode) {
            return switch (aggregatorMode) {
                case SINGLE -> SINGLE;
                case INITIAL -> PARTIAL;
                case FINAL -> FINAL;
                // If needed, we could have this return an PARTIAL instead; that's how intermediate aggs were encoded in the past for
                // data node level reduction.
                case INTERMEDIATE -> throw new UnsupportedOperationException(
                    "cannot turn intermediate aggregation into single, partial or final."
                );
            };
        }
    }

    /**
     * Aggregations are usually performed in two steps, first partial (e.g. locally on a data node) then final (on the coordinator node).
     * These are the intermediate attributes output by a partial aggregation or consumed by a final one.
     * C.f. {@link org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders#intermediateAttributes}.
     */
    public List<Attribute> intermediateAttributes() {
        return intermediateAttributes;
    }

    @Override
    public List<Attribute> output() {
        return mode.isOutputPartial() ? intermediateAttributes : Aggregate.output(aggregates);
    }

    @Override
    protected AttributeSet computeReferences() {
        return mode.isInputPartial()
            ? AttributeSet.of(intermediateAttributes)
            : Aggregate.computeReferences(aggregates, groupings).subtract(AttributeSet.of(ordinalAttributes()));
    }

    /** Returns the attributes that can be loaded from ordinals -- no explicit extraction is needed */
    public List<Attribute> ordinalAttributes() {
        List<Attribute> orginalAttributs = new ArrayList<>(groupings.size());
        // Ordinals can be leveraged just for a single grouping. If there are multiple groupings, fields need to be laoded for the
        // hash aggregator.
        // CATEGORIZE requires the standard hash aggregator as well.
        if (groupings().size() == 1 && groupings.get(0).anyMatch(e -> e instanceof Categorize) == false) {
            var leaves = new HashSet<>();
            aggregates.stream().filter(a -> groupings.contains(a) == false).forEach(a -> leaves.addAll(a.collectLeaves()));
            groupings.forEach(g -> {
                if (leaves.contains(g) == false) {
                    orginalAttributs.add((Attribute) g);
                }
            });
        }
        return orginalAttributs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, mode, intermediateAttributes, estimatedRowSize, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        AggregateExec other = (AggregateExec) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(mode, other.mode)
            && Objects.equals(intermediateAttributes, other.intermediateAttributes)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(child(), other.child());
    }

}
