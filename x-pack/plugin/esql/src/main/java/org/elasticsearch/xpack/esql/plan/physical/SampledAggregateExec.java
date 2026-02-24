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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * An extension of {@link Aggregate} to perform time-series aggregation per time-series, such as rate or _over_time.
 * The grouping must be `_tsid` and `tbucket` or just `_tsid`.
 */
public class SampledAggregateExec extends AggregateExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "SampledAggregateExec",
        SampledAggregateExec::new
    );

    private final List<? extends NamedExpression> originalAggregates;
    private final Expression sampleProbability;
    private final List<Attribute> originalIntermediateAttributes;

    public SampledAggregateExec(
        Source source,
        PhysicalPlan child,
        List<? extends Expression> groupings,
        List<? extends NamedExpression> aggregates,
        List<? extends NamedExpression> originalAggregates,
        Expression sampleProbability,
        AggregatorMode mode,
        List<Attribute> intermediateAttributes,
        List<Attribute> originalIntermediateAttributes,
        Integer estimatedRowSize
    ) {
        super(source, child, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
        this.originalAggregates = originalAggregates;
        this.sampleProbability = sampleProbability;
        this.originalIntermediateAttributes = originalIntermediateAttributes;
    }

    private SampledAggregateExec(StreamInput in) throws IOException {
        super(in);
        this.originalAggregates = in.readNamedWriteableCollectionAsList(NamedExpression.class);
        this.sampleProbability = in.readNamedWriteable(Expression.class);
        this.originalIntermediateAttributes = in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteableCollection(originalAggregates);
        out.writeNamedWriteable(sampleProbability);
        out.writeNamedWriteableCollection(originalIntermediateAttributes);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<AggregateExec> info() {
        return NodeInfo.create(
            this,
            SampledAggregateExec::new,
            child(),
            groupings(),
            aggregates(),
            originalAggregates,
            sampleProbability,
            getMode(),
            intermediateAttributes(),
            originalIntermediateAttributes,
            estimatedRowSize()
        );
    }

    @Override
    public SampledAggregateExec replaceChild(PhysicalPlan newChild) {
        return new SampledAggregateExec(
            source(),
            newChild,
            groupings(),
            aggregates(),
            originalAggregates,
            sampleProbability,
            getMode(),
            intermediateAttributes(),
            originalIntermediateAttributes,
            estimatedRowSize()
        );
    }

    @Override
    public AggregateExec withAggregates(List<? extends NamedExpression> newAggregates) {
        throw new UnsupportedOperationException("Changing aggregates is not supported for SampledAggregateExec");
    }

    @Override
    public SampledAggregateExec withMode(AggregatorMode newMode) {
        return new SampledAggregateExec(
            source(),
            child(),
            groupings(),
            aggregates(),
            originalAggregates,
            sampleProbability,
            newMode,
            intermediateAttributes(),
            originalIntermediateAttributes,
            estimatedRowSize()
        );
    }

    @Override
    protected AggregateExec withEstimatedSize(int estimatedRowSize) {
        return new SampledAggregateExec(
            source(),
            child(),
            groupings(),
            aggregates(),
            originalAggregates,
            sampleProbability,
            getMode(),
            intermediateAttributes(),
            originalIntermediateAttributes,
            estimatedRowSize
        );
    }

    public List<? extends NamedExpression> originalAggregates() {
        return originalAggregates;
    }

    public Expression sampleProbability() {
        return sampleProbability;
    }

    public List<Attribute> originalIntermediateAttributes() {
        return originalIntermediateAttributes;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SampledAggregateExec other = (SampledAggregateExec) obj;
        return super.equals(other)
            && Objects.equals(originalAggregates, other.originalAggregates)
            && Objects.equals(sampleProbability, other.sampleProbability)
            && Objects.equals(originalIntermediateAttributes, other.originalIntermediateAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originalAggregates, sampleProbability, originalIntermediateAttributes);
    }
}
