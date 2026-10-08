/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.InsertEmptyBucketsOperator.DefaultValue;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan for creating empty buckets for STATS ... BY BUCKET(..., {"include_empty_buckets": true}).
 */
public class InsertEmptyBucketsExec extends UnaryExec implements EstimatesRowSize {

    private final AttributeMap<Bucket> buckets;
    private final AttributeSet groups;
    private final AttributeMap<DefaultValue> defaultValues;
    private final Integer estimatedRowSize;

    public InsertEmptyBucketsExec(
        Source source,
        PhysicalPlan child,
        AttributeMap<Bucket> buckets,
        AttributeSet groups,
        AttributeMap<DefaultValue> defaultValues,
        Integer estimatedRowSize
    ) {
        super(source, child);
        this.buckets = buckets;
        this.groups = groups;
        this.defaultValues = defaultValues;
        this.estimatedRowSize = estimatedRowSize;
    }

    /**
     * The include-empty {@code BUCKET} groupings, keyed by their output {@link Attribute} in {@link #child()}'s output.
     */
    public AttributeMap<Bucket> buckets() {
        return buckets;
    }

    /**
     * The non-bucket grouping attributes (as they appear in the final output) the empty buckets are cross-produced against.
     */
    public AttributeSet groups() {
        return groups;
    }

    /**
     * Per non-grouping output attribute, the value an empty bucket takes on it.
     */
    public AttributeMap<DefaultValue> defaultValues() {
        return defaultValues;
    }

    /**
     * Estimate of the number of bytes that'll be loaded per position before the stream of pages is consumed.
     */
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
            : new InsertEmptyBucketsExec(source(), child(), buckets, groups, defaultValues, size);
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.builder().addAll(buckets.keySet()).addAll(groups).addAll(defaultValues.keySet()).build();
    }

    @Override
    public InsertEmptyBucketsExec replaceChild(PhysicalPlan newChild) {
        return new InsertEmptyBucketsExec(source(), newChild, buckets, groups, defaultValues, estimatedRowSize);
    }

    @Override
    protected NodeInfo<InsertEmptyBucketsExec> info() {
        return NodeInfo.create(this, InsertEmptyBucketsExec::new, child(), buckets, groups, defaultValues, estimatedRowSize);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("must not leave the coordinator node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("must not leave the coordinator node");
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), buckets, groups, defaultValues, estimatedRowSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        InsertEmptyBucketsExec other = (InsertEmptyBucketsExec) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(buckets, other.buckets)
            && Objects.equals(groups, other.groups)
            && Objects.equals(defaultValues, other.defaultValues)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize);
    }
}
