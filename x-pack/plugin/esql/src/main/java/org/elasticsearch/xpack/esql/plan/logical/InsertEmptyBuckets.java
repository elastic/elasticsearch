/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.InsertEmptyBucketsOperator.DefaultValue;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;

import java.io.IOException;
import java.util.Objects;

/**
 * Logical plan for creating empty buckets for STATS ... BY BUCKET(..., {"include_empty_buckets": true}).
 */
public class InsertEmptyBuckets extends UnaryPlan implements ExecutesOn.Coordinator {

    private final AttributeMap<Bucket> buckets;
    private final AttributeSet groups;
    private final AttributeMap<DefaultValue> defaultValues;

    public InsertEmptyBuckets(
        Source source,
        LogicalPlan child,
        AttributeMap<Bucket> buckets,
        AttributeSet groups,
        AttributeMap<DefaultValue> defaultValues
    ) {
        super(source, child);
        this.buckets = buckets;
        this.groups = groups;
        this.defaultValues = defaultValues;
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

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.builder().addAll(buckets.keySet()).addAll(groups).addAll(defaultValues.keySet()).build();
    }

    @Override
    public InsertEmptyBuckets replaceChild(LogicalPlan newChild) {
        return new InsertEmptyBuckets(source(), newChild, buckets, groups, defaultValues);
    }

    @Override
    public boolean expressionsResolved() {
        return child().expressionsResolved();
    }

    @Override
    protected NodeInfo<InsertEmptyBuckets> info() {
        return NodeInfo.create(this, InsertEmptyBuckets::new, child(), buckets, groups, defaultValues);
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
        return Objects.hash(child(), buckets, groups, defaultValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        InsertEmptyBuckets other = (InsertEmptyBuckets) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(buckets, other.buckets)
            && Objects.equals(groups, other.groups)
            && Objects.equals(defaultValues, other.defaultValues);
    }
}
