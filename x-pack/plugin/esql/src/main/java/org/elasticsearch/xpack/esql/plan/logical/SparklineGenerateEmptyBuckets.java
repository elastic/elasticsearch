/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

// This unary plan is used to generate empty bucket values for the sparkline aggregate function.
public class SparklineGenerateEmptyBuckets extends UnaryPlan {

    private final List<Attribute> values;
    private final Attribute key;
    private final List<Expression> groupings;
    private final Rounding.Prepared dateBucketRounding;
    private final long minDate;
    private final long maxDate;
    private final List<Attribute> passthroughAttributes;
    private final List<Attribute> outputAttributes;

    public SparklineGenerateEmptyBuckets(
        Source source,
        LogicalPlan child,
        List<Attribute> values,
        Attribute key,
        List<Expression> groupings,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate,
        List<Attribute> passthroughAttributes,
        List<Attribute> outputAttributes
    ) {
        super(source, child);
        this.values = values;
        this.key = key;
        this.groupings = groupings;
        this.dateBucketRounding = dateBucketRounding;
        this.minDate = minDate;
        this.maxDate = maxDate;
        this.passthroughAttributes = passthroughAttributes;
        this.outputAttributes = outputAttributes;
    }

    public List<Attribute> values() {
        return values;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Rounding.Prepared dateBucketRounding() {
        return dateBucketRounding;
    }

    public long minDate() {
        return minDate;
    }

    public long maxDate() {
        return maxDate;
    }

    public List<Attribute> passthroughAttributes() {
        return passthroughAttributes;
    }

    @Override
    public List<Attribute> output() {
        return outputAttributes;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new SparklineGenerateEmptyBuckets(
            source(),
            newChild,
            values,
            key,
            groupings,
            dateBucketRounding,
            minDate,
            maxDate,
            passthroughAttributes,
            outputAttributes
        );
    }

    @Override
    public boolean expressionsResolved() {
        return this.key.resolved() && this.values.stream().allMatch(Attribute::resolved);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            SparklineGenerateEmptyBuckets::new,
            child(),
            values,
            key,
            groupings,
            dateBucketRounding,
            minDate,
            maxDate,
            passthroughAttributes,
            outputAttributes
        );
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }
}
