/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public class SparklineGenerateEmptyBucketsExec extends UnaryExec {

    private final List<Attribute> values;
    private final List<Expression> groupings;
    private final Rounding.Prepared dateBucketRounding;
    private final long minDate;
    private final long maxDate;
    private final List<Attribute> passthroughAttributes;
    private final List<Attribute> outputAttributes;

    public SparklineGenerateEmptyBucketsExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> values,
        List<Expression> groupings,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate,
        List<Attribute> passthroughAttributes,
        List<Attribute> outputAttributes
    ) {
        super(source, child);
        this.values = values;
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
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new SparklineGenerateEmptyBucketsExec(
            source(),
            newChild,
            values,
            groupings,
            dateBucketRounding,
            minDate,
            maxDate,
            passthroughAttributes,
            outputAttributes
        );
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(
            this,
            SparklineGenerateEmptyBucketsExec::new,
            child(),
            values,
            groupings,
            dateBucketRounding,
            minDate,
            maxDate,
            passthroughAttributes,
            outputAttributes
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }
}
