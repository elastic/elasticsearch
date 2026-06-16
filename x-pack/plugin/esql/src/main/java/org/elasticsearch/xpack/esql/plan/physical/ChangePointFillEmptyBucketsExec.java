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

/**
 * Physical plan for zero-filling missing date bucket rows before {@link ChangePointExec}.
 */
public class ChangePointFillEmptyBucketsExec extends UnaryExec {

    private final Attribute value;
    private final Attribute key;
    private final List<Expression> groupings;
    private final Rounding.Prepared dateBucketRounding;
    private final long minDate;
    private final long maxDate;

    public ChangePointFillEmptyBucketsExec(
        Source source,
        PhysicalPlan child,
        Attribute value,
        Attribute key,
        List<Expression> groupings,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate
    ) {
        super(source, child);
        this.value = value;
        this.key = key;
        this.groupings = groupings;
        this.dateBucketRounding = dateBucketRounding;
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

    public Attribute value() {
        return value;
    }

    public Attribute key() {
        return key;
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

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new ChangePointFillEmptyBucketsExec(source(), newChild, value, key, groupings, dateBucketRounding, minDate, maxDate);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(
            this,
            ChangePointFillEmptyBucketsExec::new,
            child(),
            value,
            key,
            groupings,
            dateBucketRounding,
            minDate,
            maxDate
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
