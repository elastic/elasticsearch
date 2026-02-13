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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// This unary plan is used to generate empty bucket values for the sparkline aggregate function.
public class SparklineGenerateEmptyBuckets extends UnaryPlan {

    private final Attribute value;
    private final Attribute key;
    private final List<Expression> groupings;
    private final Rounding.Prepared dateBucketRounding;
    private final long minDate;
    private final long maxDate;

    public SparklineGenerateEmptyBuckets(
        Source source,
        LogicalPlan child,
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
        List<Attribute> output = new ArrayList<>();
        output.add(value);
        if (groupings != null) {
            for (Expression grouping : groupings) {
                Attribute groupingAttribute = Expressions.attribute(grouping);
                if (output.contains(groupingAttribute) == false) {
                    output.add(groupingAttribute);
                }
            }
        }
        return output;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new SparklineGenerateEmptyBuckets(source(), newChild, value, key, groupings, dateBucketRounding, minDate, maxDate);
    }

    @Override
    public boolean expressionsResolved() {
        return this.key.resolved() && this.value.resolved();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            SparklineGenerateEmptyBuckets::new,
            child(),
            key,
            value,
            groupings,
            dateBucketRounding,
            minDate,
            maxDate
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
