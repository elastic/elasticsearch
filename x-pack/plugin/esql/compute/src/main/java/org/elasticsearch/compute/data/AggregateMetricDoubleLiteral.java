/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Objects;

public class AggregateMetricDoubleLiteral {
    private final Double min;
    private final Double max;
    private final Double sum;
    private final Integer count;

    public AggregateMetricDoubleLiteral(Double min, Double max, Double sum, Integer count) {
        this.min = min.isNaN() ? null : min;
        this.max = max.isNaN() ? null : max;
        this.sum = sum.isNaN() ? null : sum;
        this.count = count;
    }

    public Double getMax() {
        return max;
    }

    public Double getMin() {
        return min;
    }

    public Double getSum() {
        return sum;
    }

    public Integer getCount() {
        return count;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var aggMetric = (AggregateMetricDoubleLiteral) obj;
        return min.equals(aggMetric.min) && max.equals(aggMetric.max) && sum.equals(aggMetric.sum) && count.equals(aggMetric.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(max, min, sum, count);
    }
}
