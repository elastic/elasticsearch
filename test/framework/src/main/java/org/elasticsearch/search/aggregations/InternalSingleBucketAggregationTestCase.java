/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

public abstract class InternalSingleBucketAggregationTestCase<T extends InternalSingleBucketAggregation> extends
    InternalAggregationTestCase<T> {

    private boolean hasInternalMax;
    private boolean hasInternalMin;

    public Supplier<InternalAggregations> subAggregationsSupplier;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        hasInternalMax = randomBoolean();
        hasInternalMin = randomBoolean();
        subAggregationsSupplier = () -> {
            List<InternalAggregation> aggs = new ArrayList<>();
            if (hasInternalMax) {
                aggs.add(new Max("max", randomDouble(), randomNumericDocValueFormat(), emptyMap()));
            }
            if (hasInternalMin) {
                aggs.add(new Min("min", randomDouble(), randomNumericDocValueFormat(), emptyMap()));
            }
            return InternalAggregations.from(aggs);
        };
    }

    protected abstract T createTestInstance(String name, long docCount, InternalAggregations aggregations, Map<String, Object> metadata);

    protected abstract void extraAssertReduced(T reduced, List<T> inputs);

    @Override
    protected final T createTestInstance(String name, Map<String, Object> metadata) {
        // we shouldn't use the full long range here since we sum doc count on reduce, and don't want to overflow the long range there
        long docCount = between(0, Integer.MAX_VALUE);
        return createTestInstance(name, docCount, subAggregationsSupplier.get(), metadata);
    }

    @Override
    protected T mutateInstance(T instance) {
        String name = instance.getName();
        long docCount = instance.getDocCount();
        InternalAggregations aggregations = instance.getAggregations();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> docCount += between(1, 2000);
            case 2 -> {
                List<InternalAggregation> aggs = new ArrayList<>();
                aggs.add(new Max("new_max", randomDouble(), randomNumericDocValueFormat(), emptyMap()));
                aggs.add(new Min("new_min", randomDouble(), randomNumericDocValueFormat(), emptyMap()));
                aggregations = InternalAggregations.from(aggs);
            }
            default -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
        }
        return createTestInstance(name, docCount, aggregations, metadata);
    }

    @Override
    protected final void assertReduced(T reduced, List<T> inputs) {
        assertEquals(inputs.stream().mapToLong(InternalSingleBucketAggregation::getDocCount).sum(), reduced.getDocCount());
        if (hasInternalMax) {
            double expected = inputs.stream().mapToDouble(i -> {
                Max max = i.getAggregations().get("max");
                return max.value();
            }).max().getAsDouble();
            Max reducedMax = reduced.getAggregations().get("max");
            assertEquals(expected, reducedMax.value(), 0);
        }
        if (hasInternalMin) {
            double expected = inputs.stream().mapToDouble(i -> {
                Min min = i.getAggregations().get("min");
                return min.value();
            }).min().getAsDouble();
            Min reducedMin = reduced.getAggregations().get("min");
            assertEquals(expected, reducedMin.value(), 0);
        }
        extraAssertReduced(reduced, inputs);
    }
}
