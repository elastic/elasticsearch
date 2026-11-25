/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.BlockFactory;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Measures the heap an operator retains, for comparison with its breaker charge.
 * <p>
 * {@link RamUsageTester} cannot look inside JDK collections and counts only their contents. This adds the smallest
 * hash node and the slot each element needs, so the result is a lower bound of the real heap usage.
 */
final class RetainedOperatorState {

    /** A {@link java.util.HashMap} node holds a hash and three references. */
    private static final long HASH_NODE_BYTES = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    );

    /** The class {@link List#of} returns for one or two elements. */
    private static final Class<?> IMMUTABLE_LIST_OF_TWO = List.of("a", "b").getClass();

    private RetainedOperatorState() {}

    static long bytes(Operator op) {
        return RamUsageTester.ramUsed(op, new RamUsageTester.Accumulator() {
            @Override
            public long accumulateObject(Object object, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
                // Shared infrastructure and inputs, not state the operator accumulates.
                if (object instanceof BlockFactory
                    || object instanceof BigArrays
                    || object instanceof CircuitBreaker
                    || object instanceof MetricsInfoOperator.MetricFieldLookup) {
                    return 0;
                }
                // BigArrays charges these itself, outside the operator's own accounting, so they are not counted. Give the
                // operator a BigArrays on another breaker to compare like with like. The objects an ObjectArray holds
                // belong to the operator, so they are still measured.
                if (object instanceof BigArray || object instanceof BytesRefHash || object instanceof LongLongHash) {
                    if (object instanceof ObjectArray<?> array) {
                        for (long i = 0; i < array.size(); i++) {
                            Object value = array.get(i);
                            if (value != null) {
                                queue.add(value);
                            }
                        }
                    }
                    return 0;
                }
                return super.accumulateObject(object, shallowSize, fieldValues, queue);
            }

            @Override
            public long accumulateArray(Object array, long shallowSize, List<Object> values, Collection<Object> queue) {
                long bytes = super.accumulateArray(array, shallowSize, values, queue);
                if (array.getClass() == IMMUTABLE_LIST_OF_TWO) {
                    // RamUsageTester adds two array headers to every collection, but this list keeps its elements in fields.
                    return bytes - 2L * RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
                }
                return switch (array) {
                    case Map<?, ?> map -> bytes + map.size() * (HASH_NODE_BYTES + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                    case HashSet<?> set -> bytes + set.size() * (HASH_NODE_BYTES + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                    case Collection<?> collection -> bytes + collection.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
                    default -> bytes;
                };
            }
        });
    }
}
