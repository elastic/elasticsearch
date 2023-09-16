/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class BreakingBytesRefBuilderTests extends ESTestCase {
    public void testAddByte() {
        testAgainstOracle(() -> new TestIteration() {
            byte b = randomByte();

            @Override
            public int size() {
                return 1;
            }

            @Override
            public void applyToBuilder(BreakingBytesRefBuilder builder) {
                builder.append(b);
            }

            @Override
            public void applyToOracle(BytesRefBuilder oracle) {
                oracle.append(b);
            }
        });
    }

    public void testAddBytesRef() {
        testAgainstOracle(() -> new TestIteration() {
            BytesRef ref = new BytesRef(randomAlphaOfLengthBetween(1, 100));

            @Override
            public int size() {
                return ref.length;
            }

            @Override
            public void applyToBuilder(BreakingBytesRefBuilder builder) {
                builder.append(ref);
            }

            @Override
            public void applyToOracle(BytesRefBuilder oracle) {
                oracle.append(ref);
            }
        });
    }

    public void testGrow() {
        testAgainstOracle(() -> new TestIteration() {
            int length = between(1, 100);
            byte b = randomByte();

            @Override
            public int size() {
                return length;
            }

            @Override
            public void applyToBuilder(BreakingBytesRefBuilder builder) {
                builder.grow(builder.length() + length);
                builder.bytes()[builder.length()] = b;
                builder.setLength(builder.length() + length);
            }

            @Override
            public void applyToOracle(BytesRefBuilder oracle) {
                oracle.grow(oracle.length() + length);
                oracle.bytes()[oracle.length()] = b;
                oracle.setLength(oracle.length() + length);
            }
        });
    }

    interface TestIteration {
        int size();

        void applyToBuilder(BreakingBytesRefBuilder builder);

        void applyToOracle(BytesRefBuilder oracle);
    }

    private void testAgainstOracle(Supplier<TestIteration> iterations) {
        int limit = between(1_000, 10_000);
        String label = randomAlphaOfLength(4);
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(limit));
        BreakingBytesRefBuilder builder = new BreakingBytesRefBuilder(breaker, label);
        BytesRefBuilder oracle = new BytesRefBuilder();

        assertThat(builder.bytesRefView(), equalTo(oracle.get()));
        while (true) {
            TestIteration iteration = iterations.get();
            boolean willResize = builder.length() + iteration.size() >= builder.bytes().length;
            if (willResize) {
                int resizeMemoryUsage = builder.bytes().length + ArrayUtil.oversize(builder.bytes().length + iteration.size(), Byte.BYTES);
                if (resizeMemoryUsage > limit) {
                    Exception e = expectThrows(CircuitBreakingException.class, () -> iteration.applyToBuilder(builder));
                    assertThat(e.getMessage(), equalTo("over test limit"));
                    break;
                }
            }
            iteration.applyToBuilder(builder);
            iteration.applyToOracle(oracle);
            assertThat(builder.bytesRefView(), equalTo(oracle.get()));
        }
    }
}
