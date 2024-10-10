/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class BreakingBytesRefBuilderTests extends ESTestCase {
    public void testBreakOnBuild() {
        String label = randomAlphaOfLength(4);
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(0));
        Exception e = expectThrows(CircuitBreakingException.class, () -> new BreakingBytesRefBuilder(breaker, label));
        assertThat(e.getMessage(), equalTo("over test limit"));
    }

    public void testAddByte() {
        testAgainstOracle(() -> new TestIteration() {
            final byte b = randomByte();

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
            final BytesRef ref = new BytesRef(randomAlphaOfLengthBetween(1, 100));

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

    public void testCopyBytes() {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(300));
        try (BreakingBytesRefBuilder builder = new BreakingBytesRefBuilder(breaker, "test")) {
            String initialValue = randomAlphaOfLengthBetween(1, 50);
            builder.copyBytes(new BytesRef(initialValue));
            assertThat(builder.bytesRefView().utf8ToString(), equalTo(initialValue));

            String newValue = randomAlphaOfLengthBetween(350, 500);
            Exception e = expectThrows(CircuitBreakingException.class, () -> builder.copyBytes(new BytesRef(newValue)));
            assertThat(e.getMessage(), equalTo("over test limit"));
        }
    }

    public void testGrow() {
        testAgainstOracle(() -> new TestIteration() {
            final int length = between(1, 100);
            final byte b = randomByte();

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
        assertThat(breaker.getUsed(), equalTo(0L));
        try (BreakingBytesRefBuilder builder = new BreakingBytesRefBuilder(breaker, label)) {
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
            BytesRefBuilder oracle = new BytesRefBuilder();

            assertThat(builder.bytesRefView(), equalTo(oracle.get()));
            while (true) {
                TestIteration iteration = iterations.get();
                int targetSize = builder.length() + iteration.size();
                boolean willResize = targetSize >= builder.bytes().length;
                if (willResize) {
                    long resizeMemoryUsage = BreakingBytesRefBuilder.SHALLOW_SIZE + ramForArray(builder.bytes().length);
                    resizeMemoryUsage += ramForArray(ArrayUtil.oversize(targetSize, Byte.BYTES));
                    if (resizeMemoryUsage > limit) {
                        Exception e = expectThrows(CircuitBreakingException.class, () -> iteration.applyToBuilder(builder));
                        assertThat(e.getMessage(), equalTo("over test limit"));
                        break;
                    }
                }
                iteration.applyToBuilder(builder);
                iteration.applyToOracle(oracle);
                assertThat(builder.bytesRefView(), equalTo(oracle.get()));
                assertThat(
                    builder.ramBytesUsed(),
                    // Label and breaker aren't counted in ramBytesUsed because they are usually shared with other instances.
                    equalTo(RamUsageTester.ramUsed(builder) - RamUsageTester.ramUsed(label) - RamUsageTester.ramUsed(breaker))
                );
                assertThat(builder.ramBytesUsed(), equalTo(breaker.getUsed()));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private long ramForArray(int length) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + length);
    }
}
