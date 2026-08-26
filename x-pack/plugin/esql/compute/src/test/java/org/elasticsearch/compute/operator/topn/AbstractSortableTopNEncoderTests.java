/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Comparator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractSortableTopNEncoderTests extends ESTestCase {
    protected record TestCase<T>(
        String name,
        Supplier<T> randomValue,
        Comparator<T> comparator,
        TriConsumer<TopNEncoder, T, BreakingBytesRefBuilder> encode,
        BiFunction<TopNEncoder, BytesRef, T> decode
    ) {
        public void testCompare(TopNEncoder encoder, BiConsumer<BreakingBytesRefBuilder, BreakingBytesRefBuilder> assertMinMax) {
            CircuitBreaker breaker = new NoopCircuitBreaker("test");
            T min = randomValue.get();
            T max = randomValueOtherThan(min, randomValue);
            if (comparator.compare(min, max) > 0) {
                T tmp = min;
                min = max;
                max = tmp;
            }
            BreakingBytesRefBuilder minBytes = new BreakingBytesRefBuilder(breaker, "min");
            BreakingBytesRefBuilder maxBytes = new BreakingBytesRefBuilder(breaker, "max");

            encode.apply(encoder, min, minBytes);
            encode.apply(encoder, max, maxBytes);
            assertMinMax.accept(minBytes, maxBytes);
        }

        public void testEncodeDecode(TopNEncoder encoder) {
            CircuitBreaker breaker = new NoopCircuitBreaker("test");
            T v = randomValue.get();
            BreakingBytesRefBuilder bytes = new BreakingBytesRefBuilder(breaker, "bytes");
            encode().apply(encoder, v, bytes);
            BytesRef bytesRef = bytes.bytesRefView();
            assertThat(decode.apply(encoder, bytesRef), equalTo(v));
            assertThat(bytesRef.length, equalTo(0));
        }

        @Override
        public String toString() {
            return name;
        }
    }

    protected final TestCase<?> testCase;

    protected AbstractSortableTopNEncoderTests(TestCase<?> testCase) {
        this.testCase = testCase;
    }

    protected abstract TopNEncoder encoder();

    protected abstract void assertMinMax(BreakingBytesRefBuilder min, BreakingBytesRefBuilder max);

    public final void testCompare() {
        testCase.testCompare(encoder(), this::assertMinMax);
    }

    public final void testEncodeDecode() {
        testCase.testEncodeDecode(encoder());
    }
}
