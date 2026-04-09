/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractVersionTopNEncoderTests extends AbstractSortableTopNEncoderTests {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCase<?>> tests = new ArrayList<>();
        tests.add(testCase("version", () -> TopNOperatorTests.randomVersion().toBytesRef()));
        tests.add(testCase("version inside garbage", () -> embedInRandomBytes(TopNOperatorTests.randomVersion().toBytesRef())));
        return tests.stream().map(t -> new Object[] { t }).toList();
    }

    protected AbstractVersionTopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }

    private static TestCase<BytesRef> testCase(String name, Supplier<BytesRef> randomValue) {
        return new TestCase<>(
            name,
            randomValue,
            BytesRef::compareTo,
            TopNEncoder::encodeBytesRef,
            (encoder, encoded) -> encoder.decodeBytesRef(encoded, new BytesRef())
        );
    }

    public void testContainingNul() {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        BytesRef v = (BytesRef) testCase.randomValue().get();
        insertNul(v);
        try (BreakingBytesRefBuilder bytes = new BreakingBytesRefBuilder(breaker, "bytes")) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> encoder().encodeBytesRef(v, bytes));
            assertThat(e.getMessage(), equalTo("Can't sort versions containing nul"));
        }
    }

    private void insertNul(BytesRef v) {
        v.bytes[between(v.offset, v.offset + v.length - 1)] = 0;
    }
}
