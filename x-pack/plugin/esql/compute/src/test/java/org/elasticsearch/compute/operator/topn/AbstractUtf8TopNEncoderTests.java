/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class AbstractUtf8TopNEncoderTests extends AbstractSortableTopNEncoderTests {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCase<?>> tests = new ArrayList<>();
        tests.addAll(testCases("alpha", () -> randomAlphaOfLengthBetween(0, 100)));
        tests.addAll(testCases("unicode", () -> randomRealisticUnicodeOfCodepointLengthBetween(0, 100)));
        tests.addAll(testCases("unrealistic unicode", () -> randomUnicodeOfLengthBetween(0, 100)));
        return tests.stream().map(t -> new Object[] { t }).toList();
    }

    private static List<TestCase<BytesRef>> testCases(String name, Supplier<String> randomValue) {
        return List.of(
            testCase(name, () -> new BytesRef(randomValue.get())),
            testCase(name + " inside garbage", () -> embedInRandomBytes(new BytesRef(randomValue.get())))
        );
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

    protected AbstractUtf8TopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }
}
