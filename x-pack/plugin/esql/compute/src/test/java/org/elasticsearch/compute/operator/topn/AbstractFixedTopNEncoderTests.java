/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class AbstractFixedTopNEncoderTests extends AbstractSortableTopNEncoderTests {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCase<?>> tests = new ArrayList<>();
        tests.add(testCase("ip", AbstractFixedTopNEncoderTests::randomIp));
        tests.add(testCase("ip inside garbage", () -> embedInRandomBytes(randomIp())));
        return tests.stream().map(t -> new Object[] { t }).toList();
    }

    protected AbstractFixedTopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }

    public static BytesRef randomIp() {
        return new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(randomBoolean())));
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
}
