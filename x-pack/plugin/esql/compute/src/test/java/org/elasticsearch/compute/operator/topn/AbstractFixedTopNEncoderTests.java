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

public abstract class AbstractFixedTopNEncoderTests extends AbstractSortableTopNEncoderTests {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCase<?>> tests = new ArrayList<>();
        tests.add(
            new TestCase<>(
                "ip",
                AbstractFixedTopNEncoderTests::randomIp,
                BytesRef::compareTo,
                TopNEncoder::encodeBytesRef,
                (encoder, encoded) -> encoder.decodeBytesRef(encoded, new BytesRef())
            )
        );
        return tests.stream().map(t -> new Object[] { t }).toList();
    }

    protected AbstractFixedTopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }

    public static BytesRef randomIp() {
        return new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(randomBoolean())));
    }

}
