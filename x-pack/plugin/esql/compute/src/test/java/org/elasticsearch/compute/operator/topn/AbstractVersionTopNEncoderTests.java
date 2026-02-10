/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Repeat(iterations = 1000)
public abstract class AbstractVersionTopNEncoderTests extends AbstractSortableTopNEncoderTests {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCase<?>> tests = new ArrayList<>();
        tests.add(
            new TestCase<>(
                "version",
                () -> TopNOperatorTests.randomVersion().toBytesRef(),
                BytesRef::compareTo,
                TopNEncoder::encodeBytesRef,
                (encoder, encoded) -> encoder.decodeBytesRef(encoded, new BytesRef())
            )
        );
        return tests.stream().map(t -> new Object[] { t }).toList();
    }

    protected AbstractVersionTopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }
}
