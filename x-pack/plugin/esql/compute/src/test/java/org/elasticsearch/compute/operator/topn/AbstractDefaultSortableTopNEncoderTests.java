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
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

@Repeat(iterations = 1000)
public abstract class AbstractDefaultSortableTopNEncoderTests extends AbstractSortableTopNEncoderTests {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCase<?>> tests = new ArrayList<>();
        tests.add(new TestCase<>("long", ESTestCase::randomLong, Long::compare, TopNEncoder::encodeLong, TopNEncoder::decodeLong));
        tests.add(
            new TestCase<>("double", ESTestCase::randomDouble, Double::compare, TopNEncoder::encodeDouble, TopNEncoder::decodeDouble)
        );
        tests.add(new TestCase<>("int", ESTestCase::randomInt, Integer::compare, TopNEncoder::encodeInt, TopNEncoder::decodeInt));
        tests.add(new TestCase<>("float", ESTestCase::randomFloat, Float::compare, TopNEncoder::encodeFloat, TopNEncoder::decodeFloat));
        tests.add(
            new TestCase<>("bool", ESTestCase::randomBoolean, Boolean::compare, TopNEncoder::encodeBoolean, TopNEncoder::decodeBoolean)
        );
        return tests.stream().map(t -> new Object[] { t }).toList();
    }

    protected AbstractDefaultSortableTopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }
}
