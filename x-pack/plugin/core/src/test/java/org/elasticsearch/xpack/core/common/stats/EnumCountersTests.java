/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.stats;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class EnumCountersTests extends AbstractWireTestCase<EnumCounters<EnumCountersTests.TestV2>> {

    enum TestV1 {A, B, C}

    enum TestV2 {A, B, C, D}

    @Override
    protected EnumCounters<TestV2> createTestInstance() {
        EnumCounters<TestV2> inst = new EnumCounters<>(TestV2.class);
        inst.inc(TestV2.A, randomNonNegativeLong());
        inst.inc(TestV2.B, randomNonNegativeLong());
        inst.inc(TestV2.C, randomNonNegativeLong());
        inst.inc(TestV2.D, randomNonNegativeLong());
        return inst;
    }

    @Override
    protected EnumCounters<TestV2> copyInstance(EnumCounters<TestV2> instance, Version version) throws IOException {
        return serialize(instance, in -> new EnumCounters<>(in, TestV2.class));
    }

    public void testIncrements() {
        EnumCounters<TestV1> counters = new EnumCounters<>(TestV1.class);
        int a = randomIntBetween(0, 100);
        int b = randomIntBetween(0, 100);
        int c = randomIntBetween(0, 100);
        incrementRandomly(counters, TestV1.A, a);
        incrementRandomly(counters, TestV1.B, b);
        incrementRandomly(counters, TestV1.C, c);
        assertEquals(a, counters.get(TestV1.A));
        assertEquals(b, counters.get(TestV1.B));
        assertEquals(c, counters.get(TestV1.C));
        Map<String, Object> map = counters.toMap();
        assertThat(map.keySet(), hasSize(3));
        assertThat(map.get("a"), equalTo((long) a));
        assertThat(map.get("b"), equalTo((long) b));
        assertThat(map.get("c"), equalTo((long) c));
    }

    public void testBackwardCompatibility() throws Exception {
        EnumCounters<TestV2> counters = new EnumCounters<>(TestV2.class);
        counters.inc(TestV2.A, 1);
        counters.inc(TestV2.B, 2);
        counters.inc(TestV2.C, 3);
        counters.inc(TestV2.D, 4);
        EnumCounters<TestV1> oldCounters = serialize(counters, in -> new EnumCounters<>(in, TestV1.class));
        assertEquals(counters.get(TestV2.A), oldCounters.get(TestV1.A));
        assertEquals(counters.get(TestV2.B), oldCounters.get(TestV1.B));
        assertEquals(counters.get(TestV2.C), oldCounters.get(TestV1.C));
    }


    public void testForwardCompatibility() throws Exception {
        EnumCounters<TestV1> counters = new EnumCounters<>(TestV1.class);
        counters.inc(TestV1.A, 1);
        counters.inc(TestV1.B, 2);
        counters.inc(TestV1.C, 3);
        EnumCounters<TestV2> newCounters = serialize(counters, in -> new EnumCounters<>(in, TestV2.class));
        assertEquals(counters.get(TestV1.A), newCounters.get(TestV2.A));
        assertEquals(counters.get(TestV1.B), newCounters.get(TestV2.B));
        assertEquals(counters.get(TestV1.C), newCounters.get(TestV2.C));
        assertEquals(0, newCounters.get(TestV2.D));
    }

    private <E1 extends Enum<E1>, E2 extends Enum<E2>> EnumCounters<E2> serialize(
        EnumCounters<E1> source, Writeable.Reader<EnumCounters<E2>> targetReader) throws IOException {

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            source.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                return targetReader.read(in);
            }
        }
    }

    private <E extends Enum<E>> void incrementRandomly(EnumCounters<E> counters, E e, int inc) {
        int single = randomIntBetween(0, inc);
        if (randomBoolean()) {
            for (int i = 0; i < single; i++) {
                counters.inc(e);
            }
            counters.inc(e, inc - single);
        } else {
            counters.inc(e, inc - single);
            for (int i = 0; i < single; i++) {
                counters.inc(e);
            }
        }
    }

}
