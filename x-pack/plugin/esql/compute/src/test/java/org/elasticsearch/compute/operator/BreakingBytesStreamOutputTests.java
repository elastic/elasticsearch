/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class BreakingBytesStreamOutputTests extends ESTestCase {
    public void testBreakOnBuild() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(0));
        Exception e = expectThrows(CircuitBreakingException.class, () -> new BreakingBytesStreamOutput(breaker, "test"));
        assertThat(e.getMessage(), equalTo("over test limit"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * The delegate builder fits in the breaker, but the stream's own shallow size doesn't.
     * The already reserved builder must be released.
     */
    public void testBreakOnBuildAfterDelegate() {
        long delegateSize;
        try (BreakingBytesRefBuilder delegate = new BreakingBytesRefBuilder(new NoopCircuitBreaker("test"), "test")) {
            delegateSize = delegate.ramBytesUsed();
        }
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(delegateSize));
        Exception e = expectThrows(CircuitBreakingException.class, () -> new BreakingBytesStreamOutput(breaker, "test"));
        assertThat(e.getMessage(), equalTo("over test limit"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testWritesMatchBytesStreamOutput() throws IOException {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(1));
        try (
            BreakingBytesStreamOutput out = new BreakingBytesStreamOutput(breaker, "test");
            BytesStreamOutput oracle = new BytesStreamOutput()
        ) {
            int iterations = between(1, 100);
            for (int i = 0; i < iterations; i++) {
                writeRandom(out, oracle);
                assertThat(out.position(), equalTo(oracle.position()));
                assertThat(out.bytesRefView(), equalTo(oracle.bytes().toBytesRef()));
                assertThat(breaker.getUsed(), equalTo(out.ramBytesUsed()));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testClear() throws IOException {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(1));
        try (BreakingBytesStreamOutput out = new BreakingBytesStreamOutput(breaker, "test")) {
            out.writeBytesRef(new BytesRef(randomAlphaOfLengthBetween(1, 100)));
            long usedBeforeClear = breaker.getUsed();
            out.clear();
            assertThat(out.position(), equalTo(0L));
            assertThat(out.bytesRefView().length, equalTo(0));
            assertThat(breaker.getUsed(), equalTo(usedBeforeClear));

            try (BytesStreamOutput oracle = new BytesStreamOutput()) {
                writeRandom(out, oracle);
                assertThat(out.bytesRefView(), equalTo(oracle.bytes().toBytesRef()));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreakWhileWriting() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(between(200, 1000)));
        try (BreakingBytesStreamOutput out = new BreakingBytesStreamOutput(breaker, "test")) {
            Exception e = expectThrows(CircuitBreakingException.class, () -> {
                while (true) {
                    out.writeBytesRef(new BytesRef(randomAlphaOfLengthBetween(1, 100)));
                }
            });
            assertThat(e.getMessage(), equalTo("over test limit"));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private void writeRandom(StreamOutput out, StreamOutput oracle) throws IOException {
        switch (between(0, 5)) {
            case 0 -> {
                byte b = randomByte();
                out.writeByte(b);
                oracle.writeByte(b);
            }
            case 1 -> {
                int v = randomInt();
                out.writeVInt(v);
                oracle.writeVInt(v);
            }
            case 2 -> {
                long v = randomLong();
                out.writeLong(v);
                oracle.writeLong(v);
            }
            case 3 -> {
                BytesRef v = new BytesRef(randomAlphaOfLengthBetween(0, 100));
                out.writeBytesRef(v);
                oracle.writeBytesRef(v);
            }
            case 4 -> {
                String v = randomUnicodeOfLengthBetween(0, 100);
                out.writeString(v);
                oracle.writeString(v);
            }
            case 5 -> {
                String v = randomBoolean() ? null : randomUnicodeOfLengthBetween(0, 100);
                out.writeOptionalString(v);
                oracle.writeOptionalString(v);
            }
            default -> throw new AssertionError();
        }
    }
}
