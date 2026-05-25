/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SizeLimitInputStreamTests extends ESTestCase {

    public void testReadsUpToLimitWithoutThrowing() throws IOException {
        int size = randomIntBetween(1, 100);
        try (SizeLimitInputStream stream = streamOf(size, size)) {
            byte[] sink = new byte[size];
            assertThat(stream.readAllBytes().length, is(size));
            assertThat(stream.read(sink), is(-1));
        }
    }

    public void testReadsOneByteAtATimeUpToLimitWithoutThrowing() throws IOException {
        int size = randomIntBetween(1, 100);
        try (SizeLimitInputStream stream = streamOf(size, size)) {
            for (int i = 0; i < size; i++) {
                assertNotEquals(-1, stream.read());
            }
            assertThat(stream.read(), is(-1));
        }
    }

    public void testThrowsWhenLimitExceededOnBulkRead() {
        int maxAllowed = randomIntBetween(1, 100);
        int dataSize = maxAllowed + 1;
        IOException e = expectThrows(IOException.class, () -> {
            try (SizeLimitInputStream stream = streamOf(dataSize, maxAllowed)) {
                stream.readAllBytes();
            }
        });
        assertThat(e.getMessage(), equalTo("Maximum limit of [" + maxAllowed + "] bytes reached"));
    }

    public void testThrowsWhenLimitExceededOnSingleByteRead() {
        int maxAllowed = randomIntBetween(1, 100);
        int dataSize = maxAllowed + 1;
        IOException e = expectThrows(IOException.class, () -> {
            try (SizeLimitInputStream stream = streamOf(dataSize, maxAllowed)) {
                for (int i = 0; i < dataSize; i++) {
                    stream.read();
                }
            }
        });
        assertThat(e.getMessage(), equalTo("Maximum limit of [" + maxAllowed + "] bytes reached"));
    }

    public void testZeroLimitRejectsAnyData() {
        IOException e = expectThrows(IOException.class, () -> {
            try (SizeLimitInputStream stream = streamOf(1, 0)) {
                stream.read();
            }
        });
        assertThat(e.getMessage(), equalTo("Maximum limit of [0] bytes reached"));
    }

    public void testEmptyUnderlyingStreamReturnsEofImmediately() throws IOException {
        try (SizeLimitInputStream stream = streamOf(0, randomIntBetween(0, 100))) {
            assertThat(stream.read(), is(-1));
            assertThat(stream.readAllBytes().length, is(0));
        }
    }

    public void testMarkResetUnsupported() throws IOException {
        try (SizeLimitInputStream stream = streamOf(4, 4)) {
            assertThat(stream.markSupported(), is(false));
            UnsupportedOperationException markEx = expectThrows(UnsupportedOperationException.class, () -> stream.mark(8));
            assertThat(markEx.getMessage(), equalTo("mark not supported"));
            IOException resetEx = expectThrows(IOException.class, stream::reset);
            assertThat(resetEx.getMessage(), equalTo("reset not supported"));
        }
    }

    public void testNegativeLimitIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SizeLimitInputStream(-1, new ByteArrayInputStream(new byte[0]))
        );
        assertThat(e.getMessage(), equalTo("maxByteSize must be non-negative, got [-1]"));
    }

    private static SizeLimitInputStream streamOf(int dataSize, long maxAllowed) {
        byte[] data = randomAlphaOfLength(dataSize).getBytes(UTF_8);
        return new SizeLimitInputStream(maxAllowed, new ByteArrayInputStream(data));
    }
}
