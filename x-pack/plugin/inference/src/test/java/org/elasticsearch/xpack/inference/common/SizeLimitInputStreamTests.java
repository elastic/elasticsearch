/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.is;

public class SizeLimitInputStreamTests extends ESTestCase {
    public void testRead_WithoutThrowingException() throws IOException {
        int size = randomIntBetween(1, 100);

        try (var stream = createRandomLimitedStream(size, size)) {
            for (int i = 0; i < size; i++) {
                stream.read(new byte[size]);
            }
        }
    }

    public void testRead_OneByteAtATime_WithoutThrowingException() throws IOException {
        int size = randomIntBetween(1, 100);

        try (var stream = createRandomLimitedStream(size, size)) {
            for (int i = 0; i < size; i++) {
                stream.read();
            }
        }
    }

    public void testRead_ThrowsException_WhenLimitReached() {
        int maxAllowed = randomIntBetween(1, 100);
        int dataSize = maxAllowed + 1;

        IOException e = expectThrows(IOException.class, () -> {
            try (var stream = createRandomLimitedStream(maxAllowed + 1, maxAllowed)) {
                stream.read(new byte[dataSize]);
            }
        });

        assertThat(e.getMessage(), is(format("Maximum limit of [%s] bytes reached", maxAllowed)));
    }

    public void testRead_OneByteAtATime_ThrowsException_WhenLimitReached() {
        int maxAllowed = randomIntBetween(1, 100);
        int dataSize = maxAllowed + 1;

        IOException e = expectThrows(IOException.class, () -> {
            try (var stream = createRandomLimitedStream(maxAllowed + 1, maxAllowed)) {
                for (int i = 0; i < dataSize; i++) {
                    stream.read();
                }
            }
        });

        assertThat(e.getMessage(), is(format("Maximum limit of [%s] bytes reached", maxAllowed)));
    }

    public void testMarkAndReset_ThrowsUnsupportedException() throws IOException {
        int size = randomIntBetween(1, 100);

        try (var stream = createRandomLimitedStream(size, size)) {
            assertThat(stream.markSupported(), is(false));

            UnsupportedOperationException unsupportedOperationException = expectThrows(
                UnsupportedOperationException.class,
                () -> stream.mark(10)
            );
            assertThat(unsupportedOperationException.getMessage(), is("mark not supported"));

            IOException e = expectThrows(IOException.class, stream::reset);
            assertThat(e.getMessage(), is("reset not supported"));
        }
    }

    private static SizeLimitInputStream createRandomLimitedStream(int dataSize, int maxAllowedSize) {
        String data = randomAlphaOfLength(dataSize);
        ByteSizeValue byteSizeValue = ByteSizeValue.of(maxAllowedSize, ByteSizeUnit.BYTES);
        return new SizeLimitInputStream(byteSizeValue, new ByteArrayInputStream(data.getBytes(UTF_8)));
    }
}
