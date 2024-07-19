/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class CountingFilterInputStreamTests extends ESTestCase {

    public void testBytesCounting() throws IOException {
        final byte[] input = randomByteArrayOfLength(between(500, 1000));
        final var in = new CountingFilterInputStream(new ByteArrayInputStream(input));

        assertThat(in.getBytesRead(), equalTo(0));

        final CheckedConsumer<Integer, IOException> readRandomly = (Integer length) -> {
            switch (between(0, 3)) {
                case 0 -> {
                    for (var i = 0; i < length; i++) {
                        final int bytesBefore = in.getBytesRead();
                        final int result = in.read();
                        assertThat((byte) result, equalTo(input[bytesBefore]));
                        assertThat(in.getBytesRead(), equalTo(bytesBefore + 1));
                    }
                }
                case 1 -> {
                    final int bytesBefore = in.getBytesRead();
                    final byte[] b;
                    if (randomBoolean()) {
                        b = in.readNBytes(length);
                    } else {
                        b = new byte[length];
                        assertThat(in.read(b), equalTo(length));
                    }
                    assertArrayEquals(Arrays.copyOfRange(input, bytesBefore, bytesBefore + length), b);
                    assertThat(in.getBytesRead(), equalTo(bytesBefore + length));
                }
                case 2 -> {
                    final int bytesBefore = in.getBytesRead();
                    final byte[] b = new byte[length * between(2, 5)];
                    if (randomBoolean()) {
                        assertThat(in.read(b, length / 2, length), equalTo(length));
                    } else {
                        assertThat(in.readNBytes(b, length / 2, length), equalTo(length));
                    }
                    assertArrayEquals(
                        Arrays.copyOfRange(input, bytesBefore, bytesBefore + length),
                        Arrays.copyOfRange(b, length / 2, length / 2 + length)
                    );
                    assertThat(in.getBytesRead(), equalTo(bytesBefore + length));
                }
                case 3 -> {
                    final int bytesBefore = in.getBytesRead();
                    if (randomBoolean()) {
                        assertThat((int) in.skip(length), equalTo(length));
                    } else {
                        in.skipNBytes(length);
                    }
                    assertThat(in.getBytesRead(), equalTo(bytesBefore + length));
                }
                default -> fail("unexpected");
            }
        };

        while (in.getBytesRead() < input.length - 50) {
            readRandomly.accept(between(1, 30));
        }

        final int bytesBefore = in.getBytesRead();
        final byte[] remainingBytes = in.readAllBytes();
        assertThat(in.getBytesRead(), equalTo(bytesBefore + remainingBytes.length));
        assertThat(in.getBytesRead(), equalTo(input.length));

        // Read beyond available data has no effect
        assertThat(in.read(), equalTo(-1));
        final byte[] bytes = new byte[between(20, 30)];
        assertThat(in.read(bytes), equalTo(-1));
        IntStream.range(0, bytes.length).forEach(i -> assertThat(bytes[i], equalTo((byte) 0)));
        assertThat(in.read(bytes, between(3, 5), between(5, 10)), equalTo(-1));
        IntStream.range(0, bytes.length).forEach(i -> assertThat(bytes[i], equalTo((byte) 0)));
        assertThat(in.skip(between(10, 20)), equalTo(0L));

        assertThat(in.getBytesRead(), equalTo(input.length));
    }
}
