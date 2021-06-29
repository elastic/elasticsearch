/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class RecyclingBytesStreamOutputTests extends ESTestCase {

    public void testReturnsWrittenBytesAndRecyclesBufferIfPossible() throws IOException {

        final byte[] source = randomUnicodeOfLength(scaledRandomIntBetween(0, 20000)).getBytes(StandardCharsets.UTF_8);
        final byte[] buffer = new byte[scaledRandomIntBetween(0, 20000)];

        final MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        try (RecyclingBytesStreamOutput output = new RecyclingBytesStreamOutput(buffer, bigArrays)) {
            int position = 0;
            while (position < source.length) {
                if (randomBoolean()) {
                    output.writeByte(source[position++]);
                } else {
                    final int length = randomIntBetween(1, source.length - position);
                    final int sliceStart = randomIntBetween(0, position);
                    final int sliceEnd = randomIntBetween(position + length, source.length);
                    final byte[] slice = new byte[sliceEnd - sliceStart];
                    System.arraycopy(source, sliceStart, slice, 0, slice.length);
                    output.writeBytes(slice, position - sliceStart, length);
                    position += length;
                }
            }

            final BytesRef bytesRef;

            if (randomBoolean()) {
                bytesRef = output.toBytesRef();
                assertThat(bytesRef.offset, equalTo(0));

                if (source.length <= buffer.length) {
                    assertThat("should have re-used the same buffer", bytesRef.bytes, sameInstance(buffer));
                } else {
                    assertThat("new buffer should be the right size", bytesRef.bytes.length, equalTo(source.length));
                }
            } else {
                bytesRef = output.bytes().toBytesRef();
            }

            assertThat(bytesRef.length, equalTo(source.length));
            final byte[] trimmed = new byte[source.length];
            System.arraycopy(bytesRef.bytes, bytesRef.offset, trimmed, 0, bytesRef.length);
            assertArrayEquals(source, trimmed);
        }
    }
}
