/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.snapshots.blobstore;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;

public class SlicedInputStreamTests extends ESTestCase {
    public void testReadRandom() throws IOException {
        int parts = randomIntBetween(1, 20);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        int numWriteOps = scaledRandomIntBetween(1000, 10000);
        final long seed = randomLong();
        Random random = new Random(seed);
        for (int i = 0; i < numWriteOps; i++) {
            switch (random.nextInt(5)) {
                case 1 -> stream.write(random.nextInt(Byte.MAX_VALUE));
                default -> stream.write(randomBytes(random));
            }
        }

        final CheckClosedInputStream[] streams = new CheckClosedInputStream[parts];
        byte[] bytes = stream.toByteArray();
        int slice = bytes.length / parts;
        int offset = 0;
        int length;
        for (int i = 0; i < parts; i++) {
            length = i == parts - 1 ? bytes.length - offset : slice;
            streams[i] = new CheckClosedInputStream(new ByteArrayInputStream(bytes, offset, length));
            offset += length;
        }

        SlicedInputStream input = new SlicedInputStream(parts) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                return streams[slice];
            }
        };
        random = new Random(seed);
        assertThat(input.available(), equalTo(streams[0].available()));
        for (int i = 0; i < numWriteOps; i++) {
            switch (random.nextInt(5)) {
                case 1 -> assertThat(random.nextInt(Byte.MAX_VALUE), equalTo(input.read()));
                default -> {
                    byte[] b = randomBytes(random);
                    byte[] buffer = new byte[b.length];
                    int read = readFully(input, buffer);
                    assertThat(b.length, equalTo(read));
                    assertArrayEquals(b, buffer);
                }
            }
        }

        assertThat(input.available(), equalTo(0));
        for (int i = 0; i < streams.length - 1; i++) {
            assertTrue(streams[i].closed);
        }
        input.close();

        for (int i = 0; i < streams.length; i++) {
            assertTrue(streams[i].closed);
        }

    }

    private int readFully(InputStream stream, byte[] buffer) throws IOException {
        for (int i = 0; i < buffer.length;) {
            int read = stream.read(buffer, i, buffer.length - i);
            if (read == -1) {
                if (i == 0) {
                    return -1;
                } else {
                    return i;
                }
            }
            i += read;
        }
        return buffer.length;
    }

    private byte[] randomBytes(Random random) {
        int length = RandomNumbers.randomIntBetween(random, 1, 10);
        byte[] data = new byte[length];
        random.nextBytes(data);
        return data;
    }

    private static final class CheckClosedInputStream extends FilterInputStream {

        public boolean closed = false;

        CheckClosedInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
