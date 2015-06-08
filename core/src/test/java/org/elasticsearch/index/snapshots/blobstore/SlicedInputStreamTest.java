/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.snapshots.blobstore;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.*;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;

public class SlicedInputStreamTest extends ElasticsearchTestCase {

    @Test
    public void readRandom() throws IOException {
        int parts = randomIntBetween(1, 20);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        int numWriteOps = scaledRandomIntBetween(1000, 10000);
        final long seed = randomLong();
        Random random = new Random(seed);
        for (int i = 0; i < numWriteOps; i++) {
            switch(random.nextInt(5)) {
                case 1:
                    stream.write(random.nextInt(Byte.MAX_VALUE));
                    break;
                default:
                    stream.write(randomBytes(random));
                    break;
            }
        }

        final CheckClosedInputStream[] streams = new CheckClosedInputStream[parts];
        byte[] bytes = stream.toByteArray();
        int slice = bytes.length / parts;
        int offset = 0;
        int length;
        for (int i = 0; i < parts; i++) {
            length = i == parts-1 ? bytes.length-offset : slice;
            streams[i] = new CheckClosedInputStream(new ByteArrayInputStream(bytes, offset, length));
            offset += length;
        }

        SlicedInputStream input = new SlicedInputStream(parts) {

            @Override
            protected InputStream openSlice(long slice) throws IOException {
                return streams[(int)slice];
            }
        };
        random = new Random(seed);
        assertThat(input.available(), equalTo(streams[0].available()));
        for (int i = 0; i < numWriteOps; i++) {
            switch(random.nextInt(5)) {
                case 1:
                    assertThat(random.nextInt(Byte.MAX_VALUE), equalTo(input.read()));
                    break;
                default:
                    byte[] b = randomBytes(random);
                    byte[] buffer = new byte[b.length];
                    int read = readFully(input, buffer);
                    assertThat(b.length, equalTo(read));
                    assertArrayEquals(b, buffer);
                    break;
            }
        }

        assertThat(input.available(), equalTo(0));
        for (int i =0; i < streams.length-1; i++) {
            assertTrue(streams[i].closed);
        }
        input.close();

        for (int i =0; i < streams.length; i++) {
            assertTrue(streams[i].closed);
        }

    }

    private int readFully(InputStream stream, byte[] buffer) throws IOException {
        for (int i = 0; i < buffer.length;) {
            int read = stream.read(buffer, i, buffer.length-i);
            if (read == -1) {
              if (i == 0) {
                  return -1;
              } else {
                  return i;
              }
            }
            i+= read;
        }
        return buffer.length;
    }

    private byte[] randomBytes(Random random) {
        int length = RandomInts.randomIntBetween(random, 1, 10);
        byte[] data = new byte[length];
        random.nextBytes(data);
        return data;
    }

    private static final class CheckClosedInputStream extends FilterInputStream {

        public boolean closed = false;

        public CheckClosedInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
