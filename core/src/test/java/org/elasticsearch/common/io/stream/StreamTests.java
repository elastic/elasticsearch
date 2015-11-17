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

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.ByteBufferBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class StreamTests extends ESTestCase {
    public void testRandomVLongSerialization() throws IOException {
        for (int i = 0; i < 1024; i++) {
            long write = randomLong();
            BytesStreamOutput out = new BytesStreamOutput();
            out.writeZLong(write);
            long read = out.bytes().streamInput().readZLong();
            assertEquals(write, read);
        }
    }

    public void testSpecificVLongSerialization() throws IOException {
        List<Tuple<Long, byte[]>> values =
                Arrays.asList(
                        new Tuple<>(0L, new byte[]{0}),
                        new Tuple<>(-1L, new byte[]{1}),
                        new Tuple<>(1L, new byte[]{2}),
                        new Tuple<>(-2L, new byte[]{3}),
                        new Tuple<>(2L, new byte[]{4}),
                        new Tuple<>(Long.MIN_VALUE, new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, 1}),
                        new Tuple<>(Long.MAX_VALUE, new byte[]{-2, -1, -1, -1, -1, -1, -1, -1, -1, 1})

                );
        for (Tuple<Long, byte[]> value : values) {
            BytesStreamOutput out = new BytesStreamOutput();
            out.writeZLong(value.v1());
            assertArrayEquals(Long.toString(value.v1()), value.v2(), out.bytes().toBytes());
            ByteBufferBytesReference bytes = new ByteBufferBytesReference(ByteBuffer.wrap(value.v2()));
            assertEquals(Arrays.toString(value.v2()), (long)value.v1(), bytes.streamInput().readZLong());
        }
    }
}
