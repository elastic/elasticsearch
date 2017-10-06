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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class StreamTests extends ESTestCase {

    public void testBooleanSerialization() throws IOException {
        final BytesStreamOutput output = new BytesStreamOutput();
        output.writeBoolean(false);
        output.writeBoolean(true);

        final BytesReference bytesReference = output.bytes();

        final BytesRef bytesRef = bytesReference.toBytesRef();
        assertThat(bytesRef.length, equalTo(2));
        final byte[] bytes = bytesRef.bytes;
        assertThat(bytes[0], equalTo((byte) 0));
        assertThat(bytes[1], equalTo((byte) 1));

        final StreamInput input = bytesReference.streamInput();
        assertFalse(input.readBoolean());
        assertTrue(input.readBoolean());

        final Set<Byte> set = IntStream.range(Byte.MIN_VALUE, Byte.MAX_VALUE).mapToObj(v -> (byte) v).collect(Collectors.toSet());
        set.remove((byte) 0);
        set.remove((byte) 1);
        final byte[] corruptBytes = new byte[] { randomFrom(set) };
        final BytesReference corrupt = new BytesArray(corruptBytes);
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> corrupt.streamInput().readBoolean());
        final String message = String.format(Locale.ROOT, "unexpected byte [0x%02x]", corruptBytes[0]);
        assertThat(e, hasToString(containsString(message)));
    }

    public void testOptionalBooleanSerialization() throws IOException {
        final BytesStreamOutput output = new BytesStreamOutput();
        output.writeOptionalBoolean(false);
        output.writeOptionalBoolean(true);
        output.writeOptionalBoolean(null);

        final BytesReference bytesReference = output.bytes();

        final BytesRef bytesRef = bytesReference.toBytesRef();
        assertThat(bytesRef.length, equalTo(3));
        final byte[] bytes = bytesRef.bytes;
        assertThat(bytes[0], equalTo((byte) 0));
        assertThat(bytes[1], equalTo((byte) 1));
        assertThat(bytes[2], equalTo((byte) 2));

        final StreamInput input = bytesReference.streamInput();
        final Boolean maybeFalse = input.readOptionalBoolean();
        assertNotNull(maybeFalse);
        assertFalse(maybeFalse);
        final Boolean maybeTrue = input.readOptionalBoolean();
        assertNotNull(maybeTrue);
        assertTrue(maybeTrue);
        assertNull(input.readOptionalBoolean());

        final Set<Byte> set = IntStream.range(Byte.MIN_VALUE, Byte.MAX_VALUE).mapToObj(v -> (byte) v).collect(Collectors.toSet());
        set.remove((byte) 0);
        set.remove((byte) 1);
        set.remove((byte) 2);
        final byte[] corruptBytes = new byte[] { randomFrom(set) };
        final BytesReference corrupt = new BytesArray(corruptBytes);
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> corrupt.streamInput().readOptionalBoolean());
        final String message = String.format(Locale.ROOT, "unexpected byte [0x%02x]", corruptBytes[0]);
        assertThat(e, hasToString(containsString(message)));
    }

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
            assertArrayEquals(Long.toString(value.v1()), value.v2(), BytesReference.toBytes(out.bytes()));
            BytesReference bytes = new BytesArray(value.v2());
            assertEquals(Arrays.toString(value.v2()), (long)value.v1(), bytes.streamInput().readZLong());
        }
    }

    public void testLinkedHashMap() throws IOException {
        int size = randomIntBetween(1, 1024);
        boolean accessOrder = randomBoolean();
        List<Tuple<String, Integer>> list = new ArrayList<>(size);
        LinkedHashMap<String, Integer> write = new LinkedHashMap<>(size, 0.75f, accessOrder);
        for (int i = 0; i < size; i++) {
            int value = randomInt();
            list.add(new Tuple<>(Integer.toString(i), value));
            write.put(Integer.toString(i), value);
        }
        if (accessOrder) {
            // randomize access order
            Collections.shuffle(list, random());
            for (Tuple<String, Integer> entry : list) {
                // touch the entries to set the access order
                write.get(entry.v1());
            }
        }
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeGenericValue(write);
        LinkedHashMap<String, Integer> read = (LinkedHashMap<String, Integer>)out.bytes().streamInput().readGenericValue();
        assertEquals(size, read.size());
        int index = 0;
        for (Map.Entry<String, Integer> entry : read.entrySet()) {
            assertEquals(list.get(index).v1(), entry.getKey());
            assertEquals(list.get(index).v2(), entry.getValue());
            index++;
        }
    }

    public void testFilterStreamInputDelegatesAvailable() throws IOException {
        final int length = randomIntBetween(1, 1024);
        StreamInput delegate = StreamInput.wrap(new byte[length]);

        FilterStreamInput filterInputStream = new FilterStreamInput(delegate) {};
        assertEquals(filterInputStream.available(), length);

        // read some bytes
        final int bytesToRead = randomIntBetween(1, length);
        filterInputStream.readBytes(new byte[bytesToRead], 0, bytesToRead);
        assertEquals(filterInputStream.available(), length - bytesToRead);
    }

    public void testInputStreamStreamInputDelegatesAvailable() throws IOException {
        final int length = randomIntBetween(1, 1024);
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[length]);
        InputStreamStreamInput streamInput = new InputStreamStreamInput(is);
        assertEquals(streamInput.available(), length);

        // read some bytes
        final int bytesToRead = randomIntBetween(1, length);
        streamInput.readBytes(new byte[bytesToRead], 0, bytesToRead);
        assertEquals(streamInput.available(), length - bytesToRead);
    }

    public void testReadArraySize() throws IOException {
        BytesStreamOutput stream = new BytesStreamOutput();
        byte[] array = new byte[randomIntBetween(1, 10)];
        for (int i = 0; i < array.length; i++) {
            array[i] = randomByte();
        }
        stream.writeByteArray(array);
        InputStreamStreamInput streamInput = new InputStreamStreamInput(StreamInput.wrap(BytesReference.toBytes(stream.bytes())), array
            .length-1);
        expectThrows(EOFException.class, streamInput::readByteArray);
        streamInput = new InputStreamStreamInput(StreamInput.wrap(BytesReference.toBytes(stream.bytes())), BytesReference.toBytes(stream
            .bytes()).length);

        assertArrayEquals(array, streamInput.readByteArray());
    }

    public void testWritableArrays() throws IOException {

        final String[] strings = generateRandomStringArray(10, 10, false, true);
        WriteableString[] sourceArray = Arrays.stream(strings).<WriteableString>map(WriteableString::new).toArray(WriteableString[]::new);
        WriteableString[] targetArray;
        BytesStreamOutput out = new BytesStreamOutput();

        if (randomBoolean()) {
            if (randomBoolean()) {
                sourceArray = null;
            }
            out.writeOptionalArray(sourceArray);
            targetArray = out.bytes().streamInput().readOptionalArray(WriteableString::new, WriteableString[]::new);
        } else {
            out.writeArray(sourceArray);
            targetArray = out.bytes().streamInput().readArray(WriteableString::new, WriteableString[]::new);
        }

        assertThat(targetArray, equalTo(sourceArray));
    }

    static final class WriteableString implements Writeable {
        final String string;

        WriteableString(String string) {
            this.string = string;
        }

        WriteableString(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            WriteableString that = (WriteableString) o;

            return string.equals(that.string);

        }

        @Override
        public int hashCode() {
            return string.hashCode();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(string);
        }
    }

}
