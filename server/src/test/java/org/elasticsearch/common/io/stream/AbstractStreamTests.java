/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractStreamTests extends ESTestCase {

    protected abstract StreamInput getStreamInput(BytesReference bytesReference) throws IOException;

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

        final StreamInput input = getStreamInput(bytesReference);
        assertFalse(input.readBoolean());
        assertTrue(input.readBoolean());

        final Set<Byte> set = IntStream.range(Byte.MIN_VALUE, Byte.MAX_VALUE).mapToObj(v -> (byte) v).collect(Collectors.toSet());
        set.remove((byte) 0);
        set.remove((byte) 1);
        final byte[] corruptBytes = new byte[] { randomFrom(set) };
        final BytesReference corrupt = new BytesArray(corruptBytes);
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> getStreamInput(corrupt).readBoolean());
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

        final StreamInput input = getStreamInput(bytesReference);
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
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> getStreamInput(corrupt).readOptionalBoolean());
        final String message = String.format(Locale.ROOT, "unexpected byte [0x%02x]", corruptBytes[0]);
        assertThat(e, hasToString(containsString(message)));
    }

    public void testRandomVLongSerialization() throws IOException {
        for (int i = 0; i < 1024; i++) {
            long write = randomLong();
            BytesStreamOutput out = new BytesStreamOutput();
            out.writeZLong(write);
            long read = getStreamInput(out.bytes()).readZLong();
            assertEquals(write, read);
        }
    }

    public void testSpecificVLongSerialization() throws IOException {
        List<Tuple<Long, byte[]>> values = Arrays.asList(
            new Tuple<>(0L, new byte[] { 0 }),
            new Tuple<>(-1L, new byte[] { 1 }),
            new Tuple<>(1L, new byte[] { 2 }),
            new Tuple<>(-2L, new byte[] { 3 }),
            new Tuple<>(2L, new byte[] { 4 }),
            new Tuple<>(Long.MIN_VALUE, new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, -1, 1 }),
            new Tuple<>(Long.MAX_VALUE, new byte[] { -2, -1, -1, -1, -1, -1, -1, -1, -1, 1 })

        );
        for (Tuple<Long, byte[]> value : values) {
            BytesStreamOutput out = new BytesStreamOutput();
            out.writeZLong(value.v1());
            assertArrayEquals(Long.toString(value.v1()), value.v2(), BytesReference.toBytes(out.bytes()));
            BytesReference bytes = new BytesArray(value.v2());
            assertEquals(Arrays.toString(value.v2()), (long) value.v1(), getStreamInput(bytes).readZLong());
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
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, Integer> read = (LinkedHashMap<String, Integer>) getStreamInput(out.bytes()).readGenericValue();
        assertEquals(size, read.size());
        int index = 0;
        for (Map.Entry<String, Integer> entry : read.entrySet()) {
            assertEquals(list.get(index).v1(), entry.getKey());
            assertEquals(list.get(index).v2(), entry.getValue());
            index++;
        }
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
            targetArray = getStreamInput(out.bytes()).readOptionalArray(WriteableString::new, WriteableString[]::new);
        } else {
            out.writeArray(sourceArray);
            targetArray = getStreamInput(out.bytes()).readArray(WriteableString::new, WriteableString[]::new);
        }

        assertThat(targetArray, equalTo(sourceArray));
    }

    public void testArrays() throws IOException {
        final String[] strings;
        final String[] deserialized;
        Writeable.Writer<String> writer = StreamOutput::writeString;
        Writeable.Reader<String> reader = StreamInput::readString;
        BytesStreamOutput out = new BytesStreamOutput();
        if (randomBoolean()) {
            if (randomBoolean()) {
                strings = null;
            } else {
                strings = generateRandomStringArray(10, 10, false, true);
            }
            out.writeOptionalArray(writer, strings);
            deserialized = getStreamInput(out.bytes()).readOptionalArray(reader, String[]::new);
        } else {
            strings = generateRandomStringArray(10, 10, false, true);
            out.writeArray(writer, strings);
            deserialized = getStreamInput(out.bytes()).readArray(reader, String[]::new);
        }
        assertThat(deserialized, equalTo(strings));
    }

    public void testCollection() throws IOException {
        class FooBar implements Writeable {

            private final int foo;
            private final int bar;

            private FooBar(final int foo, final int bar) {
                this.foo = foo;
                this.bar = bar;
            }

            private FooBar(final StreamInput in) throws IOException {
                this.foo = in.readInt();
                this.bar = in.readInt();
            }

            @Override
            public void writeTo(final StreamOutput out) throws IOException {
                out.writeInt(foo);
                out.writeInt(bar);
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                final FooBar that = (FooBar) o;
                return foo == that.foo && bar == that.bar;
            }

            @Override
            public int hashCode() {
                return Objects.hash(foo, bar);
            }

        }

        runWriteReadCollectionTest(
            () -> new FooBar(randomInt(), randomInt()),
            StreamOutput::writeCollection,
            in -> in.readList(FooBar::new)
        );
    }

    public void testStringCollection() throws IOException {
        runWriteReadCollectionTest(() -> randomUnicodeOfLength(16), StreamOutput::writeStringCollection, StreamInput::readStringList);
    }

    private <T> void runWriteReadCollectionTest(
        final Supplier<T> supplier,
        final CheckedBiConsumer<StreamOutput, Collection<T>, IOException> writer,
        final CheckedFunction<StreamInput, Collection<T>, IOException> reader
    ) throws IOException {
        final int length = randomIntBetween(0, 10);
        final Collection<T> collection = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            collection.add(supplier.get());
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writer.accept(out, collection);
            try (StreamInput in = getStreamInput(out.bytes())) {
                assertThat(collection, equalTo(reader.apply(in)));
            }
        }
    }

    public void testSetOfLongs() throws IOException {
        final int size = randomIntBetween(0, 6);
        final Set<Long> sourceSet = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            sourceSet.add(randomLongBetween(i * 1000, (i + 1) * 1000 - 1));
        }
        assertThat(sourceSet, iterableWithSize(size));

        final BytesStreamOutput out = new BytesStreamOutput();
        out.writeCollection(sourceSet, StreamOutput::writeLong);

        final Set<Long> targetSet = getStreamInput(out.bytes()).readSet(StreamInput::readLong);
        assertThat(targetSet, equalTo(sourceSet));
    }

    public void testInstantSerialization() throws IOException {
        final Instant instant = Instant.now();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeInstant(instant);
            try (StreamInput in = getStreamInput(out.bytes())) {
                final Instant serialized = in.readInstant();
                assertEquals(instant, serialized);
            }
        }
    }

    public void testOptionalInstantSerialization() throws IOException {
        final Instant instant = Instant.now();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeOptionalInstant(instant);
            try (StreamInput in = getStreamInput(out.bytes())) {
                final Instant serialized = in.readOptionalInstant();
                assertEquals(instant, serialized);
            }
        }

        final Instant missing = null;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeOptionalInstant(missing);
            try (StreamInput in = getStreamInput(out.bytes())) {
                final Instant serialized = in.readOptionalInstant();
                assertEquals(missing, serialized);
            }
        }
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

    public void testSecureStringSerialization() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            final SecureString secureString = new SecureString("super secret".toCharArray());
            output.writeSecureString(secureString);

            final BytesReference bytesReference = output.bytes();
            final StreamInput input = getStreamInput(bytesReference);

            assertThat(secureString, is(equalTo(input.readSecureString())));
        }

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            final SecureString secureString = randomBoolean() ? null : new SecureString("super secret".toCharArray());
            output.writeOptionalSecureString(secureString);

            final BytesReference bytesReference = output.bytes();
            final StreamInput input = getStreamInput(bytesReference);

            if (secureString != null) {
                assertThat(input.readOptionalSecureString(), is(equalTo(secureString)));
            } else {
                assertThat(input.readOptionalSecureString(), is(nullValue()));
            }
        }
    }

    public void testGenericSet() throws IOException {
        Set<String> set = Set.of("a", "b", "c", "d", "e");
        assertGenericRoundtrip(set);
        // reverse order in normal set so linked hashset does not match the order
        var list = new ArrayList<>(set);
        Collections.reverse(list);
        assertGenericRoundtrip(new LinkedHashSet<>(list));
    }

    public void testReadArraySize() throws IOException {
        BytesStreamOutput stream = new BytesStreamOutput();
        byte[] array = new byte[randomIntBetween(1, 10)];
        for (int i = 0; i < array.length; i++) {
            array[i] = randomByte();
        }
        stream.writeByteArray(array);
        StreamInput streamInput = new InputStreamStreamInput(getStreamInput(stream.bytes()), array.length - 1);
        expectThrows(EOFException.class, streamInput::readByteArray);
        streamInput = new InputStreamStreamInput(getStreamInput(stream.bytes()), BytesReference.toBytes(stream.bytes()).length);

        assertArrayEquals(array, streamInput.readByteArray());
    }

    public void testFilterStreamInputDelegatesAvailable() throws IOException {
        final int length = randomIntBetween(1, 1024);
        StreamInput delegate = getStreamInput(BytesReference.fromByteBuffer(ByteBuffer.wrap(new byte[length])));

        FilterStreamInput filterInputStream = new FilterStreamInput(delegate) {
        };
        assertEquals(filterInputStream.available(), length);

        // read some bytes
        final int bytesToRead = randomIntBetween(1, length);
        filterInputStream.readBytes(new byte[bytesToRead], 0, bytesToRead);
        assertEquals(filterInputStream.available(), length - bytesToRead);
    }

    private static class Unwriteable {}

    private void assertNotWriteable(Object o, Class<?> type) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StreamOutput.checkWriteable(o));
        assertThat(e.getMessage(), equalTo("Cannot write type [" + type.getCanonicalName() + "] to stream"));
    }

    public void testIsWriteable() throws IOException {
        assertNotWriteable(new Unwriteable(), Unwriteable.class);
    }

    public void testSetIsWriteable() throws IOException {
        StreamOutput.checkWriteable(Set.of("a", "b"));
        assertNotWriteable(Set.of(new Unwriteable()), Unwriteable.class);
    }

    public void testListIsWriteable() throws IOException {
        StreamOutput.checkWriteable(List.of("a", "b"));
        assertNotWriteable(List.of(new Unwriteable()), Unwriteable.class);
    }

    public void testMapIsWriteable() throws IOException {
        StreamOutput.checkWriteable(Map.of("a", "b", "c", "d"));
        assertNotWriteable(Map.of("a", new Unwriteable()), Unwriteable.class);
    }

    public void testObjectArrayIsWriteable() throws IOException {
        StreamOutput.checkWriteable(new Object[] { "a", "b" });
        assertNotWriteable(new Object[] { new Unwriteable() }, Unwriteable.class);
    }

    private void assertSerialization(
        CheckedConsumer<StreamOutput, IOException> outputAssertions,
        CheckedConsumer<StreamInput, IOException> inputAssertions
    ) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            outputAssertions.accept(output);
            final StreamInput input = getStreamInput(output.bytes());
            inputAssertions.accept(input);
        }
    }

    private void assertGenericRoundtrip(Object original) throws IOException {
        assertSerialization(output -> { output.writeGenericValue(original); }, input -> {
            Object read = input.readGenericValue();
            assertThat(read, equalTo(original));
        });
    }
}
