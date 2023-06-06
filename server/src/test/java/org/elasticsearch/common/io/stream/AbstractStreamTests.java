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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
        final String message = Strings.format("unexpected byte [0x%02x]", corruptBytes[0]);
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
        final String message = Strings.format("unexpected byte [0x%02x]", corruptBytes[0]);
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

    public void testSmallBigIntArray() throws IOException {
        assertBigIntArray(between(0, PageCacheRecycler.INT_PAGE_SIZE));
    }

    public void testLargeBigIntArray() throws IOException {
        assertBigIntArray(between(PageCacheRecycler.INT_PAGE_SIZE, 5_000_000));
    }

    public void testBigIntArraySizeAligned() throws IOException {
        assertBigIntArray(PageCacheRecycler.INT_PAGE_SIZE * between(2, 1000));
    }

    private void assertBigIntArray(int size) throws IOException {
        IntArray testData = BigArrays.NON_RECYCLING_INSTANCE.newIntArray(size, false);
        for (int i = 0; i < size; i++) {
            testData.set(i, randomInt());
        }

        BytesStreamOutput out = new BytesStreamOutput();
        testData.writeTo(out);

        try (IntArray in = IntArray.readFrom(getStreamInput(out.bytes()))) {
            assertThat(in.size(), equalTo(testData.size()));
            for (int i = 0; i < size; i++) {
                assertThat(in.get(i), equalTo(testData.get(i)));
            }
        }
    }

    public void testSmallBigDoubleArray() throws IOException {
        assertBigDoubleArray(between(0, PageCacheRecycler.DOUBLE_PAGE_SIZE));
    }

    public void testLargeBigDoubleArray() throws IOException {
        assertBigDoubleArray(between(PageCacheRecycler.DOUBLE_PAGE_SIZE, 10000));
    }

    private void assertBigDoubleArray(int size) throws IOException {
        DoubleArray testData = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(size, false);
        for (int i = 0; i < size; i++) {
            testData.set(i, randomDouble());
        }

        BytesStreamOutput out = new BytesStreamOutput();
        testData.writeTo(out);

        try (DoubleArray in = DoubleArray.readFrom(getStreamInput(out.bytes()))) {
            assertThat(in.size(), equalTo(testData.size()));
            for (int i = 0; i < size; i++) {
                assertThat(in.get(i), equalTo(testData.get(i)));
            }
        }
    }

    public void testSmallBigLongArray() throws IOException {
        assertBigLongArray(between(0, PageCacheRecycler.LONG_PAGE_SIZE));
    }

    public void testLargeBigLongArray() throws IOException {
        assertBigLongArray(between(PageCacheRecycler.LONG_PAGE_SIZE, 10000));
    }

    private void assertBigLongArray(int size) throws IOException {
        LongArray testData = BigArrays.NON_RECYCLING_INSTANCE.newLongArray(size, false);
        for (int i = 0; i < size; i++) {
            testData.set(i, randomLong());
        }

        BytesStreamOutput out = new BytesStreamOutput();
        testData.writeTo(out);

        try (LongArray in = LongArray.readFrom(getStreamInput(out.bytes()))) {
            assertThat(in.size(), equalTo(testData.size()));
            for (int i = 0; i < size; i++) {
                assertThat(in.get(i), equalTo(testData.get(i)));
            }
        }
    }

    public void testSmallBigByteArray() throws IOException {
        assertBigByteArray(between(0, PageCacheRecycler.BYTE_PAGE_SIZE / 10));
    }

    public void testLargeBigByteArray() throws IOException {
        assertBigByteArray(between(PageCacheRecycler.BYTE_PAGE_SIZE / 10, PageCacheRecycler.BYTE_PAGE_SIZE * 10));
    }

    private void assertBigByteArray(int size) throws IOException {
        ByteArray testData = BigArrays.NON_RECYCLING_INSTANCE.newByteArray(size, false);
        for (int i = 0; i < size; i++) {
            testData.set(i, randomByte());
        }

        BytesStreamOutput out = new BytesStreamOutput();
        testData.writeTo(out);

        try (ByteArray in = ByteArray.readFrom(getStreamInput(out.bytes()))) {
            assertThat(in.size(), equalTo(testData.size()));
            for (int i = 0; i < size; i++) {
                assertThat(in.get(i), equalTo(testData.get(i)));
            }
        }
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

        runWriteReadCollectionTest(
            () -> new FooBar(randomInt(), randomInt()),
            StreamOutput::writeOptionalCollection,
            in -> in.readOptionalList(FooBar::new)
        );

        runWriteReadOptionalCollectionWithNullInput(out -> out.writeOptionalCollection(null), in -> in.readOptionalList(FooBar::new));
    }

    public void testStringCollection() throws IOException {
        runWriteReadCollectionTest(() -> randomUnicodeOfLength(16), StreamOutput::writeStringCollection, StreamInput::readStringList);
    }

    public void testOptionalStringCollection() throws IOException {
        runWriteReadCollectionTest(
            () -> randomUnicodeOfLength(16),
            StreamOutput::writeOptionalStringCollection,
            StreamInput::readOptionalStringList
        );
    }

    public void testOptionalStringCollectionWithNullInput() throws IOException {
        runWriteReadOptionalCollectionWithNullInput(out -> out.writeOptionalStringCollection(null), StreamInput::readOptionalStringList);
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

    private <T> void runWriteReadOptionalCollectionWithNullInput(
        final CheckedConsumer<StreamOutput, IOException> nullWriter,
        final CheckedFunction<StreamInput, Collection<T>, IOException> reader
    ) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nullWriter.accept(out);
            try (StreamInput in = getStreamInput(out.bytes())) {
                assertNull(reader.apply(in));
            }
        }
    }

    public void testSetOfLongs() throws IOException {
        final int size = randomIntBetween(0, 6);
        final Set<Long> sourceSet = Sets.newHashSetWithExpectedSize(size);
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

    public void testDurationSerialization() throws IOException {
        Stream.generate(AbstractStreamTests::randomDuration).limit(100).forEach(this::assertDurationSerialization);
    }

    void assertDurationSerialization(Duration duration) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericValue(duration);
            try (StreamInput in = getStreamInput(out.bytes())) {
                final Duration deserialized = (Duration) in.readGenericValue();
                assertEquals(duration, deserialized);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void testPeriodSerialization() {
        Stream.generate(AbstractStreamTests::randomPeriod).limit(100).forEach(this::assertPeriodSerialization);
    }

    void assertPeriodSerialization(Period period) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericValue(period);
            try (StreamInput in = getStreamInput(out.bytes())) {
                final Period deserialized = (Period) in.readGenericValue();
                assertEquals(period, deserialized);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Duration randomDuration() {
        return randomFrom(
            List.of(
                Duration.ofNanos(randomIntBetween(1, 100_000)),
                Duration.ofMillis(randomIntBetween(1, 1_000)),
                Duration.ofSeconds(randomIntBetween(1, 100)),
                Duration.ofHours(randomIntBetween(1, 10)),
                Duration.ofDays(randomIntBetween(1, 5))
            )
        );
    }

    static Period randomPeriod() {
        return randomFrom(
            List.of(
                Period.ofDays(randomIntBetween(1, 31)),
                Period.ofWeeks(randomIntBetween(1, 52)),
                Period.ofMonths(randomIntBetween(1, 12)),
                Period.ofYears(randomIntBetween(1, 1000))
            )
        );
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

    public void assertImmutableMapSerialization(Map<String, Integer> expected) throws IOException {
        final BytesStreamOutput output = new BytesStreamOutput();
        output.writeMap(expected, StreamOutput::writeString, StreamOutput::writeVInt);
        final BytesReference bytesReference = output.bytes();

        final StreamInput input = getStreamInput(bytesReference);
        Map<String, Integer> got = input.readImmutableMap(StreamInput::readString, StreamInput::readVInt);
        assertThat(got, equalTo(expected));

        expectThrows(UnsupportedOperationException.class, () -> got.put("blah", 1));
    }

    public void testImmutableMapSerialization() throws IOException {
        assertImmutableMapSerialization(Map.of());
        assertImmutableMapSerialization(Map.of("a", 1));
        assertImmutableMapSerialization(Map.of("a", 1, "b", 2));
    }

    public <T> void assertImmutableListSerialization(List<T> expected, Writeable.Reader<T> reader, Writeable.Writer<T> writer)
        throws IOException {
        final BytesStreamOutput output = new BytesStreamOutput();
        output.writeCollection(expected, writer);
        final BytesReference bytesReference = output.bytes();

        final StreamInput input = getStreamInput(bytesReference);
        List<T> got = input.readImmutableList(reader);
        assertThat(got, equalTo(expected));

        expectThrows(UnsupportedOperationException.class, got::clear);
    }

    public void testImmutableListSerialization() throws IOException {
        assertImmutableListSerialization(List.of(), StreamInput::readString, StreamOutput::writeString);
        assertImmutableListSerialization(List.of("a"), StreamInput::readString, StreamOutput::writeString);
        assertImmutableListSerialization(List.of("a", "b"), StreamInput::readString, StreamOutput::writeString);
        assertImmutableListSerialization(List.of(1), StreamInput::readVInt, StreamOutput::writeVInt);
        assertImmutableListSerialization(List.of(1, 2, 3), StreamInput::readVInt, StreamOutput::writeVInt);
    }

    public void testReadAfterReachingEndOfStream() throws IOException {
        try (var output = new BytesStreamOutput()) {
            int len = randomIntBetween(1, 16);
            for (int i = 0; i < len; i++) {
                output.writeByte(randomByte());
            }
            StreamInput input = getStreamInput(output.bytes());
            input.readBytes(new byte[len], 0, len);

            assertEquals(-1, input.read());
            expectThrows(IOException.class, input::readByte);
        }
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
