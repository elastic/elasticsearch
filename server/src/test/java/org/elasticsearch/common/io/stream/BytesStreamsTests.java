/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link StreamOutput}.
 */
public class BytesStreamsTests extends ESTestCase {
    public void testEmpty() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        // test empty stream to array
        assertEquals(0, out.size());
        assertEquals(0, out.bytes().length());

        out.close();
    }

    public void testSingleByte() throws Exception {
        TestStreamOutput out = new TestStreamOutput();
        assertEquals(0, out.size());

        int expectedSize = 1;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write single byte
        out.writeByte(expectedData[0]);
        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testSingleShortPage() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        int expectedSize = 10;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write byte-by-byte
        for (int i = 0; i < expectedSize; i++) {
            out.writeByte(expectedData[i]);
        }

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testIllegalBulkWrite() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        // bulk-write with wrong args
        expectThrows(IndexOutOfBoundsException.class, () -> out.writeBytes(new byte[] {}, 0, 1));
        out.close();
    }

    public void testSingleShortPageBulkWrite() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        // first bulk-write empty array: should not change anything
        int expectedSize = 0;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);
        out.writeBytes(expectedData);
        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        // bulk-write again with actual bytes
        expectedSize = 10;
        expectedData = randomizedByteArrayWithSize(expectedSize);
        out.writeBytes(expectedData);
        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testSingleFullPageBulkWrite() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        int expectedSize = PageCacheRecycler.BYTE_PAGE_SIZE;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write in bulk
        out.writeBytes(expectedData);

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testSingleFullPageBulkWriteWithOffset() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        int initialOffset = 10;
        int additionalLength = PageCacheRecycler.BYTE_PAGE_SIZE;
        byte[] expectedData = randomizedByteArrayWithSize(initialOffset + additionalLength);

        // first create initial offset
        out.writeBytes(expectedData, 0, initialOffset);
        assertEquals(initialOffset, out.size());

        // now write the rest - more than fits into the remaining first page
        out.writeBytes(expectedData, initialOffset, additionalLength);
        assertEquals(expectedData.length, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testSingleFullPageBulkWriteWithOffsetCrossover() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        int initialOffset = 10;
        int additionalLength = PageCacheRecycler.BYTE_PAGE_SIZE * 2;
        byte[] expectedData = randomizedByteArrayWithSize(initialOffset + additionalLength);
        out.writeBytes(expectedData, 0, initialOffset);
        assertEquals(initialOffset, out.size());

        // now write the rest - more than fits into the remaining page + a full page after
        // that,
        // ie. we cross over into a third
        out.writeBytes(expectedData, initialOffset, additionalLength);
        assertEquals(expectedData.length, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testSingleFullPage() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        int expectedSize = PageCacheRecycler.BYTE_PAGE_SIZE;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write byte-by-byte
        for (int i = 0; i < expectedSize; i++) {
            out.writeByte(expectedData[i]);
        }

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testOneFullOneShortPage() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        int expectedSize = PageCacheRecycler.BYTE_PAGE_SIZE + 10;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write byte-by-byte
        for (int i = 0; i < expectedSize; i++) {
            out.writeByte(expectedData[i]);
        }

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testTwoFullOneShortPage() throws Exception {
        TestStreamOutput out = new TestStreamOutput();

        int expectedSize = (PageCacheRecycler.BYTE_PAGE_SIZE * 2) + 1;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write byte-by-byte
        for (int i = 0; i < expectedSize; i++) {
            out.writeByte(expectedData[i]);
        }

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testSeek() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int position = 0;
        assertEquals(position, out.position());

        out.seek(position += 10);
        out.seek(position += PageCacheRecycler.BYTE_PAGE_SIZE);
        out.seek(position += PageCacheRecycler.BYTE_PAGE_SIZE + 10);
        out.seek(position += PageCacheRecycler.BYTE_PAGE_SIZE * 2);
        assertEquals(position, out.position());
        assertEquals(position, BytesReference.toBytes(out.bytes()).length);

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> out.seek(Integer.MAX_VALUE + 1L));
        assertEquals("BytesStreamOutput cannot hold more than 2GB of data", iae.getMessage());

        out.close();
    }

    public void testSkip() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int position = 0;
        assertEquals(position, out.position());

        int forward = 100;
        out.skip(forward);
        assertEquals(position + forward, out.position());

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> out.skip(Integer.MAX_VALUE - 50));
        assertEquals("BytesStreamOutput cannot hold more than 2GB of data", iae.getMessage());

        out.close();
    }

    public void testSimpleStreams() throws Exception {
        assumeTrue("requires a 64-bit JRE ... ?!", Constants.JRE_IS_64BIT);
        TestStreamOutput out = new TestStreamOutput();
        out.writeBoolean(false);
        out.writeByte((byte) 1);
        out.writeShort((short) -1);
        out.writeInt(-1);
        out.writeVInt(2);
        out.writeLong(-3);
        out.writeVLong(4);
        out.writeOptionalLong(11234234L);
        out.writeOptionalVLong(5L);
        out.writeOptionalVLong(null);
        out.writeFloat(1.1f);
        out.writeDouble(2.2);
        int[] intArray = { 1, 2, 3 };
        out.writeGenericValue(intArray);
        int[] vIntArray = { 4, 5, 6 };
        out.writeVIntArray(vIntArray);
        long[] longArray = { 1, 2, 3 };
        out.writeGenericValue(longArray);
        long[] vLongArray = { 4, 5, 6 };
        out.writeVLongArray(vLongArray);
        float[] floatArray = { 1.1f, 2.2f, 3.3f };
        out.writeGenericValue(floatArray);
        double[] doubleArray = { 1.1, 2.2, 3.3 };
        out.writeGenericValue(doubleArray);
        out.writeString("hello");
        out.writeString("goodbye");
        out.writeGenericValue(BytesRefs.toBytesRef("bytesref"));
        out.writeStringArray(new String[] { "a", "b", "cat" });
        out.writeBytesReference(new BytesArray("test"));
        out.writeOptionalBytesReference(new BytesArray("test"));
        out.writeOptionalDouble(null);
        out.writeOptionalDouble(1.2);
        out.writeZoneId(ZoneId.of("CET"));
        out.writeOptionalZoneId(ZoneId.systemDefault());
        out.writeGenericValue(ZonedDateTime.ofInstant(Instant.ofEpochMilli(123456), ZoneId.of("America/Los_Angeles")));
        final OffsetTime offsetNow = OffsetTime.now(randomZone());
        out.writeGenericValue(offsetNow);
        final byte[] bytes = BytesReference.toBytes(out.bytes());
        StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
        assertEquals(in.available(), bytes.length);
        assertThat(in.readBoolean(), equalTo(false));
        assertThat(in.readByte(), equalTo((byte) 1));
        assertThat(in.readShort(), equalTo((short) -1));
        assertThat(in.readInt(), equalTo(-1));
        assertThat(in.readVInt(), equalTo(2));
        assertThat(in.readLong(), equalTo(-3L));
        assertThat(in.readVLong(), equalTo(4L));
        assertThat(in.readOptionalLong(), equalTo(11234234L));
        assertThat(in.readOptionalVLong(), equalTo(5L));
        assertThat(in.readOptionalVLong(), nullValue());
        assertThat((double) in.readFloat(), closeTo(1.1, 0.0001));
        assertThat(in.readDouble(), closeTo(2.2, 0.0001));
        assertThat(in.readGenericValue(), equalTo((Object) intArray));
        assertThat(in.readVIntArray(), equalTo(vIntArray));
        assertThat(in.readGenericValue(), equalTo((Object) longArray));
        assertThat(in.readVLongArray(), equalTo(vLongArray));
        assertThat(in.readGenericValue(), equalTo((Object) floatArray));
        assertThat(in.readGenericValue(), equalTo((Object) doubleArray));
        assertThat(in.readString(), equalTo("hello"));
        assertThat(in.readString(), equalTo("goodbye"));
        assertThat(in.readGenericValue(), equalTo((Object) BytesRefs.toBytesRef("bytesref")));
        assertThat(in.readStringArray(), equalTo(new String[] { "a", "b", "cat" }));
        assertThat(in.readBytesReference(), equalTo(new BytesArray("test")));
        assertThat(in.readOptionalBytesReference(), equalTo(new BytesArray("test")));
        assertNull(in.readOptionalDouble());
        assertThat(in.readOptionalDouble(), closeTo(1.2, 0.0001));
        assertEquals(ZoneId.of("CET"), in.readZoneId());
        assertEquals(ZoneId.systemDefault(), in.readOptionalZoneId());
        Object dt = in.readGenericValue();
        assertThat(dt, instanceOf(ZonedDateTime.class));
        ZonedDateTime zdt = (ZonedDateTime) dt;
        assertThat(zdt.toInstant().toEpochMilli(), equalTo(123456L));
        assertThat(zdt.getZone(), equalTo(ZoneId.of("America/Los_Angeles")));
        assertThat(in.readGenericValue(), equalTo(offsetNow));
        assertEquals(0, in.available());
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> out.writeGenericValue(new Object() {
            @Override
            public String toString() {
                return "This object cannot be serialized by writeGeneric method";
            }
        }));
        assertThat(ex.getMessage(), containsString("can not write type"));
        in.close();
        out.close();
    }

    public void testNamedWriteable() throws IOException {
        try (TestStreamOutput out = new TestStreamOutput()) {
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
                Collections.singletonList(
                    new NamedWriteableRegistry.Entry(BaseNamedWriteable.class, TestNamedWriteable.NAME, TestNamedWriteable::new)
                )
            );
            TestNamedWriteable namedWriteableIn = new TestNamedWriteable(
                randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10)
            );
            out.writeNamedWriteable(namedWriteableIn);
            byte[] bytes = BytesReference.toBytes(out.bytes());

            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry)) {
                assertEquals(in.available(), bytes.length);
                BaseNamedWriteable namedWriteableOut = in.readNamedWriteable(BaseNamedWriteable.class);
                assertEquals(namedWriteableIn, namedWriteableOut);
                assertEquals(0, in.available());
            }
        }
    }

    public void testNamedWriteableList() throws IOException {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(BaseNamedWriteable.class, TestNamedWriteable.NAME, TestNamedWriteable::new)
            )
        );
        int size = between(0, 100);
        List<BaseNamedWriteable> expected = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            expected.add(new TestNamedWriteable(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
        }

        try (TestStreamOutput out = new TestStreamOutput()) {
            out.writeNamedWriteableCollection(expected);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)) {
                assertEquals(expected, in.readNamedWriteableCollectionAsList(BaseNamedWriteable.class));
                assertEquals(0, in.available());
            }
        }
    }

    public void testNamedWriteableNotSupportedWithoutWrapping() throws IOException {
        try (TestStreamOutput out = new TestStreamOutput()) {
            TestNamedWriteable testNamedWriteable = new TestNamedWriteable("test1", "test2");
            out.writeNamedWriteable(testNamedWriteable);
            StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
            Exception e = expectThrows(UnsupportedOperationException.class, () -> in.readNamedWriteable(BaseNamedWriteable.class));
            assertThat(e.getMessage(), is("can't read named writeable from StreamInput"));
        }
    }

    public void testNamedWriteableReaderReturnsNull() throws IOException {
        try (TestStreamOutput out = new TestStreamOutput()) {
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
                Collections.singletonList(
                    new NamedWriteableRegistry.Entry(BaseNamedWriteable.class, TestNamedWriteable.NAME, (StreamInput in) -> null)
                )
            );
            TestNamedWriteable namedWriteableIn = new TestNamedWriteable(
                randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10)
            );
            out.writeNamedWriteable(namedWriteableIn);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry)) {
                assertEquals(in.available(), bytes.length);
                AssertionError e = expectThrows(AssertionError.class, () -> in.readNamedWriteable(BaseNamedWriteable.class));
                assertThat(e.getCause().getMessage(), endsWith("] returned null which is not allowed."));
            }
        }
    }

    public void testOptionalWriteableReaderReturnsNull() throws IOException {
        try (TestStreamOutput out = new TestStreamOutput()) {
            out.writeOptionalWriteable(new TestNamedWriteable(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
            StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
            AssertionError e = expectThrows(AssertionError.class, () -> in.readOptionalWriteable((StreamInput ignored) -> null));
            assertThat(e.getCause().getMessage(), endsWith("] returned null which is not allowed."));
        }
    }

    public void testWriteableReaderReturnsWrongName() throws IOException {
        try (TestStreamOutput out = new TestStreamOutput()) {
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
                Collections.singletonList(
                    new NamedWriteableRegistry.Entry(
                        BaseNamedWriteable.class,
                        TestNamedWriteable.NAME,
                        (StreamInput in) -> new TestNamedWriteable(in) {
                            @Override
                            public String getWriteableName() {
                                return "intentionally-broken";
                            }
                        }
                    )
                )
            );
            TestNamedWriteable namedWriteableIn = new TestNamedWriteable(
                randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10)
            );
            out.writeNamedWriteable(namedWriteableIn);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry)) {
                assertEquals(in.available(), bytes.length);
                AssertionError e = expectThrows(AssertionError.class, () -> in.readNamedWriteable(BaseNamedWriteable.class));
                assertThat(
                    e.getMessage(),
                    endsWith(" claims to have a different name [intentionally-broken] than it was read from [test-named-writeable].")
                );
            }
        }
    }

    public void testWriteWriteableList() throws IOException {
        final int size = randomIntBetween(0, 5);
        final List<TestWriteable> expected = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            expected.add(new TestWriteable(randomBoolean()));
        }

        final BytesStreamOutput out = new BytesStreamOutput();
        out.writeCollection(expected);

        final StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));

        final List<TestWriteable> loaded = in.readCollectionAsList(TestWriteable::new);

        assertThat(loaded, hasSize(expected.size()));

        for (int i = 0; i < expected.size(); ++i) {
            assertEquals(expected.get(i).value, loaded.get(i).value);
        }

        assertEquals(0, in.available());

        in.close();
        out.close();
    }

    public void testWriteMap() throws IOException {
        final int size = randomIntBetween(0, 100);
        final Map<String, String> expected = Maps.newMapWithExpectedSize(size);
        for (int i = 0; i < size; ++i) {
            expected.put(randomAlphaOfLength(2), randomAlphaOfLength(5));
        }

        final TestStreamOutput out = new TestStreamOutput();
        out.writeMap(expected, StreamOutput::writeString, StreamOutput::writeString);
        final StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
        final Map<String, String> loaded = in.readMap(StreamInput::readString, StreamInput::readString);

        assertThat(loaded.size(), equalTo(expected.size()));
        assertThat(expected, equalTo(loaded));
    }

    public void testWriteImmutableMap() throws IOException {
        final int size = randomIntBetween(0, 100);
        final ImmutableOpenMap.Builder<String, String> expectedBuilder = ImmutableOpenMap.builder(randomIntBetween(0, 100));
        for (int i = 0; i < size; ++i) {
            expectedBuilder.put(randomAlphaOfLength(2), randomAlphaOfLength(5));
        }

        final ImmutableOpenMap<String, String> expected = expectedBuilder.build();
        final TestStreamOutput out = new TestStreamOutput();
        out.writeMap(expected, StreamOutput::writeString, StreamOutput::writeString);
        final StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
        final ImmutableOpenMap<String, String> loaded = in.readImmutableOpenMap(StreamInput::readString, StreamInput::readString);

        assertThat(expected, equalTo(loaded));
    }

    public void testWriteImmutableMapOfWritable() throws IOException {
        final int size = randomIntBetween(0, 100);
        final ImmutableOpenMap.Builder<TestWriteable, TestWriteable> expectedBuilder = ImmutableOpenMap.builder(randomIntBetween(0, 100));
        for (int i = 0; i < size; ++i) {
            expectedBuilder.put(new TestWriteable(randomBoolean()), new TestWriteable(randomBoolean()));
        }

        final ImmutableOpenMap<TestWriteable, TestWriteable> expected = expectedBuilder.build();
        final TestStreamOutput out = new TestStreamOutput();
        out.writeMap(expected);
        final StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
        final ImmutableOpenMap<TestWriteable, TestWriteable> loaded = in.readImmutableOpenMap(TestWriteable::new, TestWriteable::new);

        assertThat(expected, equalTo(loaded));
    }

    public void testWriteMapAsList() throws IOException {
        final int size = randomIntBetween(0, 100);
        final Map<String, String> expected = Maps.newMapWithExpectedSize(size);
        for (int i = 0; i < size; ++i) {
            final String value = randomAlphaOfLength(5);
            expected.put("key_" + value, value);
        }

        final TestStreamOutput out = new TestStreamOutput();
        out.writeMapValues(expected, StreamOutput::writeString);
        final StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
        final Map<String, String> loaded = in.readMapValues(StreamInput::readString, value -> "key_" + value);

        assertThat(loaded.size(), equalTo(expected.size()));
        assertThat(expected, equalTo(loaded));
    }

    private abstract static class BaseNamedWriteable implements NamedWriteable {

    }

    private static class TestNamedWriteable extends BaseNamedWriteable {

        private static final String NAME = "test-named-writeable";

        private final String field1;
        private final String field2;

        TestNamedWriteable(String field1, String field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        TestNamedWriteable(StreamInput in) throws IOException {
            field1 = in.readString();
            field2 = in.readString();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(field1);
            out.writeString(field2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestNamedWriteable that = (TestNamedWriteable) o;
            return Objects.equals(field1, that.field1) && Objects.equals(field2, that.field2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2);
        }
    }

    // create & fill byte[] with randomized data
    protected byte[] randomizedByteArrayWithSize(int size) {
        byte[] data = new byte[size];
        random().nextBytes(data);
        return data;
    }

    public void testReadWriteGeoPoint() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            GeoPoint geoPoint = new GeoPoint(randomDouble(), randomDouble());
            out.writeGenericValue(geoPoint);
            StreamInput wrap = out.bytes().streamInput();
            GeoPoint point = (GeoPoint) wrap.readGenericValue();
            assertEquals(point, geoPoint);
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            GeoPoint geoPoint = new GeoPoint(randomDouble(), randomDouble());
            out.writeGeoPoint(geoPoint);
            StreamInput wrap = out.bytes().streamInput();
            GeoPoint point = wrap.readGeoPoint();
            assertEquals(point, geoPoint);
        }
    }

    private static class TestWriteable implements Writeable {

        private boolean value;

        TestWriteable(boolean value) {
            this.value = value;
        }

        TestWriteable(StreamInput in) throws IOException {
            value = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(value);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof TestWriteable && value == ((TestWriteable) o).value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    public void testWriteMapWithConsistentOrder() throws IOException {
        Map<String, String> map = randomMap(
            new TreeMap<>(),
            randomIntBetween(2, 20),
            () -> randomAlphaOfLength(5),
            () -> randomAlphaOfLength(5)
        );

        Map<String, Object> reverseMap = new TreeMap<>(Collections.reverseOrder());
        reverseMap.putAll(map);

        List<String> mapKeys = map.entrySet().stream().map(Map.Entry::getKey).toList();
        List<String> reverseMapKeys = reverseMap.entrySet().stream().map(Map.Entry::getKey).toList();

        assertNotEquals(mapKeys, reverseMapKeys);

        try (TestStreamOutput output = new TestStreamOutput(); TestStreamOutput reverseMapOutput = new TestStreamOutput()) {
            output.writeMapWithConsistentOrder(map);
            reverseMapOutput.writeMapWithConsistentOrder(reverseMap);

            assertEquals(output.bytes(), reverseMapOutput.bytes());
        }
    }

    public void testReadMapByUsingWriteMapWithConsistentOrder() throws IOException {
        Map<String, String> streamOutMap = randomMap(
            new HashMap<>(),
            randomIntBetween(2, 20),
            () -> randomAlphaOfLength(5),
            () -> randomAlphaOfLength(5)
        );
        try (TestStreamOutput streamOut = new TestStreamOutput()) {
            streamOut.writeMapWithConsistentOrder(streamOutMap);
            StreamInput in = StreamInput.wrap(BytesReference.toBytes(streamOut.bytes()));
            Map<String, Object> streamInMap = in.readGenericMap();
            assertEquals(streamOutMap, streamInMap);
        }
    }

    public void testWriteMapWithConsistentOrderWithLinkedHashMapShouldThrowAssertError() throws IOException {
        try (TestStreamOutput output = new TestStreamOutput()) {
            Map<String, Object> map = new LinkedHashMap<>();
            Throwable e = expectThrows(AssertionError.class, () -> output.writeMapWithConsistentOrder(map));
            assertEquals(AssertionError.class, e.getClass());
        }
    }

    private static <K, V> Map<K, V> randomMap(Map<K, V> map, int size, Supplier<K> keyGenerator, Supplier<V> valueGenerator) {
        IntStream.range(0, size).forEach(i -> map.put(keyGenerator.get(), valueGenerator.get()));
        return map;
    }

    public void testWriteRandomStrings() throws IOException {
        final int iters = scaledRandomIntBetween(5, 20);
        for (int iter = 0; iter < iters; iter++) {
            List<String> strings = new ArrayList<>();
            int numStrings = randomIntBetween(100, 1000);
            TestStreamOutput output = new TestStreamOutput();
            for (int i = 0; i < numStrings; i++) {
                String s = randomRealisticUnicodeOfLengthBetween(0, 2048);
                strings.add(s);
                output.writeString(s);
            }

            try (StreamInput streamInput = output.bytes().streamInput()) {
                for (int i = 0; i < numStrings; i++) {
                    String s = streamInput.readString();
                    assertEquals(strings.get(i), s);
                }
            }
        }
    }

    /*
     * tests the extreme case where characters use more than 2 bytes
     */
    public void testWriteLargeSurrogateOnlyString() throws IOException {
        String deseretLetter = "\uD801\uDC00";
        assertEquals(2, deseretLetter.length());
        String largeString = IntStream.range(0, 2048).mapToObj(s -> deseretLetter).collect(Collectors.joining("")).trim();
        assertEquals("expands to 4 bytes", 4, new BytesRef(deseretLetter).length);
        try (TestStreamOutput output = new TestStreamOutput()) {
            output.writeString(largeString);
            try (StreamInput streamInput = output.bytes().streamInput()) {
                assertEquals(largeString, streamInput.readString());
            }
        }
    }

    public void testReadTooLargeArraySize() throws IOException {
        try (TestStreamOutput output = new TestStreamOutput()) {
            output.writeVInt(10);
            for (int i = 0; i < 10; i++) {
                output.writeInt(i);
            }

            output.writeVInt(Integer.MAX_VALUE);
            for (int i = 0; i < 10; i++) {
                output.writeInt(i);
            }
            try (StreamInput streamInput = output.bytes().streamInput()) {
                int[] ints = streamInput.readIntArray();
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, ints[i]);
                }
                expectThrows(IllegalStateException.class, () -> streamInput.readIntArray());
            }
        }
    }

    public void testReadCorruptedArraySize() throws IOException {
        try (TestStreamOutput output = new TestStreamOutput()) {
            output.writeVInt(10);
            for (int i = 0; i < 10; i++) {
                output.writeInt(i);
            }

            output.writeVInt(100);
            for (int i = 0; i < 10; i++) {
                output.writeInt(i);
            }
            try (StreamInput streamInput = output.bytes().streamInput()) {
                int[] ints = streamInput.readIntArray();
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, ints[i]);
                }
                EOFException eofException = expectThrows(EOFException.class, () -> streamInput.readIntArray());
                assertEquals("tried to read: 100 bytes but only 40 remaining", eofException.getMessage());
            }
        }
    }

    public void testReadNegativeArraySize() throws IOException {
        try (TestStreamOutput output = new TestStreamOutput()) {
            output.writeVInt(10);
            for (int i = 0; i < 10; i++) {
                output.writeInt(i);
            }

            output.writeVInt(Integer.MIN_VALUE);
            for (int i = 0; i < 10; i++) {
                output.writeInt(i);
            }
            try (StreamInput streamInput = output.bytes().streamInput()) {
                int[] ints = streamInput.readIntArray();
                for (int i = 0; i < 10; i++) {
                    assertEquals(i, ints[i]);
                }
                NegativeArraySizeException exception = expectThrows(NegativeArraySizeException.class, () -> streamInput.readIntArray());
                assertEquals("array size must be positive but was: -2147483648", exception.getMessage());
            }
        }
    }

    public void testVInt() throws IOException {
        final int value = randomInt();
        TestStreamOutput output = new TestStreamOutput();
        output.writeVInt(value);

        TestStreamOutput simple = new TestStreamOutput();
        int i = value;
        while ((i & ~0x7F) != 0) {
            simple.writeByte(((byte) ((i & 0x7f) | 0x80)));
            i >>>= 7;
        }
        simple.writeByte((byte) i);
        assertEquals(simple.bytes().toBytesRef().toString(), output.bytes().toBytesRef().toString());

        StreamInput input = output.bytes().streamInput();
        assertEquals(value, input.readVInt());
    }

    public void testVLong() throws IOException {
        final long value = randomLong();
        {
            // Read works for positive and negative numbers
            TestStreamOutput output = new TestStreamOutput();
            output.writeVLongNoCheck(value); // Use NoCheck variant so we can write negative numbers
            StreamInput input = output.bytes().streamInput();
            assertEquals(value, input.readVLong());
        }
        if (value < 0) {
            // Write doesn't work for negative numbers
            TestStreamOutput output = new TestStreamOutput();
            Exception e = expectThrows(IllegalStateException.class, () -> output.writeVLong(value));
            assertEquals("Negative longs unsupported, use writeLong or writeZLong for negative numbers [" + value + "]", e.getMessage());
        }
    }

    public enum TestEnum {
        ONE,
        TWO,
        THREE
    }

    public void testEnum() throws IOException {
        TestEnum value = randomFrom(TestEnum.values());
        TestStreamOutput output = new TestStreamOutput();
        output.writeEnum(value);
        StreamInput input = output.bytes().streamInput();
        assertEquals(value, input.readEnum(TestEnum.class));
        assertEquals(0, input.available());
    }

    public void testInvalidEnum() throws IOException {
        TestStreamOutput output = new TestStreamOutput();
        int randomNumber = randomInt();
        boolean validEnum = randomNumber >= 0 && randomNumber < TestEnum.values().length;
        output.writeVInt(randomNumber);
        StreamInput input = output.bytes().streamInput();
        if (validEnum) {
            assertEquals(TestEnum.values()[randomNumber], input.readEnum(TestEnum.class));
        } else {
            IOException ex = expectThrows(IOException.class, () -> input.readEnum(TestEnum.class));
            assertEquals("Unknown TestEnum ordinal [" + randomNumber + "]", ex.getMessage());
        }
        assertEquals(0, input.available());
    }

    private static void assertEqualityAfterSerialize(TimeValue value, int expectedSize) throws IOException {
        TestStreamOutput out = new TestStreamOutput();
        out.writeTimeValue(value);
        assertEquals(expectedSize, out.size());

        StreamInput in = out.bytes().streamInput();
        TimeValue inValue = in.readTimeValue();

        assertThat(inValue, equalTo(value));
        assertThat(inValue.duration(), equalTo(value.duration()));
        assertThat(inValue.timeUnit(), equalTo(value.timeUnit()));
    }

    public void testTimeValueSerialize() throws Exception {
        assertEqualityAfterSerialize(new TimeValue(100, TimeUnit.DAYS), 3);
        assertEqualityAfterSerialize(TimeValue.timeValueNanos(-1), 2);
        assertEqualityAfterSerialize(TimeValue.timeValueNanos(1), 2);
        assertEqualityAfterSerialize(TimeValue.timeValueSeconds(30), 2);

        final TimeValue timeValue = new TimeValue(randomIntBetween(0, 1024), randomFrom(TimeUnit.values()));
        TestStreamOutput out = new TestStreamOutput();
        out.writeZLong(timeValue.duration());
        assertEqualityAfterSerialize(timeValue, 1 + out.bytes().length());
    }

    public void testTimeValueInterning() throws IOException {
        try (var bytesOut = new BytesStreamOutput()) {
            bytesOut.writeTimeValue(randomBoolean() ? TimeValue.MINUS_ONE : new TimeValue(-1, TimeUnit.MILLISECONDS));
            bytesOut.writeTimeValue(randomBoolean() ? TimeValue.ZERO : new TimeValue(0, TimeUnit.MILLISECONDS));
            bytesOut.writeTimeValue(randomBoolean() ? TimeValue.THIRTY_SECONDS : new TimeValue(30, TimeUnit.SECONDS));
            bytesOut.writeTimeValue(randomBoolean() ? TimeValue.ONE_MINUTE : new TimeValue(1, TimeUnit.MINUTES));

            try (var in = bytesOut.bytes().streamInput()) {
                assertSame(TimeValue.MINUS_ONE, in.readTimeValue());
                assertSame(TimeValue.ZERO, in.readTimeValue());
                assertSame(TimeValue.THIRTY_SECONDS, in.readTimeValue());
                assertSame(TimeValue.ONE_MINUTE, in.readTimeValue());
            }
        }
    }

    private static class TestStreamOutput extends BytesStream {

        private final BytesStreamOutput output = new BytesStreamOutput();
        private final CountingStreamOutput counting = new CountingStreamOutput();

        @Override
        public void writeByte(byte b) {
            output.writeByte(b);
            counting.writeByte(b);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) {
            output.writeBytes(b, offset, length);
            counting.writeBytes(b, offset, length);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void writeInt(int i) throws IOException {
            output.writeInt(i);
            counting.writeInt(i);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void writeIntArray(int[] values) throws IOException {
            output.writeIntArray(values);
            counting.writeIntArray(values);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void writeLong(long i) throws IOException {
            output.writeLong(i);
            counting.writeLong(i);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void writeLongArray(long[] values) throws IOException {
            output.writeLongArray(values);
            counting.writeLongArray(values);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void writeFloat(float v) throws IOException {
            output.writeFloat(v);
            counting.writeFloat(v);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void writeFloatArray(float[] values) throws IOException {
            output.writeFloatArray(values);
            counting.writeFloatArray(values);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void writeDouble(double v) throws IOException {
            output.writeDouble(v);
            counting.writeDouble(v);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void writeDoubleArray(double[] values) throws IOException {
            output.writeDoubleArray(values);
            counting.writeDoubleArray(values);
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public BytesReference bytes() {
            BytesReference bytesReference = output.bytes();
            assertThat((long) bytesReference.length(), equalTo(counting.size()));
            return bytesReference;
        }

        @Override
        public void seek(long position) {
            output.seek(position);
        }

        public int size() {
            int size = output.size();
            assertThat((long) size, equalTo(counting.size()));
            return size;
        }

        @Override
        public void flush() {
            output.flush();
            counting.flush();
            assertThat((long) output.size(), equalTo(counting.size()));
        }

        @Override
        public void close() {
            assertThat((long) output.size(), equalTo(counting.size()));
            output.close();
            counting.close();
        }
    }
}
