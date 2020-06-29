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

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.EOFException;
import java.io.IOException;
import java.time.ZoneId;
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
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests for {@link BytesStreamOutput} paging behaviour.
 */
public class BytesStreamsTests extends ESTestCase {
    public void testEmpty() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        // test empty stream to array
        assertEquals(0, out.size());
        assertEquals(0, out.bytes().length());

        out.close();
    }

    public void testSingleByte() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
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
        BytesStreamOutput out = new BytesStreamOutput();

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
        BytesStreamOutput out = new BytesStreamOutput();

        // bulk-write with wrong args
        expectThrows(IllegalArgumentException.class, () -> out.writeBytes(new byte[]{}, 0, 1));
        out.close();
    }

    public void testSingleShortPageBulkWrite() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

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
        BytesStreamOutput out = new BytesStreamOutput();

        int expectedSize = PageCacheRecycler.BYTE_PAGE_SIZE;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write in bulk
        out.writeBytes(expectedData);

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, BytesReference.toBytes(out.bytes()));

        out.close();
    }

    public void testSingleFullPageBulkWriteWithOffset() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

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
        BytesStreamOutput out = new BytesStreamOutput();

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
        BytesStreamOutput out = new BytesStreamOutput();

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
        BytesStreamOutput out = new BytesStreamOutput();

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
        BytesStreamOutput out = new BytesStreamOutput();

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
        BytesStreamOutput out = new BytesStreamOutput();
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
        int[] intArray = {1, 2, 3};
        out.writeGenericValue(intArray);
        int[] vIntArray = {4, 5, 6};
        out.writeVIntArray(vIntArray);
        long[] longArray = {1, 2, 3};
        out.writeGenericValue(longArray);
        long[] vLongArray = {4, 5, 6};
        out.writeVLongArray(vLongArray);
        float[] floatArray = {1.1f, 2.2f, 3.3f};
        out.writeGenericValue(floatArray);
        double[] doubleArray = {1.1, 2.2, 3.3};
        out.writeGenericValue(doubleArray);
        out.writeString("hello");
        out.writeString("goodbye");
        out.writeGenericValue(BytesRefs.toBytesRef("bytesref"));
        out.writeStringArray(new String[] {"a", "b", "cat"});
        out.writeBytesReference(new BytesArray("test"));
        out.writeOptionalBytesReference(new BytesArray("test"));
        out.writeOptionalDouble(null);
        out.writeOptionalDouble(1.2);
        out.writeTimeZone(DateTimeZone.forID("CET"));
        out.writeOptionalTimeZone(DateTimeZone.getDefault());
        out.writeOptionalTimeZone(null);
        out.writeGenericValue(new DateTime(123456, DateTimeZone.forID("America/Los_Angeles")));
        final byte[] bytes = BytesReference.toBytes(out.bytes());
        StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
        assertEquals(in.available(), bytes.length);
        assertThat(in.readBoolean(), equalTo(false));
        assertThat(in.readByte(), equalTo((byte)1));
        assertThat(in.readShort(), equalTo((short)-1));
        assertThat(in.readInt(), equalTo(-1));
        assertThat(in.readVInt(), equalTo(2));
        assertThat(in.readLong(), equalTo(-3L));
        assertThat(in.readVLong(), equalTo(4L));
        assertThat(in.readOptionalLong(), equalTo(11234234L));
        assertThat(in.readOptionalVLong(), equalTo(5L));
        assertThat(in.readOptionalVLong(), nullValue());
        assertThat((double)in.readFloat(), closeTo(1.1, 0.0001));
        assertThat(in.readDouble(), closeTo(2.2, 0.0001));
        assertThat(in.readGenericValue(), equalTo((Object) intArray));
        assertThat(in.readVIntArray(), equalTo(vIntArray));
        assertThat(in.readGenericValue(), equalTo((Object)longArray));
        assertThat(in.readVLongArray(), equalTo(vLongArray));
        assertThat(in.readGenericValue(), equalTo((Object)floatArray));
        assertThat(in.readGenericValue(), equalTo((Object)doubleArray));
        assertThat(in.readString(), equalTo("hello"));
        assertThat(in.readString(), equalTo("goodbye"));
        assertThat(in.readGenericValue(), equalTo((Object)BytesRefs.toBytesRef("bytesref")));
        assertThat(in.readStringArray(), equalTo(new String[] {"a", "b", "cat"}));
        assertThat(in.readBytesReference(), equalTo(new BytesArray("test")));
        assertThat(in.readOptionalBytesReference(), equalTo(new BytesArray("test")));
        assertNull(in.readOptionalDouble());
        assertThat(in.readOptionalDouble(), closeTo(1.2, 0.0001));
        assertEquals(DateTimeZone.forID("CET"), in.readTimeZone());
        assertEquals(DateTimeZone.getDefault(), in.readOptionalTimeZone());
        assertNull(in.readOptionalTimeZone());
        Object dt = in.readGenericValue();
        assertThat(dt, instanceOf(JodaCompatibleZonedDateTime.class));
        JodaCompatibleZonedDateTime jdt = (JodaCompatibleZonedDateTime) dt;
        assertThat(jdt.getZonedDateTime().toInstant().toEpochMilli(), equalTo(123456L));
        assertThat(jdt.getZonedDateTime().getZone(), equalTo(ZoneId.of("America/Los_Angeles")));
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
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.singletonList(
                    new NamedWriteableRegistry.Entry(BaseNamedWriteable.class, TestNamedWriteable.NAME, TestNamedWriteable::new)));
            TestNamedWriteable namedWriteableIn = new TestNamedWriteable(randomAlphaOfLengthBetween(1, 10),
                    randomAlphaOfLengthBetween(1, 10));
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
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.singletonList(
            new NamedWriteableRegistry.Entry(BaseNamedWriteable.class, TestNamedWriteable.NAME, TestNamedWriteable::new)
        ));
        int size = between(0, 100);
        List<BaseNamedWriteable> expected = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            expected.add(new TestNamedWriteable(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteableList(expected);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)) {
                assertEquals(expected, in.readNamedWriteableList(BaseNamedWriteable.class));
                assertEquals(0, in.available());
            }
        }
    }

    public void testNamedWriteableNotSupportedWithoutWrapping() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            TestNamedWriteable testNamedWriteable = new TestNamedWriteable("test1", "test2");
            out.writeNamedWriteable(testNamedWriteable);
            StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
            Exception e = expectThrows(UnsupportedOperationException.class, () -> in.readNamedWriteable(BaseNamedWriteable.class));
            assertThat(e.getMessage(), is("can't read named writeable from StreamInput"));
        }
    }

    public void testNamedWriteableReaderReturnsNull() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.singletonList(
                    new NamedWriteableRegistry.Entry(BaseNamedWriteable.class, TestNamedWriteable.NAME, (StreamInput in) -> null)));
            TestNamedWriteable namedWriteableIn = new TestNamedWriteable(randomAlphaOfLengthBetween(1, 10),
                    randomAlphaOfLengthBetween(1, 10));
            out.writeNamedWriteable(namedWriteableIn);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry)) {
                assertEquals(in.available(), bytes.length);
                IOException e = expectThrows(IOException.class, () -> in.readNamedWriteable(BaseNamedWriteable.class));
                assertThat(e.getMessage(), endsWith("] returned null which is not allowed and probably means it screwed up the stream."));
            }
        }
    }

    public void testOptionalWriteableReaderReturnsNull() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeOptionalWriteable(new TestNamedWriteable(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
            StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
            IOException e = expectThrows(IOException.class, () -> in.readOptionalWriteable((StreamInput ignored) -> null));
            assertThat(e.getMessage(), endsWith("] returned null which is not allowed and probably means it screwed up the stream."));
        }
    }

    public void testWriteableReaderReturnsWrongName() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
                    Collections.singletonList(new NamedWriteableRegistry.Entry(BaseNamedWriteable.class, TestNamedWriteable.NAME,
                            (StreamInput in) -> new TestNamedWriteable(in) {
                                @Override
                                public String getWriteableName() {
                                    return "intentionally-broken";
                                }
                            })));
            TestNamedWriteable namedWriteableIn = new TestNamedWriteable(randomAlphaOfLengthBetween(1, 10),
                    randomAlphaOfLengthBetween(1, 10));
            out.writeNamedWriteable(namedWriteableIn);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry)) {
                assertEquals(in.available(), bytes.length);
                AssertionError e = expectThrows(AssertionError.class, () -> in.readNamedWriteable(BaseNamedWriteable.class));
                assertThat(e.getMessage(),
                        endsWith(" claims to have a different name [intentionally-broken] than it was read from [test-named-writeable]."));
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
        out.writeList(expected);

        final StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));

        final List<TestWriteable> loaded = in.readList(TestWriteable::new);

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
        final Map<String, String> expected = new HashMap<>(randomIntBetween(0, 100));
        for (int i = 0; i < size; ++i) {
            expected.put(randomAlphaOfLength(2), randomAlphaOfLength(5));
        }

        final BytesStreamOutput out = new BytesStreamOutput();
        out.writeMap(expected, StreamOutput::writeString, StreamOutput::writeString);
        final StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
        final Map<String, String> loaded = in.readMap(StreamInput::readString, StreamInput::readString);

        assertThat(loaded.size(), equalTo(expected.size()));
        assertThat(expected, equalTo(loaded));
    }

    public void testWriteMapOfLists() throws IOException {
        final int size = randomIntBetween(0, 5);
        final Map<String, List<String>> expected = new HashMap<>(size);

        for (int i = 0; i < size; ++i) {
            int listSize = randomIntBetween(0, 5);
            List<String> list = new ArrayList<>(listSize);

            for (int j = 0; j < listSize; ++j) {
                list.add(randomAlphaOfLength(5));
            }

            expected.put(randomAlphaOfLength(2), list);
        }

        final BytesStreamOutput out = new BytesStreamOutput();
        out.writeMapOfLists(expected, StreamOutput::writeString, StreamOutput::writeString);

        final StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));

        final Map<String, List<String>> loaded = in.readMapOfLists(StreamInput::readString, StreamInput::readString);

        assertThat(loaded.size(), equalTo(expected.size()));

        for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
            assertThat(loaded.containsKey(entry.getKey()), equalTo(true));

            List<String> loadedList = loaded.get(entry.getKey());

            assertThat(loadedList, hasSize(entry.getValue().size()));

            for (int i = 0; i < loadedList.size(); ++i) {
                assertEquals(entry.getValue().get(i), loadedList.get(i));
            }
        }

        assertEquals(0, in.available());

        in.close();
        out.close();
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
            return Objects.equals(field1, that.field1) &&
                    Objects.equals(field2, that.field2);
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
    }

    public void testWriteMapWithConsistentOrder() throws IOException {
        Map<String, String> map =
            randomMap(new TreeMap<>(), randomIntBetween(2, 20),
                () -> randomAlphaOfLength(5),
                () -> randomAlphaOfLength(5));

        Map<String, Object> reverseMap = new TreeMap<>(Collections.reverseOrder());
        reverseMap.putAll(map);

        List<String> mapKeys = map.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        List<String> reverseMapKeys = reverseMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());

        assertNotEquals(mapKeys, reverseMapKeys);

        try (BytesStreamOutput output = new BytesStreamOutput(); BytesStreamOutput reverseMapOutput = new BytesStreamOutput()) {
            output.writeMapWithConsistentOrder(map);
            reverseMapOutput.writeMapWithConsistentOrder(reverseMap);

            assertEquals(output.bytes(), reverseMapOutput.bytes());
        }
    }

    public void testReadMapByUsingWriteMapWithConsistentOrder() throws IOException {
        Map<String, String> streamOutMap =
            randomMap(new HashMap<>(), randomIntBetween(2, 20),
                () -> randomAlphaOfLength(5),
                () -> randomAlphaOfLength(5));
        try (BytesStreamOutput streamOut = new BytesStreamOutput()) {
            streamOut.writeMapWithConsistentOrder(streamOutMap);
            StreamInput in = StreamInput.wrap(BytesReference.toBytes(streamOut.bytes()));
            Map<String, Object> streamInMap = in.readMap();
            assertEquals(streamOutMap, streamInMap);
        }
    }

    public void testWriteMapWithConsistentOrderWithLinkedHashMapShouldThrowAssertError() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
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
            BytesStreamOutput output = new BytesStreamOutput(0);
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
        try (BytesStreamOutput output = new BytesStreamOutput(0)) {
            output.writeString(largeString);
            try (StreamInput streamInput = output.bytes().streamInput()) {
                assertEquals(largeString, streamInput.readString());
            }
        }
    }

    public void testReadTooLargeArraySize() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput(0)) {
            output.writeVInt(10);
            for (int i = 0; i < 10; i ++) {
                output.writeInt(i);
            }

            output.writeVInt(Integer.MAX_VALUE);
            for (int i = 0; i < 10; i ++) {
                output.writeInt(i);
            }
            try (StreamInput streamInput = output.bytes().streamInput()) {
                int[] ints = streamInput.readIntArray();
                for (int i = 0; i < 10; i ++) {
                    assertEquals(i, ints[i]);
                }
                expectThrows(IllegalStateException.class, () -> streamInput.readIntArray());
            }
        }
    }

    public void testReadCorruptedArraySize() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput(0)) {
            output.writeVInt(10);
            for (int i = 0; i < 10; i ++) {
                output.writeInt(i);
            }

            output.writeVInt(100);
            for (int i = 0; i < 10; i ++) {
                output.writeInt(i);
            }
            try (StreamInput streamInput = output.bytes().streamInput()) {
                int[] ints = streamInput.readIntArray();
                for (int i = 0; i < 10; i ++) {
                    assertEquals(i, ints[i]);
                }
                EOFException eofException = expectThrows(EOFException.class, () -> streamInput.readIntArray());
                assertEquals("tried to read: 100 bytes but only 40 remaining", eofException.getMessage());
            }
        }
    }

    public void testReadNegativeArraySize() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput(0)) {
            output.writeVInt(10);
            for (int i = 0; i < 10; i ++) {
                output.writeInt(i);
            }

            output.writeVInt(Integer.MIN_VALUE);
            for (int i = 0; i < 10; i ++) {
                output.writeInt(i);
            }
            try (StreamInput streamInput = output.bytes().streamInput()) {
                int[] ints = streamInput.readIntArray();
                for (int i = 0; i < 10; i ++) {
                    assertEquals(i, ints[i]);
                }
                NegativeArraySizeException exception = expectThrows(NegativeArraySizeException.class, () -> streamInput.readIntArray());
                assertEquals("array size must be positive but was: -2147483648", exception.getMessage());
            }
        }
    }

    public void testVInt() throws IOException {
        final int value = randomInt();
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeVInt(value);
        StreamInput input = output.bytes().streamInput();
        assertEquals(value, input.readVInt());
    }

    public void testVLong() throws IOException {
        final long value = randomLong();
        {
            // Read works for positive and negative numbers
            BytesStreamOutput output = new BytesStreamOutput();
            output.writeVLongNoCheck(value); // Use NoCheck variant so we can write negative numbers
            StreamInput input = output.bytes().streamInput();
            assertEquals(value, input.readVLong());
        }
        if (value < 0) {
            // Write doesn't work for negative numbers
            BytesStreamOutput output = new BytesStreamOutput();
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
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeEnum(value);
        StreamInput input = output.bytes().streamInput();
        assertEquals(value, input.readEnum(TestEnum.class));
        assertEquals(0, input.available());
    }

    public void testInvalidEnum() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
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
        BytesStreamOutput out = new BytesStreamOutput();
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
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeZLong(timeValue.duration());
        assertEqualityAfterSerialize(timeValue, 1 + out.bytes().length());
    }

    public void testWriteCircularReferenceException() throws IOException {
        IOException rootEx = new IOException("disk broken");
        AlreadyClosedException ace = new AlreadyClosedException("closed", rootEx);
        rootEx.addSuppressed(ace); // circular reference

        BytesStreamOutput testOut = new BytesStreamOutput();
        AssertionError error = expectThrows(AssertionError.class, () -> testOut.writeException(rootEx));
        assertThat(error.getMessage(), containsString("too many nested exceptions"));
        assertThat(error.getCause(), equalTo(rootEx));

        BytesStreamOutput prodOut = new BytesStreamOutput() {
            @Override
            boolean failOnTooManyNestedExceptions(Throwable throwable) {
                assertThat(throwable, sameInstance(rootEx));
                return true;
            }
        };
        prodOut.writeException(rootEx);
        StreamInput in = prodOut.bytes().streamInput();
        Exception newEx = in.readException();
        assertThat(newEx, instanceOf(IOException.class));
        assertThat(newEx.getMessage(), equalTo("disk broken"));
        assertArrayEquals(newEx.getStackTrace(), rootEx.getStackTrace());
    }
}
