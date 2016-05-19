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

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for {@link BytesStreamOutput} paging behaviour.
 */
public class BytesStreamsTests extends ESTestCase {
    public void testEmpty() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        // test empty stream to array
        assertEquals(0, out.size());
        assertEquals(0, out.bytes().toBytes().length);

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
        assertArrayEquals(expectedData, out.bytes().toBytes());

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
        assertArrayEquals(expectedData, out.bytes().toBytes());

        out.close();
    }

    public void testIllegalBulkWrite() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        // bulk-write with wrong args
        try {
            out.writeBytes(new byte[]{}, 0, 1);
            fail("expected IllegalArgumentException: length > (size-offset)");
        }
        catch (IllegalArgumentException iax1) {
            // expected
        }

        out.close();
    }

    public void testSingleShortPageBulkWrite() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        // first bulk-write empty array: should not change anything
        int expectedSize = 0;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);
        out.writeBytes(expectedData);
        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, out.bytes().toBytes());

        // bulk-write again with actual bytes
        expectedSize = 10;
        expectedData = randomizedByteArrayWithSize(expectedSize);
        out.writeBytes(expectedData);
        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, out.bytes().toBytes());

        out.close();
    }

    public void testSingleFullPageBulkWrite() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int expectedSize = BigArrays.BYTE_PAGE_SIZE;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write in bulk
        out.writeBytes(expectedData);

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, out.bytes().toBytes());

        out.close();
    }

    public void testSingleFullPageBulkWriteWithOffset() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int initialOffset = 10;
        int additionalLength = BigArrays.BYTE_PAGE_SIZE;
        byte[] expectedData = randomizedByteArrayWithSize(initialOffset + additionalLength);

        // first create initial offset
        out.writeBytes(expectedData, 0, initialOffset);
        assertEquals(initialOffset, out.size());

        // now write the rest - more than fits into the remaining first page
        out.writeBytes(expectedData, initialOffset, additionalLength);
        assertEquals(expectedData.length, out.size());
        assertArrayEquals(expectedData, out.bytes().toBytes());

        out.close();
    }

    public void testSingleFullPageBulkWriteWithOffsetCrossover() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int initialOffset = 10;
        int additionalLength = BigArrays.BYTE_PAGE_SIZE * 2;
        byte[] expectedData = randomizedByteArrayWithSize(initialOffset + additionalLength);
        out.writeBytes(expectedData, 0, initialOffset);
        assertEquals(initialOffset, out.size());

        // now write the rest - more than fits into the remaining page + a full page after
        // that,
        // ie. we cross over into a third
        out.writeBytes(expectedData, initialOffset, additionalLength);
        assertEquals(expectedData.length, out.size());
        assertArrayEquals(expectedData, out.bytes().toBytes());

        out.close();
    }

    public void testSingleFullPage() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int expectedSize = BigArrays.BYTE_PAGE_SIZE;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write byte-by-byte
        for (int i = 0; i < expectedSize; i++) {
            out.writeByte(expectedData[i]);
        }

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, out.bytes().toBytes());

        out.close();
    }

    public void testOneFullOneShortPage() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int expectedSize = BigArrays.BYTE_PAGE_SIZE + 10;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write byte-by-byte
        for (int i = 0; i < expectedSize; i++) {
            out.writeByte(expectedData[i]);
        }

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, out.bytes().toBytes());

        out.close();
    }

    public void testTwoFullOneShortPage() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int expectedSize = (BigArrays.BYTE_PAGE_SIZE * 2) + 1;
        byte[] expectedData = randomizedByteArrayWithSize(expectedSize);

        // write byte-by-byte
        for (int i = 0; i < expectedSize; i++) {
            out.writeByte(expectedData[i]);
        }

        assertEquals(expectedSize, out.size());
        assertArrayEquals(expectedData, out.bytes().toBytes());

        out.close();
    }

    public void testSeek() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int position = 0;
        assertEquals(position, out.position());

        out.seek(position += 10);
        out.seek(position += BigArrays.BYTE_PAGE_SIZE);
        out.seek(position += BigArrays.BYTE_PAGE_SIZE + 10);
        out.seek(position += BigArrays.BYTE_PAGE_SIZE * 2);
        assertEquals(position, out.position());
        assertEquals(position, out.bytes().toBytes().length);

        out.close();
    }

    public void testSkip() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int position = 0;
        assertEquals(position, out.position());

        int forward = 100;
        out.skip(forward);
        assertEquals(position + forward, out.position());

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
        final byte[] bytes = out.bytes().toBytes();
        StreamInput in = StreamInput.wrap(out.bytes().toBytes());
        assertEquals(in.available(), bytes.length);
        assertThat(in.readBoolean(), equalTo(false));
        assertThat(in.readByte(), equalTo((byte)1));
        assertThat(in.readShort(), equalTo((short)-1));
        assertThat(in.readInt(), equalTo(-1));
        assertThat(in.readVInt(), equalTo(2));
        assertThat(in.readLong(), equalTo((long)-3));
        assertThat(in.readVLong(), equalTo((long)4));
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
        assertEquals(0, in.available());
        in.close();
        out.close();
    }

    public void testNamedWriteable() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.register(BaseNamedWriteable.class, TestNamedWriteable.NAME, TestNamedWriteable::new);
        TestNamedWriteable namedWriteableIn = new TestNamedWriteable(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
        out.writeNamedWriteable(namedWriteableIn);
        byte[] bytes = out.bytes().toBytes();
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry);
        assertEquals(in.available(), bytes.length);
        BaseNamedWriteable namedWriteableOut = in.readNamedWriteable(BaseNamedWriteable.class);
        assertEquals(namedWriteableOut, namedWriteableIn);
        assertEquals(in.available(), 0);
    }

    public void testNamedWriteableDuplicates() throws IOException {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.register(BaseNamedWriteable.class, TestNamedWriteable.NAME, TestNamedWriteable::new);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> namedWriteableRegistry.register(BaseNamedWriteable.class, TestNamedWriteable.NAME, TestNamedWriteable::new));
        assertThat(e.getMessage(), startsWith("named writeable [" + BaseNamedWriteable.class.getName() + "][" + TestNamedWriteable.NAME
                + "] is already registered by ["));
    }

    public void testNamedWriteableUnknownCategory() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(new TestNamedWriteable("test1", "test2"));
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes().toBytes()), new NamedWriteableRegistry());
        //no named writeable registered with given name, can write but cannot read it back
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> in.readNamedWriteable(BaseNamedWriteable.class));
        assertThat(e.getMessage(), equalTo("unknown named writeable category [" + BaseNamedWriteable.class.getName() + "]"));
    }

    public void testNamedWriteableUnknownNamedWriteable() throws IOException {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.register(BaseNamedWriteable.class, TestNamedWriteable.NAME, TestNamedWriteable::new);
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(new NamedWriteable() {
            @Override
            public String getWriteableName() {
                return "unknown";
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
            }
        });
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes().toBytes()), namedWriteableRegistry);
        try {
            //no named writeable registered with given name under test category, can write but cannot read it back
            in.readNamedWriteable(BaseNamedWriteable.class);
            fail("read should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unknown named writeable [" + BaseNamedWriteable.class.getName() + "][unknown]"));
        }
    }

    public void testNamedWriteableNotSupportedWithoutWrapping() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        TestNamedWriteable testNamedWriteable = new TestNamedWriteable("test1", "test2");
        out.writeNamedWriteable(testNamedWriteable);
        StreamInput in = StreamInput.wrap(out.bytes().toBytes());
        try {
            in.readNamedWriteable(BaseNamedWriteable.class);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage(), is("can't read named writeable from StreamInput"));
        }
    }

    public void testNamedWriteableReaderReturnsNull() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.register(BaseNamedWriteable.class, TestNamedWriteable.NAME, (StreamInput in) -> null);
        TestNamedWriteable namedWriteableIn = new TestNamedWriteable(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
        out.writeNamedWriteable(namedWriteableIn);
        byte[] bytes = out.bytes().toBytes();
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry);
        assertEquals(in.available(), bytes.length);
        IOException e = expectThrows(IOException.class, () -> in.readNamedWriteable(BaseNamedWriteable.class));
        assertThat(e.getMessage(), endsWith("] returned null which is not allowed and probably means it screwed up the stream."));
    }

    public void testOptionalWriteableReaderReturnsNull() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeOptionalWriteable(new TestNamedWriteable(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10)));
        StreamInput in = StreamInput.wrap(out.bytes().toBytes());
        IOException e = expectThrows(IOException.class, () -> in.readOptionalWriteable((StreamInput ignored) -> null));
        assertThat(e.getMessage(), endsWith("] returned null which is not allowed and probably means it screwed up the stream."));
    }

    public void testWriteableReaderReturnsWrongName() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.register(BaseNamedWriteable.class, TestNamedWriteable.NAME, (StreamInput in) -> new TestNamedWriteable(in) {
            @Override
            public String getWriteableName() {
                return "intentionally-broken";
            }
        });
        TestNamedWriteable namedWriteableIn = new TestNamedWriteable(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
        out.writeNamedWriteable(namedWriteableIn);
        byte[] bytes = out.bytes().toBytes();
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry);
        assertEquals(in.available(), bytes.length);
        AssertionError e = expectThrows(AssertionError.class, () -> in.readNamedWriteable(BaseNamedWriteable.class));
        assertThat(e.getMessage(),
                endsWith(" claims to have a different name [intentionally-broken] than it was read from [test-named-writeable]."));
    }

    public void testWriteStreamableList() throws IOException {
        final int size = randomIntBetween(0, 5);
        final List<TestStreamable> expected = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            expected.add(new TestStreamable(randomBoolean()));
        }

        final BytesStreamOutput out = new BytesStreamOutput();
        out.writeStreamableList(expected);

        final StreamInput in = StreamInput.wrap(out.bytes().toBytes());

        List<TestStreamable> loaded = in.readStreamableList(TestStreamable::new);

        assertThat(loaded, hasSize(expected.size()));

        for (int i = 0; i < expected.size(); ++i) {
            assertEquals(expected.get(i).value, loaded.get(i).value);
        }

        assertEquals(0, in.available());

        in.close();
        out.close();
    }

    private static abstract class BaseNamedWriteable implements NamedWriteable {

    }

    private static class TestNamedWriteable extends BaseNamedWriteable {

        private static final String NAME = "test-named-writeable";

        private final String field1;
        private final String field2;

        TestNamedWriteable(String field1, String field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        public TestNamedWriteable(StreamInput in) throws IOException {
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

    // we ignore this test for now since all existing callers of BytesStreamOutput happily
    // call bytes() after close().
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/12620")
    public void testAccessAfterClose() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        // immediately close
        out.close();

        assertEquals(-1, out.size());
        assertEquals(-1, out.position());

        // writing a single byte must fail
        try {
            out.writeByte((byte)0);
            fail("expected IllegalStateException: stream closed");
        }
        catch (IllegalStateException iex1) {
            // expected
        }

        // writing in bulk must fail
        try {
            out.writeBytes(new byte[0], 0, 0);
            fail("expected IllegalStateException: stream closed");
        }
        catch (IllegalStateException iex1) {
            // expected
        }

        // toByteArray() must fail
        try {
            out.bytes().toBytes();
            fail("expected IllegalStateException: stream closed");
        }
        catch (IllegalStateException iex1) {
            // expected
        }

    }

    // create & fill byte[] with randomized data
    protected byte[] randomizedByteArrayWithSize(int size) {
        byte[] data = new byte[size];
        random().nextBytes(data);
        return data;
    }

    public void testReadWriteGeoPoint() throws IOException {
        {
            BytesStreamOutput out = new BytesStreamOutput();
            GeoPoint geoPoint = new GeoPoint(randomDouble(), randomDouble());
            out.writeGenericValue(geoPoint);
            StreamInput wrap = StreamInput.wrap(out.bytes());
            GeoPoint point = (GeoPoint) wrap.readGenericValue();
            assertEquals(point, geoPoint);
        }
        {
            BytesStreamOutput out = new BytesStreamOutput();
            GeoPoint geoPoint = new GeoPoint(randomDouble(), randomDouble());
            out.writeGeoPoint(geoPoint);
            StreamInput wrap = StreamInput.wrap(out.bytes());
            GeoPoint point = wrap.readGeoPoint();
            assertEquals(point, geoPoint);
        }
    }

    private static class TestStreamable implements Streamable {

        private boolean value;

        public TestStreamable() { }

        public TestStreamable(boolean value) {
            this.value = value;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            value = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(value);
        }
    }
}
