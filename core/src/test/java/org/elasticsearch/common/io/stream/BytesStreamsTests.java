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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link BytesStreamOutput} paging behaviour.
 */
public class BytesStreamsTests extends ESTestCase {

    @Test
    public void testEmpty() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        // test empty stream to array
        assertEquals(0, out.size());
        assertEquals(0, out.bytes().toBytes().length);

        out.close();
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
    public void testSkip() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        int position = 0;
        assertEquals(position, out.position());

        int forward = 100;
        out.skip(forward);
        assertEquals(position + forward, out.position());

        out.close();
    }

    @Test
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
        long[] longArray = {1, 2, 3};
        out.writeGenericValue(longArray);
        float[] floatArray = {1.1f, 2.2f, 3.3f};
        out.writeGenericValue(floatArray);
        double[] doubleArray = {1.1, 2.2, 3.3};
        out.writeGenericValue(doubleArray);
        out.writeString("hello");
        out.writeString("goodbye");
        out.writeGenericValue(BytesRefs.toBytesRef("bytesref"));
        StreamInput in = StreamInput.wrap(out.bytes().toBytes());
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
        assertThat(in.readGenericValue(), equalTo((Object)longArray));
        assertThat(in.readGenericValue(), equalTo((Object)floatArray));
        assertThat(in.readGenericValue(), equalTo((Object)doubleArray));
        assertThat(in.readString(), equalTo("hello"));
        assertThat(in.readString(), equalTo("goodbye"));
        assertThat(in.readGenericValue(), equalTo((Object)BytesRefs.toBytesRef("bytesref")));
        in.close();
        out.close();
    }

    @Test
    public void testNamedWriteable() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.registerPrototype(BaseNamedWriteable.class, new TestNamedWriteable(null, null));
        TestNamedWriteable namedWriteableIn = new TestNamedWriteable(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
        out.writeNamedWriteable(namedWriteableIn);
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes().toBytes()), namedWriteableRegistry);
        BaseNamedWriteable namedWriteableOut = in.readNamedWriteable(BaseNamedWriteable.class);
        assertEquals(namedWriteableOut, namedWriteableIn);
    }

    @Test
    public void testNamedWriteableDuplicates() throws IOException {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.registerPrototype(BaseNamedWriteable.class, new TestNamedWriteable(null, null));
        try {
            namedWriteableRegistry.registerPrototype(BaseNamedWriteable.class, new TestNamedWriteable(null, null));
            fail("registerPrototype should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("named writeable of type [" + TestNamedWriteable.class.getName() + "] with name [" + TestNamedWriteable.NAME + "] is already registered by type ["
                    + TestNamedWriteable.class.getName() + "] within category [" + BaseNamedWriteable.class.getName() + "]"));
        }
    }

    @Test
    public void testNamedWriteableUnknownCategory() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(new TestNamedWriteable("test1", "test2"));
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes().toBytes()), new NamedWriteableRegistry());
        try {
            //no named writeable registered with given name, can write but cannot read it back
            in.readNamedWriteable(BaseNamedWriteable.class);
            fail("read should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unknown named writeable category [" + BaseNamedWriteable.class.getName() + "]"));
        }
    }

    @Test
    public void testNamedWriteableUnknownNamedWriteable() throws IOException {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.registerPrototype(BaseNamedWriteable.class, new TestNamedWriteable(null, null));
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(new NamedWriteable() {
            @Override
            public String getWriteableName() {
                return "unknown";
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
            }

            @Override
            public Object readFrom(StreamInput in) throws IOException {
                return null;
            }
        });
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes().toBytes()), namedWriteableRegistry);
        try {
            //no named writeable registered with given name under test category, can write but cannot read it back
            in.readNamedWriteable(BaseNamedWriteable.class);
            fail("read should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unknown named writeable with name [unknown] within category [" + BaseNamedWriteable.class.getName() + "]"));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNamedWriteableNotSupportedWithoutWrapping() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        TestNamedWriteable testNamedWriteable = new TestNamedWriteable("test1", "test2");
        out.writeNamedWriteable(testNamedWriteable);
        StreamInput in = StreamInput.wrap(out.bytes().toBytes());
        in.readNamedWriteable(BaseNamedWriteable.class);
    }

    private static abstract class BaseNamedWriteable<T> implements NamedWriteable<T> {

    }

    private static class TestNamedWriteable extends BaseNamedWriteable<TestNamedWriteable> {

        private static final String NAME = "test-named-writeable";

        private final String field1;
        private final String field2;

        TestNamedWriteable(String field1, String field2) {
            this.field1 = field1;
            this.field2 = field2;
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
        public TestNamedWriteable readFrom(StreamInput in) throws IOException {
            return new TestNamedWriteable(in.readString(), in.readString());
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
    @Test
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
        getRandom().nextBytes(data);
        return data;
    }
}
