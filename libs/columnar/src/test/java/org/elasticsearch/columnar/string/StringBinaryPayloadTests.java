/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * The {@code BinaryDocValues} payload wire format. Beyond round-tripping, this pins the exact byte layout for
 * a known input, so a change to the payload shape has to be deliberate rather than incidental.
 */
public class StringBinaryPayloadTests extends ESTestCase {

    public void testExactLayoutSingleValue() {
        BytesRef payload = StringBinaryPayload.encode(new BytesRef[] { new BytesRef("ab") }, 1, new BytesRefBuilder());
        // [vint count=1][vint len=2]['a']['b']
        assertArrayEquals(new byte[] { 1, 2, 'a', 'b' }, toArray(payload));
    }

    public void testExactLayoutMultipleValues() {
        BytesRef[] values = { new BytesRef("a"), new BytesRef(""), new BytesRef("cd") };
        BytesRef payload = StringBinaryPayload.encode(values, 3, new BytesRefBuilder());
        // [count=3][len=1]['a'][len=0][len=2]['c']['d']
        assertArrayEquals(new byte[] { 3, 1, 'a', 0, 2, 'c', 'd' }, toArray(payload));
    }

    public void testEmptyPayload() {
        BytesRef payload = StringBinaryPayload.encode(new BytesRef[0], 0, new BytesRefBuilder());
        assertArrayEquals(new byte[] { 0 }, toArray(payload));
        assertEquals(0, new StringBinaryPayload.Reader().reset(payload));
    }

    /** A length that needs a two-byte vint, so the length prefix is not assumed to be one byte. */
    public void testLongValueUsesMultiByteVInt() {
        BytesRef value = new BytesRef(randomAlphaOfLength(300));
        BytesRef payload = StringBinaryPayload.encode(new BytesRef[] { value }, 1, new BytesRefBuilder());
        StringBinaryPayload.Reader reader = new StringBinaryPayload.Reader();
        assertEquals(1, reader.reset(payload));
        assertEquals(value, reader.next());
    }

    public void testRoundTrip() {
        int count = between(0, 20);
        BytesRef[] values = new BytesRef[count];
        for (int i = 0; i < count; i++) {
            values[i] = new BytesRef(randomBoolean() ? "" : randomRealisticUnicodeOfCodepointLength(between(1, 50)));
        }
        BytesRef payload = StringBinaryPayload.encode(values, count, new BytesRefBuilder());

        StringBinaryPayload.Reader reader = new StringBinaryPayload.Reader();
        assertEquals(count, reader.reset(payload));
        for (int i = 0; i < count; i++) {
            assertEquals("value " + i, values[i], reader.next());
        }
    }

    /** Reading past the value count would run off the end of the payload, so it trips an assertion instead. */
    public void testReadingPastCountTrips() {
        BytesRef[] values = { new BytesRef("a"), new BytesRef("b") };
        BytesRef payload = StringBinaryPayload.encode(values, 2, new BytesRefBuilder());

        StringBinaryPayload.Reader reader = new StringBinaryPayload.Reader();
        assertEquals(2, reader.reset(payload));
        assertEquals(new BytesRef("a"), reader.next());
        assertEquals(new BytesRef("b"), reader.next());
        AssertionError error = expectThrows(AssertionError.class, reader::next);
        assertThat(error.getMessage(), containsString("payload holding 2 value(s)"));
    }

    /** The reader is reused across documents, so the count has to be re-armed by each reset. */
    public void testResetRearmsTheCount() {
        StringBinaryPayload.Reader reader = new StringBinaryPayload.Reader();
        BytesRefBuilder builder = new BytesRefBuilder();

        assertEquals(2, reader.reset(StringBinaryPayload.encode(new BytesRef[] { new BytesRef("a"), new BytesRef("b") }, 2, builder)));
        reader.next();

        // A fresh payload mid-iteration: the second value of the previous one must not stay available.
        assertEquals(1, reader.reset(StringBinaryPayload.encode(new BytesRef[] { new BytesRef("c") }, 1, builder)));
        assertEquals(new BytesRef("c"), reader.next());
        expectThrows(AssertionError.class, reader::next);
    }

    /** An empty payload hands out nothing at all. */
    public void testEmptyPayloadYieldsNoValues() {
        StringBinaryPayload.Reader reader = new StringBinaryPayload.Reader();
        assertEquals(0, reader.reset(StringBinaryPayload.encode(new BytesRef[0], 0, new BytesRefBuilder())));
        expectThrows(AssertionError.class, reader::next);
    }

    /** The builder is reused across documents on the hot path, so a second encode must not see the first's bytes. */
    public void testBuilderIsReusable() {
        BytesRefBuilder builder = new BytesRefBuilder();
        BytesRef first = StringBinaryPayload.encode(new BytesRef[] { new BytesRef("aaaaaaaaaa") }, 1, builder);
        assertEquals(new BytesRef("aaaaaaaaaa"), readSingle(first));

        BytesRef second = StringBinaryPayload.encode(new BytesRef[] { new BytesRef("b") }, 1, builder);
        assertEquals(new BytesRef("b"), readSingle(second));
        assertEquals(3, second.length);
    }

    /** Encoding must respect a value's offset into its backing array, not assume it starts at zero. */
    public void testHonoursValueOffset() {
        byte[] backing = new byte[] { 'x', 'y', 'a', 'b', 'z' };
        BytesRef value = new BytesRef(backing, 2, 2); // "ab"
        BytesRef payload = StringBinaryPayload.encode(new BytesRef[] { value }, 1, new BytesRefBuilder());
        assertArrayEquals(new byte[] { 1, 2, 'a', 'b' }, toArray(payload));
    }

    private static BytesRef readSingle(BytesRef payload) {
        StringBinaryPayload.Reader reader = new StringBinaryPayload.Reader();
        assertEquals(1, reader.reset(payload));
        return reader.next();
    }

    private static byte[] toArray(BytesRef ref) {
        byte[] bytes = new byte[ref.length];
        System.arraycopy(ref.bytes, ref.offset, bytes, 0, ref.length);
        return bytes;
    }
}
