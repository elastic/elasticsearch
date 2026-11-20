/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Base64;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;

/**
 * Tests of {@link StreamId}.
 */
public class StreamIdTests extends ESTestCase {

    /** A random valid id. */
    public static StreamId randomStreamId() {
        return new StreamId(new TaskId(randomAlphaOfLengthBetween(5, 20), randomNonNegativeLong()), UUIDs.randomBase64UUID(random()));
    }

    // Creating an id

    /** An id cannot be created without a stream key or without a task. */
    public void testRequiresKeyAndTask() {
        TaskId taskId = new TaskId("node", 1);
        expectThrows(IllegalArgumentException.class, () -> new StreamId(taskId, ""));
        expectThrows(IllegalArgumentException.class, () -> new StreamId(taskId, null));
        expectThrows(IllegalArgumentException.class, () -> new StreamId(TaskId.EMPTY_TASK_ID, "key"));
        expectThrows(NullPointerException.class, () -> new StreamId(null, "key"));
    }

    /** A task id parsed from a string like {@code :1} is set but names no node. It is rejected, also when a client sends it. */
    public void testNodeIdOfTheTaskMustNotBeEmpty() throws IOException {
        expectThrows(
            IllegalArgumentException.class,
            equalTo("the node id of the task must not be empty"),
            () -> new StreamId(new TaskId(":1"), "key")
        );

        assertInvalid(encode(out -> {
            out.writeString(":1");
            out.writeString("key");
        }));
    }

    /** The task id is written as {@code node:number}, so a node id with a colon could not be read back. */
    public void testNodeIdOfTheTaskMustNotContainColon() {
        expectThrows(
            IllegalArgumentException.class,
            equalTo("the node id of the task must not contain [:]"),
            () -> new StreamId(new TaskId("a:b", 1), "key")
        );
    }

    // Equality

    /** Two ids are equal when their task and their stream key are equal. */
    public void testEqualsAndHashcode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            randomStreamId(),
            instance -> new StreamId(instance.taskId(), instance.streamKey()),
            StreamIdTests::mutate
        );
    }

    /** Task numbers start again when a node restarts, so the key must tell two streams with the same task apart. */
    public void testSameTaskDifferentKey() {
        TaskId taskId = new TaskId("node", 7);
        StreamId one = new StreamId(taskId, UUIDs.randomBase64UUID(random()));
        StreamId other = new StreamId(taskId, UUIDs.randomBase64UUID(random()));
        assertThat(one, not(equalTo(other)));
        assertThat(one.encoded(), not(equalTo(other.encoded())));
    }

    // The encoded form

    /** Decoding the encoded form gives back the same id, with the same parts and the same encoded form. */
    public void testEncodeAndDecode() {
        for (int i = 0; i < 10; i++) {
            StreamId instance = randomStreamId();
            StreamId same = StreamId.decode(instance.encoded());
            assertThat(same, equalTo(instance));
            assertThat(same.streamKey(), equalTo(instance.streamKey()));
            assertThat(same.taskId(), equalTo(instance.taskId()));
            assertThat(same.encoded(), equalTo(instance.encoded()));
        }
    }

    /** The id is a path parameter of the REST API, so it must need no escaping in a URL. */
    public void testEncodedIsUrlSafe() {
        for (int i = 0; i < 10; i++) {
            assertThat(randomStreamId().encoded(), matchesRegex("[A-Za-z0-9_-]+=*"));
        }
        // random ids look the same in both Base64 alphabets, this one does not: standard Base64 writes it with [/]
        StreamId id = new StreamId(new TaskId("?node", 1), "key");
        assertThat(Base64.getEncoder().encodeToString(Base64.getUrlDecoder().decode(id.encoded())), containsString("/"));
        assertThat(id.encoded(), matchesRegex("[A-Za-z0-9_-]+=*"));
        assertRoundTrip(id);
    }

    /** The layout is the reserved byte, the task id, then the stream key. The right parts are accepted, whatever wrote them. */
    public void testLayout() throws IOException {
        TaskId taskId = new TaskId("node", 1);
        String encoded = encode(out -> {
            out.writeString(taskId.toString());
            out.writeString("key");
        });
        assertThat(StreamId.decode(encoded), equalTo(new StreamId(taskId, "key")));
        assertThat(new StreamId(taskId, "key").encoded(), equalTo(encoded));
    }

    /** The first byte is reserved and unset: an id is written with {@code 0} there, and no other value is accepted. */
    public void testReservedByte() throws IOException {
        StreamId id = randomStreamId();
        assertThat(Base64.getUrlDecoder().decode(id.encoded())[0], equalTo(StreamId.RESERVED));
        assertThat(StreamId.RESERVED, equalTo((byte) 0));

        byte set = randomValueOtherThan(StreamId.RESERVED, ESTestCase::randomByte);
        assertInvalid(encode(set, out -> {
            out.writeString(id.taskId().toString());
            out.writeString(id.streamKey());
        }));
        // the layout without the reserved byte is not accepted either
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(id.taskId().toString());
            out.writeString(id.streamKey());
            assertInvalid(Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes())));
        }
    }

    // Decoding what is not an id

    /** Strings that are not the Base64 of anything like an id. The cause says what was wrong with the string. */
    public void testDecodeInvalidString() {
        for (String invalid : new String[] { "", "wrong", "*", "_all", "a,b", "not base64 !", randomStreamId().encoded() + "?pretty" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StreamId.decode(invalid));
            assertThat(e.getMessage(), equalTo("invalid id: [" + invalid + "]"));
            assertThat(e.getCause(), instanceOf(Exception.class));
        }
        expectThrows(IllegalArgumentException.class, equalTo("invalid id: [null]"), () -> StreamId.decode(null));
    }

    /** Well-formed Base64 with the reserved byte, but the parts after it are not a task id and a stream key. */
    public void testDecodeInvalidParts() throws IOException {
        String taskId = new TaskId("node", 1).toString();
        // a missing part
        assertInvalid(encode(out -> out.writeString(taskId)));
        // trailing bytes
        assertInvalid(encode(out -> {
            out.writeString(taskId);
            out.writeString(randomAlphaOfLength(10));
            out.writeString("more");
        }));
        // a task id that cannot be parsed
        assertInvalid(encode(out -> {
            out.writeString("node:1:2");
            out.writeString(randomAlphaOfLength(10));
        }));
        // no task
        assertInvalid(encode(out -> {
            out.writeString(TaskId.EMPTY_TASK_ID.toString());
            out.writeString(randomAlphaOfLength(10));
        }));
        // no key
        assertInvalid(encode(out -> {
            out.writeString(taskId);
            out.writeString("");
        }));
        // the parts in the other order
        assertInvalid(encode(out -> {
            out.writeString(UUIDs.randomBase64UUID(random()));
            out.writeString(taskId);
        }));
    }

    /** A stream has one id: the same parts written with other padding are not accepted, because the storage knows one string. */
    public void testDecodeRejectsOtherEncodingsOfTheSameId() {
        StreamId random = randomStreamId();
        // one more byte in the key adds the padding: Base64 pads when the number of bytes is not a multiple of three
        StreamId id = random.encoded().endsWith("=") ? random : new StreamId(random.taskId(), random.streamKey() + "x");
        assertThat(id.encoded(), endsWith("="));
        String withoutPadding = id.encoded().replace("=", "");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StreamId.decode(withoutPadding));
        assertThat(e.getMessage(), equalTo("invalid id: [" + withoutPadding + "]"));
        // the string decodes, but it is not the one form of the id
        assertThat(e.getCause().getMessage(), equalTo("not the one encoded form of the id"));
    }

    // Helpers

    /** An id that differs in the stream key or in the task. */
    private static StreamId mutate(StreamId id) {
        return switch (between(0, 1)) {
            case 0 -> new StreamId(id.taskId(), randomAlphaOfLength(id.streamKey().length() + 1));
            case 1 -> new StreamId(
                new TaskId(randomAlphaOfLength(id.taskId().getNodeId().length() + 1), randomNonNegativeLong()),
                id.streamKey()
            );
            default -> throw new AssertionError();
        };
    }

    /** Asserts that decoding the encoded form gives back the same id. */
    private static void assertRoundTrip(StreamId id) {
        StreamId same = StreamId.decode(id.encoded());
        assertThat(same, equalTo(id));
        assertThat(same.encoded(), equalTo(id.encoded()));
    }

    /** Asserts that the string is rejected with the message a client sees. */
    private static void assertInvalid(String encoded) {
        expectThrows(IllegalArgumentException.class, equalTo("invalid id: [" + encoded + "]"), () -> StreamId.decode(encoded));
    }

    /** Writes the parts of an id that follow the reserved byte. */
    private interface Parts {
        void write(BytesStreamOutput out) throws IOException;
    }

    /** Encodes the parts after a reserved byte that is unset. */
    private static String encode(Parts parts) throws IOException {
        return encode(StreamId.RESERVED, parts);
    }

    /** Encodes the parts after the given reserved byte. */
    private static String encode(byte reserved, Parts parts) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeByte(reserved);
            parts.write(out);
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        }
    }
}
