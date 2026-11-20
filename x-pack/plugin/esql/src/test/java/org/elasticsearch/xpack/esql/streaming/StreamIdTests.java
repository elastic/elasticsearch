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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * The counterpart of {@code AsyncExecutionIdTests}.
 */
public class StreamIdTests extends ESTestCase {

    public static StreamId randomStreamId() {
        return new StreamId(UUIDs.randomBase64UUID(random()), new TaskId(randomAlphaOfLengthBetween(5, 20), randomNonNegativeLong()));
    }

    private static StreamId mutate(StreamId id) {
        return switch (between(0, 1)) {
            case 0 -> new StreamId(randomAlphaOfLength(id.streamKey().length() + 1), id.taskId());
            case 1 -> new StreamId(
                id.streamKey(),
                new TaskId(randomAlphaOfLength(id.taskId().getNodeId().length() + 1), randomNonNegativeLong())
            );
            default -> throw new AssertionError();
        };
    }

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

    public void testEqualsAndHashcode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            randomStreamId(),
            instance -> new StreamId(instance.streamKey(), instance.taskId()),
            StreamIdTests::mutate
        );
    }

    /** Task numbers start again when a node restarts, so the key must tell two streams with the same task apart. */
    public void testSameTaskDifferentKey() {
        TaskId taskId = new TaskId("node", 7);
        StreamId one = new StreamId(UUIDs.randomBase64UUID(random()), taskId);
        StreamId other = new StreamId(UUIDs.randomBase64UUID(random()), taskId);
        assertThat(one, not(equalTo(other)));
        assertThat(one.encoded(), not(equalTo(other.encoded())));
    }

    public void testRequiresKeyAndTask() {
        expectThrows(IllegalArgumentException.class, () -> new StreamId("", new TaskId("node", 1)));
        expectThrows(IllegalArgumentException.class, () -> new StreamId("key", TaskId.EMPTY_TASK_ID));
    }

    public void testDecodeInvalidId() throws IOException {
        for (String invalid : new String[] { "", "wrong", "*", "_all", "a,b", "not base64 !", randomStreamId().encoded() + "?pretty" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StreamId.decode(invalid));
            assertThat(e.getMessage(), equalTo("invalid id: [" + invalid + "]"));
        }
        // a missing part
        assertInvalid(encode(out -> out.writeString(randomAlphaOfLength(10))));
        // trailing bytes
        assertInvalid(encode(out -> {
            out.writeString(randomAlphaOfLength(10));
            out.writeString(new TaskId("node", 1).toString());
            out.writeString("more");
        }));
        // a task id that cannot be parsed
        assertInvalid(encode(out -> {
            out.writeString(randomAlphaOfLength(10));
            out.writeString("node:1:2");
        }));
        // no task
        assertInvalid(encode(out -> {
            out.writeString(randomAlphaOfLength(10));
            out.writeString(TaskId.EMPTY_TASK_ID.toString());
        }));
        // no key
        assertInvalid(encode(out -> {
            out.writeString("");
            out.writeString(new TaskId("node", 1).toString());
        }));
        // the right parts in the right encoding are accepted, whatever wrote them
        assertThat(StreamId.decode(encode(out -> {
            out.writeString("key");
            out.writeString(new TaskId("node", 1).toString());
        })), equalTo(new StreamId("key", new TaskId("node", 1))));
    }

    /** The first byte is reserved and unset: an id is written with {@code 0} there, and no other value is accepted. */
    public void testReservedByte() throws IOException {
        StreamId id = randomStreamId();
        assertThat(Base64.getUrlDecoder().decode(id.encoded())[0], equalTo(StreamId.RESERVED));
        assertThat(StreamId.RESERVED, equalTo((byte) 0));

        byte set = randomValueOtherThan(StreamId.RESERVED, ESTestCase::randomByte);
        assertInvalid(encode(set, out -> {
            out.writeString(id.streamKey());
            out.writeString(id.taskId().toString());
        }));
        // the layout without the reserved byte is not accepted either
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(id.streamKey());
            out.writeString(id.taskId().toString());
            assertInvalid(Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes())));
        }
    }

    /** A stream has one id: the same parts written with other padding are not accepted, because the storage knows one string. */
    public void testDecodeRejectsOtherEncodingsOfTheSameId() {
        StreamId id;
        do {
            id = randomStreamId();
        } while (id.encoded().endsWith("=") == false);
        String withoutPadding = id.encoded().replace("=", "");
        assertInvalid(withoutPadding);
    }

    private static void assertInvalid(String encoded) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StreamId.decode(encoded));
        assertThat(e.getMessage(), equalTo("invalid id: [" + encoded + "]"));
    }

    private interface Parts {
        void write(BytesStreamOutput out) throws IOException;
    }

    /** Encodes the parts after the reserved byte, unset. */
    private static String encode(Parts parts) throws IOException {
        return encode(StreamId.RESERVED, parts);
    }

    private static String encode(byte reserved, Parts parts) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeByte(reserved);
            parts.write(out);
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        }
    }
}
