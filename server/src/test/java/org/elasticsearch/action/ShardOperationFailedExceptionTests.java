package org.elasticsearch.action;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ShardOperationFailedExceptionTests extends ESTestCase {

    public void testCauseCannotBeNull() {
        NullPointerException nullPointerException = expectThrows(NullPointerException.class, () -> new Failure(
            randomAlphaOfLengthBetween(3, 10), randomInt(), randomAlphaOfLengthBetween(5, 10), randomFrom(RestStatus.values()), null));
        assertEquals("cause cannot be null", nullPointerException.getMessage());
    }

    public void testStatusCannotBeNull() {
        NullPointerException nullPointerException = expectThrows(NullPointerException.class, () -> new Failure(
            randomAlphaOfLengthBetween(3, 10), randomInt(), randomAlphaOfLengthBetween(5, 10), null, new IllegalArgumentException()));
        assertEquals("status cannot be null", nullPointerException.getMessage());
    }

    public void testReasonCannotBeNull() {
        NullPointerException nullPointerException = expectThrows(NullPointerException.class, () -> new Failure(
            randomAlphaOfLengthBetween(3, 10), randomInt(), null, randomFrom(RestStatus.values()), new IllegalArgumentException()));
        assertEquals("reason cannot be null", nullPointerException.getMessage());
    }

    public void testIndexIsNullable() {
        new Failure(null, randomInt(), randomAlphaOfLengthBetween(5, 10), randomFrom(RestStatus.values()), new IllegalArgumentException());
    }

    private static class Failure extends ShardOperationFailedException {

        Failure(@Nullable String index, int shardId, String reason, RestStatus status, Throwable cause) {
           super(index, shardId, reason, status, cause);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }
}
