/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ShardOperationFailedExceptionTests extends ESTestCase {

    public void testCauseCannotBeNull() {
        NullPointerException nullPointerException = expectThrows(
            NullPointerException.class,
            () -> new Failure(
                randomAlphaOfLengthBetween(3, 10),
                randomInt(),
                randomAlphaOfLengthBetween(5, 10),
                randomFrom(RestStatus.values()),
                null
            )
        );
        assertEquals("cause cannot be null", nullPointerException.getMessage());
    }

    public void testStatusCannotBeNull() {
        NullPointerException nullPointerException = expectThrows(
            NullPointerException.class,
            () -> new Failure(
                randomAlphaOfLengthBetween(3, 10),
                randomInt(),
                randomAlphaOfLengthBetween(5, 10),
                null,
                new IllegalArgumentException()
            )
        );
        assertEquals("status cannot be null", nullPointerException.getMessage());
    }

    public void testReasonCannotBeNull() {
        NullPointerException nullPointerException = expectThrows(
            NullPointerException.class,
            () -> new Failure(
                randomAlphaOfLengthBetween(3, 10),
                randomInt(),
                null,
                randomFrom(RestStatus.values()),
                new IllegalArgumentException()
            )
        );
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
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }
}
