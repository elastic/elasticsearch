/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Date;
import static org.elasticsearch.xpack.core.async.GetAsyncResultRequestTests.randomSearchId;

public class AsyncStatusResponseTests extends AbstractWireSerializingTestCase<AsyncStatusResponse> {

    @Override
    protected AsyncStatusResponse createTestInstance() {
        String id = randomSearchId();
        boolean isRunning = randomBoolean();
        boolean isPartial = isRunning ? randomBoolean() : false;
        long startTimeMillis = (new Date(randomLongBetween(0, 3000000000000L))).getTime();
        long expirationTimeMillis = startTimeMillis + 3600000L;
        int totalShards = randomIntBetween(10, 150);
        int successfulShards = randomIntBetween(0, totalShards - 5);
        int skippedShards = randomIntBetween(0, 5);
        int failedShards = totalShards - successfulShards - skippedShards;
        RestStatus completionStatus = isRunning ? null : randomBoolean() ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        return new AsyncStatusResponse(
            id,
            isRunning,
            isPartial,
            startTimeMillis,
            expirationTimeMillis,
            totalShards,
            successfulShards,
            skippedShards,
            failedShards,
            completionStatus
        );
    }

    @Override
    protected Writeable.Reader<AsyncStatusResponse> instanceReader() {
        return AsyncStatusResponse::new;
    }

    @Override
    protected AsyncStatusResponse mutateInstance(AsyncStatusResponse instance) {
        // return a response with the opposite running status
        boolean isRunning = instance.isRunning() == false;
        boolean isPartial = isRunning ? randomBoolean() : false;
        RestStatus completionStatus = isRunning ? null : randomBoolean() ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        return new AsyncStatusResponse(
            instance.getId(),
            isRunning,
            isPartial,
            instance.getStartTime(),
            instance.getExpirationTime(),
            instance.getTotalShards(),
            instance.getSuccessfulShards(),
            instance.getSkippedShards(),
            instance.getFailedShards(),
            completionStatus
        );
    }

    public void testToXContent() throws IOException {
        AsyncStatusResponse response = createTestInstance();
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            String expectedJson = "{\n" +
                "  \"id\" : \"" + response.getId() + "\",\n" +
                "  \"is_running\" : " + response.isRunning() + ",\n" +
                "  \"is_partial\" : " + response.isPartial() + ",\n" +
                "  \"start_time_in_millis\" : " + response.getStartTime() + ",\n" +
                "  \"expiration_time_in_millis\" : " + response.getExpirationTime() + ",\n" +
                "  \"_shards\" : {\n" +
                "    \"total\" : " + response.getTotalShards() + ",\n" +
                "    \"successful\" : " + response.getSuccessfulShards() + ",\n" +
                "    \"skipped\" : " + response.getSkippedShards() + ",\n" +
                "    \"failed\" : " + response.getFailedShards() + "\n";
            if (response.getCompletionStatus() == null) {
                expectedJson = expectedJson +
                    "  }\n" +
                    "}";
            } else {
                expectedJson = expectedJson +
                    "  },\n" +
                    "  \"completion_status\" : " + response.getCompletionStatus().getStatus() + "\n" +
                    "}";
            }
            builder.prettyPrint();
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(expectedJson, Strings.toString(builder));
        }
    }
}
