/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.xpack.core.async.GetAsyncResultRequestTests.randomSearchId;

public class EqlStatusResponseTests extends AbstractWireSerializingTestCase<EqlStatusResponse> {

    @Override
    protected EqlStatusResponse createTestInstance() {
        String id = randomSearchId();
        boolean isRunning = randomBoolean();
        boolean isPartial = isRunning ? randomBoolean() : false;
        long randomDate = (new Date(randomLongBetween(0, 3000000000000L))).getTime();
        Long startTimeMillis = randomBoolean() ? null : randomDate;
        long expirationTimeMillis = startTimeMillis == null ? randomDate : startTimeMillis + 3600000L;
        RestStatus completionStatus = isRunning ? null : randomBoolean() ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        return new EqlStatusResponse(id, isRunning, isPartial, startTimeMillis, expirationTimeMillis, completionStatus);
    }

    @Override
    protected Writeable.Reader<EqlStatusResponse> instanceReader() {
        return EqlStatusResponse::new;
    }

    @Override
    protected EqlStatusResponse mutateInstance(EqlStatusResponse instance) {
        // return a response with the opposite running status
        boolean isRunning = instance.isRunning() == false;
        boolean isPartial = isRunning ? randomBoolean() : false;
        RestStatus completionStatus = isRunning ? null : randomBoolean() ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        return new EqlStatusResponse(
            instance.getId(),
            isRunning,
            isPartial,
            instance.getStartTime(),
            instance.getExpirationTime(),
            completionStatus
        );
    }

    public void testToXContent() throws IOException {
        EqlStatusResponse response = createTestInstance();
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            String expectedJson = "{\n" +
                "  \"id\" : \"" + response.getId() + "\",\n" +
                "  \"is_running\" : " + response.isRunning() + ",\n" +
                "  \"is_partial\" : " + response.isPartial() + ",\n";

            if (response.getStartTime() != null) {
                expectedJson = expectedJson +
                    "  \"start_time_in_millis\" : " + response.getStartTime() + ",\n";
            }
            expectedJson = expectedJson +
                "  \"expiration_time_in_millis\" : " + response.getExpirationTime();

            if (response.getCompletionStatus() == null) {
                expectedJson = expectedJson + "\n" +
                    "}";
            } else {
                expectedJson = expectedJson + ",\n" +
                    "  \"completion_status\" : " + response.getCompletionStatus().getStatus() + "\n" +
                    "}";
            }
            builder.prettyPrint();
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(expectedJson, Strings.toString(builder));
        }
    }
}
