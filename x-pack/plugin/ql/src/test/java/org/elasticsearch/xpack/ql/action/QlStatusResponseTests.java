/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.async.QlStatusResponse;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.xpack.core.async.GetAsyncResultRequestTests.randomSearchId;

public class QlStatusResponseTests extends AbstractWireSerializingTestCase<QlStatusResponse> {

    @Override
    protected QlStatusResponse createTestInstance() {
        String id = randomSearchId();
        boolean isRunning = randomBoolean();
        boolean isPartial = isRunning ? randomBoolean() : false;
        long randomDate = (new Date(randomLongBetween(0, 3000000000000L))).getTime();
        Long startTimeMillis = randomBoolean() ? null : randomDate;
        long expirationTimeMillis = startTimeMillis == null ? randomDate : startTimeMillis + 3600000L;
        RestStatus completionStatus = isRunning ? null : randomBoolean() ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        return new QlStatusResponse(id, isRunning, isPartial, startTimeMillis, expirationTimeMillis, completionStatus);
    }

    @Override
    protected Writeable.Reader<QlStatusResponse> instanceReader() {
        return QlStatusResponse::new;
    }

    @Override
    protected QlStatusResponse mutateInstance(QlStatusResponse instance) {
        // return a response with the opposite running status
        boolean isRunning = instance.isRunning() == false;
        boolean isPartial = isRunning ? randomBoolean() : false;
        RestStatus completionStatus = isRunning ? null : randomBoolean() ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        return new QlStatusResponse(
            instance.getId(),
            isRunning,
            isPartial,
            instance.getStartTime(),
            instance.getExpirationTime(),
            completionStatus
        );
    }

    public void testToXContent() throws IOException {
        QlStatusResponse response = createTestInstance();
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
