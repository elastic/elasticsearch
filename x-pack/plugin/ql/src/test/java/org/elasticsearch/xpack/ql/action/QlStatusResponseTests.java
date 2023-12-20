/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
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
            Object[] args = new Object[] {
                response.getId(),
                response.isRunning(),
                response.isPartial(),
                response.getStartTime() != null ? "\"start_time_in_millis\" : " + response.getStartTime() + "," : "",
                response.getExpirationTime(),
                response.getCompletionStatus() != null ? ", \"completion_status\" : " + response.getCompletionStatus().getStatus() : "" };
            String expectedJson = Strings.format("""
                {
                  "id" : "%s",
                  "is_running" : %s,
                  "is_partial" : %s,
                  %s
                  "expiration_time_in_millis" : %s
                  %s
                }
                """, args);
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(XContentHelper.stripWhitespace(expectedJson), Strings.toString(builder));
        }
    }
}
