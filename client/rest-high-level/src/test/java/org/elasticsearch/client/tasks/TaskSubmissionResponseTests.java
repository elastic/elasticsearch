/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.tasks;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class TaskSubmissionResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            this::createTestInstance,
            this::toXContent,
            TaskSubmissionResponse::fromXContent)
            .supportsUnknownFields(true)
            .test();
    }

    private void toXContent(TaskSubmissionResponse response, XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.startObject();
        xContentBuilder.field("task", response.getTask());
        xContentBuilder.endObject();
    }

    private TaskSubmissionResponse createTestInstance() {
        String taskId = randomAlphaOfLength(5) + ":" + randomLong();
        return new TaskSubmissionResponse(taskId);
    }
}
