/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TaskOperationFailureTests extends AbstractXContentTestCase<TaskOperationFailure> {

    @Override
    protected TaskOperationFailure createTestInstance() {
        return new TaskOperationFailure(randomAlphaOfLength(5), randomNonNegativeLong(), new IllegalStateException("message"));
    }

    @Override
    protected TaskOperationFailure doParseInstance(XContentParser parser) throws IOException {
        return TaskOperationFailure.fromXContent(parser);
    }

    @Override
    protected void assertEqualInstances(TaskOperationFailure expectedInstance, TaskOperationFailure newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.getNodeId(), equalTo(expectedInstance.getNodeId()));
        assertThat(newInstance.getTaskId(), equalTo(expectedInstance.getTaskId()));
        assertThat(newInstance.getStatus(), equalTo(expectedInstance.getStatus()));
        // XContent loses the original exception and wraps it as a message in Elasticsearch exception
        assertThat(newInstance.getCause().getMessage(), equalTo("Elasticsearch exception [type=illegal_state_exception, reason=message]"));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }
}
