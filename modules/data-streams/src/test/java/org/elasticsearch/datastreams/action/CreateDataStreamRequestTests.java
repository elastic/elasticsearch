/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction.Request;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CreateDataStreamRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, randomAlphaOfLength(8));
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String name = instance.getName();
        long startTime = instance.getStartTime();
        switch (between(0, 1)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomAlphaOfLength(8));
            case 1 -> startTime = randomValueOtherThan(startTime, () -> randomNonNegativeLong());
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name, startTime);
    }

    public void testValidateRequest() {
        CreateDataStreamAction.Request req = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "my-data-stream"
        );
        ActionRequestValidationException e = req.validate();
        assertNull(e);
    }

    public void testValidateRequestWithoutName() {
        CreateDataStreamAction.Request req = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "");
        ActionRequestValidationException e = req.validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("name is missing"));
    }

}
