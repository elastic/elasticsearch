/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The request is sent between nodes, because a delete is forwarded to the node that runs the query.
 */
public class DeleteStreamResultRequestTests extends AbstractWireSerializingTestCase<DeleteStreamResultRequest> {

    @Override
    protected Writeable.Reader<DeleteStreamResultRequest> instanceReader() {
        return DeleteStreamResultRequest::new;
    }

    @Override
    protected DeleteStreamResultRequest createTestInstance() {
        return new DeleteStreamResultRequest(StreamIdTests.randomStreamId().encoded());
    }

    @Override
    protected DeleteStreamResultRequest mutateInstance(DeleteStreamResultRequest instance) {
        return new DeleteStreamResultRequest(randomValueOtherThan(instance.id(), () -> StreamIdTests.randomStreamId().encoded()));
    }

    public void testIdIsRequired() {
        for (String missing : new String[] { null, "" }) {
            ActionRequestValidationException e = new DeleteStreamResultRequest(missing).validate();
            assertThat(e, notNullValue());
            assertThat(e.getMessage(), containsString("id is required"));
        }
    }

    /** The request does not decode the id: whether it names a stream is decided by the action, which answers not found. */
    public void testAnyNonEmptyIdIsValid() {
        assertThat(new DeleteStreamResultRequest(randomAlphaOfLengthBetween(1, 20)).validate(), nullValue());
        assertThat(createTestInstance().validate(), nullValue());
    }
}
