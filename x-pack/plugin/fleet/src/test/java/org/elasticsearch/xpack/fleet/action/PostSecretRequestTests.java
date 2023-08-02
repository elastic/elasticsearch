/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PostSecretRequestTests extends AbstractWireSerializingTestCase<PostSecretRequest> {

    @Override
    protected Writeable.Reader<PostSecretRequest> instanceReader() {
        return PostSecretRequest::new;
    }

    @Override
    protected PostSecretRequest createTestInstance() {
        return new PostSecretRequest(randomAlphaOfLengthBetween(10, 100), randomFrom(XContentType.values()));
    }

    @Override
    protected PostSecretRequest mutateInstance(PostSecretRequest instance) {
        return new PostSecretRequest(instance.source() + randomAlphaOfLength(1), instance.xContentType());
    }

    public void testValidateRequest() {
        PostSecretRequest req = new PostSecretRequest("{\"value\": \"secret\"}", XContentType.fromFormat("application/json"));
        ActionRequestValidationException e = req.validate();
        assertNull(e);
    }

    public void testValidateRequestWithoutValue() {
        PostSecretRequest req = new PostSecretRequest("{\"something\": \"else\"}", XContentType.fromFormat("application/json"));
        ActionRequestValidationException e = req.validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("value is missing"));
    }

    public void testValidateRequestWithEmptyContent() {
        PostSecretRequest req = new PostSecretRequest("{}", XContentType.fromFormat("application/json"));
        ActionRequestValidationException e = req.validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("value is missing"));
    }
}
