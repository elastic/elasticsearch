/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction.Request;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class UpdateTrainedModelDeploymentRequestTests extends AbstractXContentSerializingTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.parseRequest(null, parser);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return createRandom();
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static Request createRandom() {
        Request request = new Request(randomAlphaOfLength(10));
        if (randomBoolean()) {
            request.setNumberOfAllocations(randomIntBetween(1, 512));
        }
        return request;
    }

    public void testValidate_GivenNumberOfAllocationsIsZero() {
        Request request = createRandom();
        request.setNumberOfAllocations(0);

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[number_of_allocations] must be a positive integer"));
    }

    public void testValidate_GivenNumberOfAllocationsIsNegative() {
        Request request = createRandom();
        request.setNumberOfAllocations(randomIntBetween(-100, -1));

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[number_of_allocations] must be a positive integer"));
    }
}
