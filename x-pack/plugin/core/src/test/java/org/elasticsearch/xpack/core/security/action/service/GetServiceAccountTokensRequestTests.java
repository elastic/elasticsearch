/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class GetServiceAccountTokensRequestTests extends AbstractWireSerializingTestCase<GetServiceAccountTokensRequest> {

    @Override
    protected Writeable.Reader<GetServiceAccountTokensRequest> instanceReader() {
        return GetServiceAccountTokensRequest::new;
    }

    @Override
    protected GetServiceAccountTokensRequest createTestInstance() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        return new GetServiceAccountTokensRequest(namespace, serviceName);
    }

    @Override
    protected GetServiceAccountTokensRequest mutateInstance(GetServiceAccountTokensRequest instance) throws IOException {
        switch (randomIntBetween(0, 2)) {
            case 0:
                return new GetServiceAccountTokensRequest(
                    randomValueOtherThan(instance.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)), instance.getServiceName());
            case 1:
                return new GetServiceAccountTokensRequest(
                    instance.getNamespace(), randomValueOtherThan(instance.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8)));
            default:
                return new GetServiceAccountTokensRequest(
                    randomValueOtherThan(instance.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)),
                    randomValueOtherThan(instance.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8)));
        }
    }

    public void testValidate() {
        assertNull(createTestInstance().validate());

        final GetServiceAccountTokensRequest request1 =
            new GetServiceAccountTokensRequest(randomFrom("", null), randomAlphaOfLengthBetween(3, 8));
        final ActionRequestValidationException e1 = request1.validate();
        assertThat(e1.getMessage(), containsString("service account namespace is required"));

        final GetServiceAccountTokensRequest request2 =
            new GetServiceAccountTokensRequest(randomAlphaOfLengthBetween(3, 8), randomFrom("", null));
        final ActionRequestValidationException e2 = request2.validate();
        assertThat(e2.getMessage(), containsString("service account service-name is required"));
    }
}
