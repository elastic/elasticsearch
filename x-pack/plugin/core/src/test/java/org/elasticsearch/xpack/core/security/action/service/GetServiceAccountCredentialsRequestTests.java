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

public class GetServiceAccountCredentialsRequestTests extends AbstractWireSerializingTestCase<GetServiceAccountCredentialsRequest> {

    @Override
    protected Writeable.Reader<GetServiceAccountCredentialsRequest> instanceReader() {
        return GetServiceAccountCredentialsRequest::new;
    }

    @Override
    protected GetServiceAccountCredentialsRequest createTestInstance() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        return new GetServiceAccountCredentialsRequest(namespace, serviceName);
    }

    @Override
    protected GetServiceAccountCredentialsRequest mutateInstance(GetServiceAccountCredentialsRequest instance) throws IOException {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new GetServiceAccountCredentialsRequest(
                randomValueOtherThan(instance.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)),
                instance.getServiceName()
            );
            case 1 -> new GetServiceAccountCredentialsRequest(
                instance.getNamespace(),
                randomValueOtherThan(instance.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8))
            );
            default -> new GetServiceAccountCredentialsRequest(
                randomValueOtherThan(instance.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)),
                randomValueOtherThan(instance.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8))
            );
        };
    }

    public void testValidate() {
        assertNull(createTestInstance().validate());

        final GetServiceAccountCredentialsRequest request1 = new GetServiceAccountCredentialsRequest(
            randomFrom("", null),
            randomAlphaOfLengthBetween(3, 8)
        );
        final ActionRequestValidationException e1 = request1.validate();
        assertThat(e1.getMessage(), containsString("service account namespace is required"));

        final GetServiceAccountCredentialsRequest request2 = new GetServiceAccountCredentialsRequest(
            randomAlphaOfLengthBetween(3, 8),
            randomFrom("", null)
        );
        final ActionRequestValidationException e2 = request2.validate();
        assertThat(e2.getMessage(), containsString("service account service-name is required"));
    }
}
