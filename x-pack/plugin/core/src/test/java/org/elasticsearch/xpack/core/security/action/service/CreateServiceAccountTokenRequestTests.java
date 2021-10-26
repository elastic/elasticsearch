/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.support.ValidationTests;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CreateServiceAccountTokenRequestTests extends ESTestCase {

    public void testReadWrite() throws IOException {
        final CreateServiceAccountTokenRequest request = new CreateServiceAccountTokenRequest(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().array()))) {
                assertThat(new CreateServiceAccountTokenRequest(in), equalTo(request));
            }
        }
    }

    public void testValidation() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String tokenName = ValidationTests.randomTokenName();

        final CreateServiceAccountTokenRequest request1 =
            new CreateServiceAccountTokenRequest(randomFrom("", null), serviceName, tokenName);
        final ActionRequestValidationException validation1 = request1.validate();
        assertThat(validation1.validationErrors(), contains(containsString("namespace is required")));

        final CreateServiceAccountTokenRequest request2 =
            new CreateServiceAccountTokenRequest(namespace, randomFrom("", null), tokenName);
        final ActionRequestValidationException validation2 = request2.validate();
        assertThat(validation2.validationErrors(), contains(containsString("service-name is required")));

        final CreateServiceAccountTokenRequest request3 =
            new CreateServiceAccountTokenRequest(namespace, serviceName, ValidationTests.randomInvalidTokenName());
        final ActionRequestValidationException validation3 = request3.validate();
        assertThat(validation3.validationErrors(), contains(containsString(Validation.INVALID_SERVICE_ACCOUNT_TOKEN_NAME_MESSAGE)));
        assertThat(validation3.validationErrors(),
            contains(containsString("invalid service token name [" + request3.getTokenName() + "]")));

        final CreateServiceAccountTokenRequest request4 = new CreateServiceAccountTokenRequest(namespace, serviceName, tokenName);
        final ActionRequestValidationException validation4 = request4.validate();
        assertThat(validation4, nullValue());
    }
}
