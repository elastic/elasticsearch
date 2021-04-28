/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.hamcrest.Matchers.equalTo;

public class GetServiceAccountCredentialsRequestTests extends ESTestCase {

    public void testNewInstance() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);

        final GetServiceAccountCredentialsRequest request = new GetServiceAccountCredentialsRequest(namespace, serviceName);
        assertThat(request.getNamespace(), equalTo(namespace));
        assertThat(request.getServiceName(), equalTo(serviceName));
    }

    public void testEqualsHashCode() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final GetServiceAccountCredentialsRequest request = new GetServiceAccountCredentialsRequest(namespace, serviceName);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request,
            original -> new GetServiceAccountCredentialsRequest(request.getNamespace(), request.getServiceName()),
            this::mutateInstance);
    }

    private GetServiceAccountCredentialsRequest mutateInstance(GetServiceAccountCredentialsRequest request) {
        switch (randomIntBetween(0, 1)) {
            case 0:
                return new GetServiceAccountCredentialsRequest(randomValueOtherThan(request.getNamespace(),
                    () -> randomAlphaOfLengthBetween(3, 8)), request.getServiceName());
            default:
                return new GetServiceAccountCredentialsRequest(request.getNamespace(),
                    randomValueOtherThan(request.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8)));
        }
    }
}
