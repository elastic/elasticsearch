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

public class CreateServiceAccountTokenRequestTests extends ESTestCase {

    public void testNewInstance() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);

        final CreateServiceAccountTokenRequest request1 = new CreateServiceAccountTokenRequest(namespace, serviceName);
        assertThat(request1.getNamespace(), equalTo(namespace));
        assertThat(request1.getServiceName(), equalTo(serviceName));
        assertNull(request1.getTokenName());
        assertNull(request1.getRefreshPolicy());

        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final CreateServiceAccountTokenRequest request2 = new CreateServiceAccountTokenRequest(namespace, serviceName, tokenName);
        assertThat(request2.getNamespace(), equalTo(namespace));
        assertThat(request2.getServiceName(), equalTo(serviceName));
        assertThat(request2.getTokenName(), equalTo(tokenName));
        assertNull(request2.getRefreshPolicy());

        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final CreateServiceAccountTokenRequest request3 =
            new CreateServiceAccountTokenRequest(namespace, serviceName, tokenName, refreshPolicy);
        assertThat(request3.getNamespace(), equalTo(namespace));
        assertThat(request3.getServiceName(), equalTo(serviceName));
        assertThat(request3.getTokenName(), equalTo(tokenName));
        assertThat(request3.getRefreshPolicy(), equalTo(refreshPolicy));
    }

    public void testEqualsHashCode() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String service = randomAlphaOfLengthBetween(3, 8);
        final String tokenName = randomBoolean() ? randomAlphaOfLengthBetween(3, 8) : null;
        final RefreshPolicy refreshPolicy = randomBoolean() ? randomFrom(RefreshPolicy.values()) : null;

        final CreateServiceAccountTokenRequest request = new CreateServiceAccountTokenRequest(namespace, service, tokenName, refreshPolicy);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request,
            original -> new CreateServiceAccountTokenRequest(
                request.getNamespace(), request.getServiceName(), request.getTokenName(), request.getRefreshPolicy()),
            this::mutateInstance);
    }

    private CreateServiceAccountTokenRequest mutateInstance(CreateServiceAccountTokenRequest request) {
        switch (randomIntBetween(0, 3)) {
            case 0:
                return new CreateServiceAccountTokenRequest(
                    randomValueOtherThan(request.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)),
                    request.getServiceName(),
                    request.getTokenName(),
                    request.getRefreshPolicy());
            case 1:
                return new CreateServiceAccountTokenRequest(
                    request.getNamespace(),
                    randomValueOtherThan(request.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8)),
                    request.getTokenName(),
                    request.getRefreshPolicy());
            case 2:
                return new CreateServiceAccountTokenRequest(
                    request.getNamespace(),
                    request.getServiceName(),
                    randomValueOtherThan(request.getTokenName(), () -> randomAlphaOfLengthBetween(3, 8)),
                    request.getRefreshPolicy());
            default:
                return new CreateServiceAccountTokenRequest(
                    request.getNamespace(),
                    request.getServiceName(),
                    request.getTokenName(),
                    randomValueOtherThan(request.getRefreshPolicy(), () -> randomFrom(RefreshPolicy.values())));
        }
    }
}
