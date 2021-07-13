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

public class DeleteServiceAccountTokenRequestTests extends ESTestCase {

    public void testNewInstance() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String tokenName = randomAlphaOfLengthBetween(3, 8);

        final DeleteServiceAccountTokenRequest request1 = new DeleteServiceAccountTokenRequest(namespace, serviceName, tokenName);
        assertThat(request1.getNamespace(), equalTo(namespace));
        assertThat(request1.getServiceName(), equalTo(serviceName));
        assertThat(request1.getTokenName(), equalTo(tokenName));
        assertNull(request1.getRefreshPolicy());

        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final DeleteServiceAccountTokenRequest request2 =
            new DeleteServiceAccountTokenRequest(namespace, serviceName, tokenName, refreshPolicy);
        assertThat(request2.getNamespace(), equalTo(namespace));
        assertThat(request2.getServiceName(), equalTo(serviceName));
        assertThat(request2.getTokenName(), equalTo(tokenName));
        assertThat(request2.getRefreshPolicy(), equalTo(refreshPolicy));
    }

    public void testEqualsHashCode() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final RefreshPolicy refreshPolicy = randomBoolean() ? randomFrom(RefreshPolicy.values()) : null;

        final DeleteServiceAccountTokenRequest request =
            new DeleteServiceAccountTokenRequest(namespace, serviceName, tokenName, refreshPolicy);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request,
            original -> new DeleteServiceAccountTokenRequest(
                request.getNamespace(), request.getServiceName(), request.getTokenName(), request.getRefreshPolicy()),
            this::mutateInstance);
    }

    private DeleteServiceAccountTokenRequest mutateInstance(DeleteServiceAccountTokenRequest request) {
        switch (randomIntBetween(0, 3)) {
            case 0:
                return new DeleteServiceAccountTokenRequest(
                    randomValueOtherThan(request.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)),
                    request.getServiceName(),
                    request.getTokenName(),
                    request.getRefreshPolicy());
            case 1:
                return new DeleteServiceAccountTokenRequest(
                    request.getNamespace(),
                    randomValueOtherThan(request.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8)),
                    request.getTokenName(),
                    request.getRefreshPolicy());
            case 2:
                return new DeleteServiceAccountTokenRequest(
                    request.getNamespace(),
                    request.getServiceName(),
                    randomValueOtherThan(request.getTokenName(), () -> randomAlphaOfLengthBetween(3, 8)),
                    request.getRefreshPolicy());
            default:
                return new DeleteServiceAccountTokenRequest(
                    request.getNamespace(),
                    request.getServiceName(),
                    request.getTokenName(),
                    randomValueOtherThan(request.getRefreshPolicy(), () -> randomFrom(RefreshPolicy.values())));
        }
    }

}
