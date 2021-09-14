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

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class ClearServiceAccountTokenCacheRequestTests extends ESTestCase {

    public void testNewInstance() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String[] tokenNames = randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));

        final ClearServiceAccountTokenCacheRequest clearServiceAccountTokenCacheRequest =
            new ClearServiceAccountTokenCacheRequest(namespace, serviceName, tokenNames);

        assertThat(clearServiceAccountTokenCacheRequest.getNamespace(), equalTo(namespace));
        assertThat(clearServiceAccountTokenCacheRequest.getServiceName(), equalTo(serviceName));
        assertThat(clearServiceAccountTokenCacheRequest.getTokenNames(), equalTo(tokenNames));
    }

    public void testEqualsHashCode() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String[] tokenNames = randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));

        final ClearServiceAccountTokenCacheRequest request = new ClearServiceAccountTokenCacheRequest(namespace, serviceName, tokenNames);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request,
            original -> new ClearServiceAccountTokenCacheRequest(request.getNamespace(), request.getServiceName(), request.getTokenNames()),
            this::mutateInstance);
    }

    private ClearServiceAccountTokenCacheRequest mutateInstance(ClearServiceAccountTokenCacheRequest request) {
        switch (randomIntBetween(0, 2)) {
            case 0:
                return new ClearServiceAccountTokenCacheRequest(
                    randomValueOtherThan(request.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)),
                    request.getServiceName(),
                    request.getTokenNames());
            case 1:
                return new ClearServiceAccountTokenCacheRequest(
                    request.getNamespace(),
                    randomValueOtherThan(request.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8)),
                    request.getTokenNames());
            default:
                return new ClearServiceAccountTokenCacheRequest(
                    request.getNamespace(),
                    request.getServiceName(),
                    randomValueOtherThanMany(a -> Arrays.equals(a, request.getTokenNames()),
                        () -> randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8))));
        }
    }
}
