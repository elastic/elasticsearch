/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ServiceAccountTokenTests extends ESTestCase {

    public void testNewToken() {
        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        ServiceAccountToken.newToken(accountId, ValidationTests.randomTokenName());

        final String invalidTokeName = ValidationTests.randomInvalidTokenName();
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> ServiceAccountToken.newToken(accountId, invalidTokeName)
        );
        assertThat(e1.getMessage(), containsString(Validation.INVALID_SERVICE_ACCOUNT_TOKEN_NAME_MESSAGE));
        assertThat(e1.getMessage(), containsString("invalid service token name [" + invalidTokeName + "]"));

        final NullPointerException e2 = expectThrows(
            NullPointerException.class,
            () -> ServiceAccountToken.newToken(null, ValidationTests.randomTokenName())
        );
        assertThat(e2.getMessage(), containsString("service account ID cannot be null"));
    }

    public void testServiceAccountTokenNew() {
        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final SecureString secret = new SecureString(randomAlphaOfLength(20).toCharArray());
        new ServiceAccountToken(accountId, ValidationTests.randomTokenName(), secret);

        final NullPointerException e1 = expectThrows(
            NullPointerException.class,
            () -> new ServiceAccountToken(null, ValidationTests.randomTokenName(), secret)
        );
        assertThat(e1.getMessage(), containsString("service account ID cannot be null"));

        final String invalidTokenName = ValidationTests.randomInvalidTokenName();
        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> new ServiceAccountToken(accountId, invalidTokenName, secret)
        );
        assertThat(e2.getMessage(), containsString(Validation.INVALID_SERVICE_ACCOUNT_TOKEN_NAME_MESSAGE));
        assertThat(e2.getMessage(), containsString("invalid service token name [" + invalidTokenName + "]"));

        final NullPointerException e3 = expectThrows(
            NullPointerException.class,
            () -> new ServiceAccountToken(accountId, ValidationTests.randomTokenName(), null)
        );
        assertThat(e3.getMessage(), containsString("service account token secret cannot be null"));
    }

    public void testBearerString() throws IOException {
        final ServiceAccountToken serviceAccountToken = new ServiceAccountToken(
            new ServiceAccountId("elastic", "fleet-server"),
            "token1",
            new SecureString("supersecret".toCharArray())
        );
        assertThat(serviceAccountToken.asBearerString(), equalTo("AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTpzdXBlcnNlY3JldA"));

        assertThat(
            ServiceAccountToken.fromBearerString(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTpzdXBlcnNlY3JldA".toCharArray())
            ),
            equalTo(serviceAccountToken)
        );

        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final ServiceAccountToken serviceAccountToken1 = ServiceAccountToken.newToken(accountId, ValidationTests.randomTokenName());
        assertThat(ServiceAccountToken.fromBearerString(serviceAccountToken1.asBearerString()), equalTo(serviceAccountToken1));
    }
}
