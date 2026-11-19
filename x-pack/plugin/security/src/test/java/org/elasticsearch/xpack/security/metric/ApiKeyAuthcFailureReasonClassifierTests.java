/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.security.authc.ApiKeyAuthcFailureReasonClassifier;

import static org.hamcrest.Matchers.equalTo;

public class ApiKeyAuthcFailureReasonClassifierTests extends ESTestCase {

    public void testApiKeyExpiredFromUnsuccessfulResult() {
        assertThat(
            ApiKeyAuthcFailureReasonClassifier.INSTANCE.fromResult(AuthenticationResult.unsuccessful("api key is expired", null)).value(),
            equalTo("client.api_key_expired")
        );
    }

    public void testApiKeyInvalidatedFromUnsuccessfulResult() {
        assertThat(
            ApiKeyAuthcFailureReasonClassifier.INSTANCE.fromResult(AuthenticationResult.unsuccessful("api key has been invalidated", null))
                .value(),
            equalTo("client.api_key_invalidated")
        );
    }

    public void testApiKeyInvalidCredentialsFromUnsuccessfulResult() {
        assertThat(
            ApiKeyAuthcFailureReasonClassifier.INSTANCE.fromResult(AuthenticationResult.unsuccessful("invalid credentials", null)).value(),
            equalTo("client.invalid_credentials")
        );
    }
}
