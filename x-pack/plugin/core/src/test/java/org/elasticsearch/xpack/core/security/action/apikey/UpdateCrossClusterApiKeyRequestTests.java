/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.notNullValue;

public class UpdateCrossClusterApiKeyRequestTests extends ESTestCase {

    public void testNotEmptyUpdateValidation() {
        final var request = new UpdateCrossClusterApiKeyRequest(randomAlphaOfLength(10), null, null, null);
        final ActionRequestValidationException ve = request.validate();
        assertThat(ve, notNullValue());
        assertThat(ve.validationErrors(), contains("must update either [access] or [metadata] for cross-cluster API keys"));
    }

    public void testMetadataKeyValidation() {
        final var reservedKey = "_" + randomAlphaOfLengthBetween(0, 10);
        final var metadataValue = randomAlphaOfLengthBetween(1, 10);
        final var request = new UpdateCrossClusterApiKeyRequest(randomAlphaOfLength(10), null, Map.of(reservedKey, metadataValue), null);
        final ActionRequestValidationException ve = request.validate();
        assertThat(ve, notNullValue());
        assertThat(ve.validationErrors(), contains("API key metadata keys may not start with [_]"));
    }
}
