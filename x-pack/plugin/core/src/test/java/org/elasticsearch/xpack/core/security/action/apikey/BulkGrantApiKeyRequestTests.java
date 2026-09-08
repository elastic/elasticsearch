/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class BulkGrantApiKeyRequestTests extends ESTestCase {

    public void testEmptyApiKeysNotValid() {
        final var request = new BulkGrantApiKeyRequest();
        request.getGrant().setType("password");
        request.getGrant().setUsername("user");
        request.getGrant().setPassword(new SecureString("password".toCharArray()));
        final ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().get(0), containsString("[api_keys] must not be empty"));
    }

    public void testValidRequest() {
        final var request = new BulkGrantApiKeyRequest();
        request.getGrant().setType("password");
        request.getGrant().setUsername("user");
        request.getGrant().setPassword(new SecureString("password".toCharArray()));
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("key-1", List.of(), null);
        createApiKeyRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        request.setApiKeyRequests(List.of(createApiKeyRequest));
        assertThat(request.validate(), nullValue());
        assertThat(request.getApiKeyRequests().size(), equalTo(1));
        assertThat(request.getApiKeyRequests().get(0).getName(), equalTo("key-1"));
    }

    public void testSetRefreshPolicyAppliesToAllKeys() {
        final var request = new BulkGrantApiKeyRequest();
        final CreateApiKeyRequest first = new CreateApiKeyRequest("key-1", List.of(), null);
        final CreateApiKeyRequest second = new CreateApiKeyRequest("key-2", List.of(), null);
        request.setApiKeyRequests(List.of(first, second));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertThat(first.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.IMMEDIATE));
        assertThat(second.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.IMMEDIATE));
        assertThat(request.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.IMMEDIATE));
    }
}
