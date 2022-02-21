/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.security.ClearRealmCacheRequest;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.DelegatePkiAuthenticationRequest;
import org.elasticsearch.client.security.GetApiKeyRequest;
import org.elasticsearch.client.security.InvalidateApiKeyRequest;
import org.elasticsearch.client.security.InvalidateTokenRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class SecurityRequestConverters {

    private SecurityRequestConverters() {}

    static Request clearRealmCache(ClearRealmCacheRequest clearRealmCacheRequest) {
        RequestConverters.EndpointBuilder builder = new RequestConverters.EndpointBuilder().addPathPartAsIs("_security/realm");
        if (clearRealmCacheRequest.getRealms().isEmpty() == false) {
            builder.addCommaSeparatedPathParts(clearRealmCacheRequest.getRealms().toArray(Strings.EMPTY_ARRAY));
        } else {
            builder.addPathPart("_all");
        }
        final String endpoint = builder.addPathPartAsIs("_clear_cache").build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        if (clearRealmCacheRequest.getUsernames().isEmpty() == false) {
            RequestConverters.Params params = new RequestConverters.Params();
            params.putParam("usernames", Strings.collectionToCommaDelimitedString(clearRealmCacheRequest.getUsernames()));
            request.addParameters(params.asMap());
        }
        return request;
    }

    static Request createToken(CreateTokenRequest createTokenRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_security/oauth2/token");
        request.setEntity(createEntity(createTokenRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request delegatePkiAuthentication(DelegatePkiAuthenticationRequest delegatePkiAuthenticationRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_security/delegate_pki");
        request.setEntity(createEntity(delegatePkiAuthenticationRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request invalidateToken(InvalidateTokenRequest invalidateTokenRequest) throws IOException {
        Request request = new Request(HttpDelete.METHOD_NAME, "/_security/oauth2/token");
        request.setEntity(createEntity(invalidateTokenRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getApiKey(final GetApiKeyRequest getApiKeyRequest) throws IOException {
        final Request request = new Request(HttpGet.METHOD_NAME, "/_security/api_key");
        if (Strings.hasText(getApiKeyRequest.getId())) {
            request.addParameter("id", getApiKeyRequest.getId());
        }
        if (Strings.hasText(getApiKeyRequest.getName())) {
            request.addParameter("name", getApiKeyRequest.getName());
        }
        if (Strings.hasText(getApiKeyRequest.getUserName())) {
            request.addParameter("username", getApiKeyRequest.getUserName());
        }
        if (Strings.hasText(getApiKeyRequest.getRealmName())) {
            request.addParameter("realm_name", getApiKeyRequest.getRealmName());
        }
        request.addParameter("owner", Boolean.toString(getApiKeyRequest.ownedByAuthenticatedUser()));
        return request;
    }

    static Request invalidateApiKey(final InvalidateApiKeyRequest invalidateApiKeyRequest) throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "/_security/api_key");
        request.setEntity(createEntity(invalidateApiKeyRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

}
