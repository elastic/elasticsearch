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
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.ClearRealmCacheRequest;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.DelegatePkiAuthenticationRequest;
import org.elasticsearch.client.security.DeleteRoleMappingRequest;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.GetApiKeyRequest;
import org.elasticsearch.client.security.GetRolesRequest;
import org.elasticsearch.client.security.InvalidateApiKeyRequest;
import org.elasticsearch.client.security.InvalidateTokenRequest;
import org.elasticsearch.client.security.PutPrivilegesRequest;
import org.elasticsearch.client.security.PutRoleMappingRequest;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class SecurityRequestConverters {

    private SecurityRequestConverters() {}

    static Request changePassword(ChangePasswordRequest changePasswordRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_security/user")
            .addPathPart(changePasswordRequest.getUsername())
            .addPathPartAsIs("_password")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(changePasswordRequest, REQUEST_BODY_CONTENT_TYPE));
        RequestConverters.Params params = new RequestConverters.Params();
        params.withRefreshPolicy(changePasswordRequest.getRefreshPolicy());
        request.addParameters(params.asMap());
        return request;
    }

    static Request putUser(PutUserRequest putUserRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_security/user")
            .addPathPart(putUserRequest.getUser().getUsername())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putUserRequest, REQUEST_BODY_CONTENT_TYPE));
        RequestConverters.Params params = new RequestConverters.Params();
        params.withRefreshPolicy(putUserRequest.getRefreshPolicy());
        request.addParameters(params.asMap());
        return request;
    }

    static Request deleteUser(DeleteUserRequest deleteUserRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_security", "user")
            .addPathPart(deleteUserRequest.getName())
            .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withRefreshPolicy(deleteUserRequest.getRefreshPolicy());
        request.addParameters(params.asMap());
        return request;
    }

    static Request putRoleMapping(final PutRoleMappingRequest putRoleMappingRequest) throws IOException {
        final String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_security/role_mapping")
            .addPathPart(putRoleMappingRequest.getName())
            .build();
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putRoleMappingRequest, REQUEST_BODY_CONTENT_TYPE));
        final RequestConverters.Params params = new RequestConverters.Params();
        params.withRefreshPolicy(putRoleMappingRequest.getRefreshPolicy());
        request.addParameters(params.asMap());
        return request;
    }

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

    static Request deleteRoleMapping(DeleteRoleMappingRequest deleteRoleMappingRequest) {
        final String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_security/role_mapping")
            .addPathPart(deleteRoleMappingRequest.getName())
            .build();
        final Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        final RequestConverters.Params params = new RequestConverters.Params();
        params.withRefreshPolicy(deleteRoleMappingRequest.getRefreshPolicy());
        request.addParameters(params.asMap());
        return request;
    }

    static Request deleteRole(DeleteRoleRequest deleteRoleRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_security/role")
            .addPathPart(deleteRoleRequest.getName())
            .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withRefreshPolicy(deleteRoleRequest.getRefreshPolicy());
        request.addParameters(params.asMap());
        return request;
    }

    static Request getRoles(GetRolesRequest getRolesRequest) {
        RequestConverters.EndpointBuilder builder = new RequestConverters.EndpointBuilder();
        builder.addPathPartAsIs("_security/role");
        if (getRolesRequest.getRoleNames().size() > 0) {
            builder.addPathPart(Strings.collectionToCommaDelimitedString(getRolesRequest.getRoleNames()));
        }
        return new Request(HttpGet.METHOD_NAME, builder.build());
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

    static Request putPrivileges(final PutPrivilegesRequest putPrivilegesRequest) throws IOException {
        Request request = new Request(HttpPut.METHOD_NAME, "/_security/privilege");
        request.setEntity(createEntity(putPrivilegesRequest, REQUEST_BODY_CONTENT_TYPE));
        RequestConverters.Params params = new RequestConverters.Params();
        params.withRefreshPolicy(putPrivilegesRequest.getRefreshPolicy());
        request.addParameters(params.asMap());
        return request;
    }

    static Request putRole(final PutRoleRequest putRoleRequest) throws IOException {
        final String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_security/role")
            .addPathPart(putRoleRequest.getRole().getName())
            .build();
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putRoleRequest, REQUEST_BODY_CONTENT_TYPE));
        final RequestConverters.Params params = new RequestConverters.Params();
        params.withRefreshPolicy(putRoleRequest.getRefreshPolicy());
        request.addParameters(params.asMap());
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
