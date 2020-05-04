/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.security.AuthenticateRequest;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.ClearPrivilegesCacheRequest;
import org.elasticsearch.client.security.ClearPrivilegesCacheResponse;
import org.elasticsearch.client.security.ClearRealmCacheRequest;
import org.elasticsearch.client.security.ClearRealmCacheResponse;
import org.elasticsearch.client.security.ClearRolesCacheRequest;
import org.elasticsearch.client.security.ClearRolesCacheResponse;
import org.elasticsearch.client.security.CreateApiKeyRequest;
import org.elasticsearch.client.security.CreateApiKeyResponse;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.CreateTokenResponse;
import org.elasticsearch.client.security.DelegatePkiAuthenticationRequest;
import org.elasticsearch.client.security.DelegatePkiAuthenticationResponse;
import org.elasticsearch.client.security.DeletePrivilegesRequest;
import org.elasticsearch.client.security.DeletePrivilegesResponse;
import org.elasticsearch.client.security.DeleteRoleMappingRequest;
import org.elasticsearch.client.security.DeleteRoleMappingResponse;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteRoleResponse;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.DeleteUserResponse;
import org.elasticsearch.client.security.DisableUserRequest;
import org.elasticsearch.client.security.EnableUserRequest;
import org.elasticsearch.client.security.GetApiKeyRequest;
import org.elasticsearch.client.security.GetApiKeyResponse;
import org.elasticsearch.client.security.GetBuiltinPrivilegesRequest;
import org.elasticsearch.client.security.GetBuiltinPrivilegesResponse;
import org.elasticsearch.client.security.GetPrivilegesRequest;
import org.elasticsearch.client.security.GetPrivilegesResponse;
import org.elasticsearch.client.security.GetRoleMappingsRequest;
import org.elasticsearch.client.security.GetRoleMappingsResponse;
import org.elasticsearch.client.security.GetRolesRequest;
import org.elasticsearch.client.security.GetRolesResponse;
import org.elasticsearch.client.security.GetSslCertificatesRequest;
import org.elasticsearch.client.security.GetSslCertificatesResponse;
import org.elasticsearch.client.security.GetUserPrivilegesRequest;
import org.elasticsearch.client.security.GetUserPrivilegesResponse;
import org.elasticsearch.client.security.GetUsersRequest;
import org.elasticsearch.client.security.GetUsersResponse;
import org.elasticsearch.client.security.HasPrivilegesRequest;
import org.elasticsearch.client.security.HasPrivilegesResponse;
import org.elasticsearch.client.security.InvalidateApiKeyRequest;
import org.elasticsearch.client.security.InvalidateApiKeyResponse;
import org.elasticsearch.client.security.InvalidateTokenRequest;
import org.elasticsearch.client.security.InvalidateTokenResponse;
import org.elasticsearch.client.security.PutPrivilegesRequest;
import org.elasticsearch.client.security.PutPrivilegesResponse;
import org.elasticsearch.client.security.PutRoleMappingRequest;
import org.elasticsearch.client.security.PutRoleMappingResponse;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutRoleResponse;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.PutUserResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Security APIs.
 * <p>
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api.html">Security APIs on elastic.co</a>
 */
public final class SecurityClient {

    private final RestHighLevelClient restHighLevelClient;

    SecurityClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Get a user, or list of users, in the native realm synchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-user.html">
     * the docs</a> for more information.
     * @param request the request with the user's name
     * @param options the request options (e.g., headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the get users call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetUsersResponse getUsers(GetUsersRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::getUsers, options,
            GetUsersResponse::fromXContent, emptySet());
    }

    /**
     * Get a user, or list of users, in the native realm asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-user.html">
     * the docs</a> for more information.
     * @param request the request with the user's name
     * @param options the request options (e.g., headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getUsersAsync(GetUsersRequest request, RequestOptions options, ActionListener<GetUsersResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::getUsers, options,
            GetUsersResponse::fromXContent, listener, emptySet());
    }

    /**
     * Create/update a user in the native realm synchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-users.html">
     * the docs</a> for more.
     *
     * @param request the request with the user's information
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the put user call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public PutUserResponse putUser(PutUserRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::putUser, options,
            PutUserResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously create/update a user in the native realm.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-users.html">
     * the docs</a> for more.
     *
     * @param request  the request with the user's information
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putUserAsync(PutUserRequest request, RequestOptions options, ActionListener<PutUserResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::putUser, options,
            PutUserResponse::fromXContent, listener, emptySet());
    }

    /**
     * Removes user from the native realm synchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delete-user.html">
     * the docs</a> for more.
     * @param request the request with the user to delete
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the delete user call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public DeleteUserResponse deleteUser(DeleteUserRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::deleteUser, options,
            DeleteUserResponse::fromXContent, singleton(404));
    }

    /**
     *  Asynchronously deletes a user in the native realm.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delete-user.html">
     * the docs</a> for more.
     * @param request the request with the user to delete
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteUserAsync(DeleteUserRequest request, RequestOptions options, ActionListener<DeleteUserResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::deleteUser, options,
            DeleteUserResponse::fromXContent, listener, singleton(404));
    }

    /**
     * Create/Update a role mapping.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-role-mapping.html">
     * the docs</a> for more.
     * @param request the request with the role mapping information
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the put role mapping call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public PutRoleMappingResponse putRoleMapping(final PutRoleMappingRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::putRoleMapping, options,
                PutRoleMappingResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously create/update a role mapping.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-role-mapping.html">
     * the docs</a> for more.
     * @param request the request with the role mapping information
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putRoleMappingAsync(final PutRoleMappingRequest request, final RequestOptions options,
                                           final ActionListener<PutRoleMappingResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::putRoleMapping, options,
                PutRoleMappingResponse::fromXContent, listener, emptySet());
    }

    /**
     * Synchronously get role mapping(s).
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-role-mapping.html">
     * the docs</a> for more.
     *
     * @param request {@link GetRoleMappingsRequest} with role mapping name(s).
     * If no role mapping name is provided then retrieves all role mappings.
     * @param options the request options (e.g. headers), use
     * {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the get role mapping call
     * @throws IOException in case there is a problem sending the request or
     * parsing back the response
     */
    public GetRoleMappingsResponse getRoleMappings(final GetRoleMappingsRequest request,
                                                   final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::getRoleMappings,
            options, GetRoleMappingsResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get role mapping(s).
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-role-mapping.html">
     * the docs</a> for more.
     *
     * @param request {@link GetRoleMappingsRequest} with role mapping name(s).
     * If no role mapping name is provided then retrieves all role mappings.
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getRoleMappingsAsync(final GetRoleMappingsRequest request, final RequestOptions options,
                                            final ActionListener<GetRoleMappingsResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::getRoleMappings,
                options, GetRoleMappingsResponse::fromXContent, listener, emptySet());
    }

    /**
     * Enable a native realm or built-in user synchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-enable-user.html">
     * the docs</a> for more.
     *
     * @param request the request with the user to enable
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@code true} if the request succeeded (the user is enabled)
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public boolean enableUser(EnableUserRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequest(request, SecurityRequestConverters::enableUser, options,
            RestHighLevelClient::convertExistsResponse, emptySet());
    }

    /**
     * Enable a native realm or built-in user synchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-enable-user.html">
     * the docs</a> for more.
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param request the request with the user to enable
     * @return {@code true} if the request succeeded (the user is enabled)
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #enableUser(EnableUserRequest, RequestOptions)} instead
     */
    @Deprecated
    public boolean enableUser(RequestOptions options, EnableUserRequest request) throws IOException {
        return enableUser(request, options);
    }

    /**
     * Enable a native realm or built-in user asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-enable-user.html">
     * the docs</a> for more.
     *
     * @param request  the request with the user to enable
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable enableUserAsync(EnableUserRequest request, RequestOptions options,
                                       ActionListener<Boolean> listener) {
        return restHighLevelClient.performRequestAsync(request, SecurityRequestConverters::enableUser, options,
            RestHighLevelClient::convertExistsResponse, listener, emptySet());
    }

    /**
     * Enable a native realm or built-in user asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-enable-user.html">
     * the docs</a> for more.
     *
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param request  the request with the user to enable
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #enableUserAsync(EnableUserRequest, RequestOptions, ActionListener)} instead
     * @return cancellable that may be used to cancel the request
     */
    @Deprecated
    public Cancellable enableUserAsync(RequestOptions options, EnableUserRequest request,
                                       ActionListener<Boolean> listener) {
        return enableUserAsync(request, options, listener);
    }

    /**
     * Disable a native realm or built-in user synchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-disable-user.html">
     * the docs</a> for more.
     *
     * @param request the request with the user to disable
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@code true} if the request succeeded (the user is disabled)
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public boolean disableUser(DisableUserRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequest(request, SecurityRequestConverters::disableUser, options,
            RestHighLevelClient::convertExistsResponse, emptySet());
    }

    /**
     * Disable a native realm or built-in user synchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-disable-user.html">
     * the docs</a> for more.
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param request the request with the user to disable
     * @return {@code true} if the request succeeded (the user is disabled)
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #disableUser(DisableUserRequest, RequestOptions)} instead
     */
    @Deprecated
    public boolean disableUser(RequestOptions options, DisableUserRequest request) throws IOException {
        return disableUser(request, options);
    }

    /**
     * Disable a native realm or built-in user asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-disable-user.html">
     * the docs</a> for more.
     *
     * @param request  the request with the user to disable
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable disableUserAsync(DisableUserRequest request, RequestOptions options,
                                        ActionListener<Boolean> listener) {
        return restHighLevelClient.performRequestAsync(request, SecurityRequestConverters::disableUser, options,
            RestHighLevelClient::convertExistsResponse, listener, emptySet());
    }

    /**
     * Disable a native realm or built-in user asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-disable-user.html">
     * the docs</a> for more.
     *
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param request  the request with the user to disable
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #disableUserAsync(DisableUserRequest, RequestOptions, ActionListener)} instead
     * @return cancellable that may be used to cancel the request
     */
    @Deprecated
    public Cancellable disableUserAsync(RequestOptions options, DisableUserRequest request,
                                        ActionListener<Boolean> listener) {
        return disableUserAsync(request, options, listener);
    }

    /**
     * Authenticate the current user and return all the information about the authenticated user.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-authenticate.html">
     * the docs</a> for more.
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the responsee from the authenticate user call
     */
    public AuthenticateResponse authenticate(RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(AuthenticateRequest.INSTANCE, AuthenticateRequest::getRequest, options,
                AuthenticateResponse::fromXContent, emptySet());
    }

    /**
     * Authenticate the current user asynchronously and return all the information about the authenticated user.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-authenticate.html">
     * the docs</a> for more.
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable authenticateAsync(RequestOptions options, ActionListener<AuthenticateResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(AuthenticateRequest.INSTANCE, AuthenticateRequest::getRequest, options,
                AuthenticateResponse::fromXContent, listener, emptySet());
    }

    /**
     * Determine whether the current user has a specified list of privileges
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-has-privileges.html">
     * the docs</a> for more.
     *
     * @param request the request with the privileges to check
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the has privileges call
     */
    public HasPrivilegesResponse hasPrivileges(HasPrivilegesRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::hasPrivileges, options,
            HasPrivilegesResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously determine whether the current user has a specified list of privileges
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-has-privileges.html">
     * the docs</a> for more.
     * @param request the request with the privileges to check
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable hasPrivilegesAsync(HasPrivilegesRequest request, RequestOptions options,
                                          ActionListener<HasPrivilegesResponse> listener) {
         return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::hasPrivileges, options,
            HasPrivilegesResponse::fromXContent, listener, emptySet());
    }

    /**
     * Retrieve the set of effective privileges held by the current user.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public GetUserPrivilegesResponse getUserPrivileges(RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(GetUserPrivilegesRequest.INSTANCE, GetUserPrivilegesRequest::getRequest,
            options, GetUserPrivilegesResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously retrieve the set of effective privileges held by the current user.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getUserPrivilegesAsync(RequestOptions options, ActionListener<GetUserPrivilegesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            GetUserPrivilegesRequest.INSTANCE, GetUserPrivilegesRequest::getRequest,
            options, GetUserPrivilegesResponse::fromXContent, listener, emptySet());
    }

    /**
     * Clears the cache in one or more realms.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-clear-cache.html">
     * the docs</a> for more.
     *
     * @param request the request with the realm names and usernames to clear the cache for
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the clear realm cache call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClearRealmCacheResponse clearRealmCache(ClearRealmCacheRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::clearRealmCache, options,
            ClearRealmCacheResponse::fromXContent, emptySet());
    }

    /**
     * Clears the cache in one or more realms asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-clear-cache.html">
     * the docs</a> for more.
     *
     * @param request  the request with the realm names and usernames to clear the cache for
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable clearRealmCacheAsync(ClearRealmCacheRequest request, RequestOptions options,
                                            ActionListener<ClearRealmCacheResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::clearRealmCache, options,
            ClearRealmCacheResponse::fromXContent, listener, emptySet());
    }

    /**
     * Clears the roles cache for a set of roles.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-clear-role-cache.html">
     * the docs</a> for more.
     *
     * @param request the request with the roles for which the cache should be cleared.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the clear roles cache call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClearRolesCacheResponse clearRolesCache(ClearRolesCacheRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::clearRolesCache, options,
            ClearRolesCacheResponse::fromXContent, emptySet());
    }

    /**
     * Clears the roles cache for a set of roles asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-clear-role-cache.html">
     * the docs</a> for more.
     *
     * @param request  the request with the roles for which the cache should be cleared.
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable clearRolesCacheAsync(ClearRolesCacheRequest request, RequestOptions options,
                                            ActionListener<ClearRolesCacheResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::clearRolesCache, options,
            ClearRolesCacheResponse::fromXContent, listener, emptySet());
    }

    /**
     * Clears the privileges cache for a set of privileges.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-clear-privilege-cache.html">
     * the docs</a> for more.
     *
     * @param request the request with the privileges for which the cache should be cleared.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the clear privileges cache call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClearPrivilegesCacheResponse clearPrivilegesCache(ClearPrivilegesCacheRequest request,
                                                             RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::clearPrivilegesCache, options,
            ClearPrivilegesCacheResponse::fromXContent, emptySet());
    }

    /**
     * Clears the privileges cache for a set of privileges asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-clear-privilege-cache.html">
     * the docs</a> for more.
     *
     * @param request  the request with the privileges for which the cache should be cleared.
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable clearPrivilegesCacheAsync(ClearPrivilegesCacheRequest request, RequestOptions options,
                                                 ActionListener<ClearPrivilegesCacheResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::clearPrivilegesCache, options,
            ClearPrivilegesCacheResponse::fromXContent, listener, emptySet());
    }

    /**
     * Synchronously retrieve the X.509 certificates that are used to encrypt communications in an Elasticsearch cluster.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-ssl.html">
     * the docs</a> for more.
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the get certificates call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetSslCertificatesResponse getSslCertificates(RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(GetSslCertificatesRequest.INSTANCE, GetSslCertificatesRequest::getRequest,
            options, GetSslCertificatesResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously retrieve the X.509 certificates that are used to encrypt communications in an Elasticsearch cluster.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-ssl.html">
     * the docs</a> for more.
     *
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getSslCertificatesAsync(RequestOptions options, ActionListener<GetSslCertificatesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            GetSslCertificatesRequest.INSTANCE, GetSslCertificatesRequest::getRequest,
            options, GetSslCertificatesResponse::fromXContent, listener, emptySet());
    }

    /**
     * Change the password of a user of a native realm or built-in user synchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-change-password.html">
     * the docs</a> for more.
     *
     * @param request the request with the user's new password
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return {@code true} if the request succeeded (the new password was set)
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public boolean changePassword(ChangePasswordRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequest(request, SecurityRequestConverters::changePassword, options,
            RestHighLevelClient::convertExistsResponse, emptySet());
    }

    /**
     * Change the password of a user of a native realm or built-in user synchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-change-password.html">
     * the docs</a> for more.
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param request the request with the user's new password
     * @return {@code true} if the request succeeded (the new password was set)
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #changePassword(ChangePasswordRequest, RequestOptions)} instead
     */
    @Deprecated
    public boolean changePassword(RequestOptions options, ChangePasswordRequest request) throws IOException {
        return changePassword(request, options);
    }

    /**
     * Change the password of a user of a native realm or built-in user asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-change-password.html">
     * the docs</a> for more.
     *
     * @param request  the request with the user's new password
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable changePasswordAsync(ChangePasswordRequest request, RequestOptions options,
                                           ActionListener<Boolean> listener) {
        return restHighLevelClient.performRequestAsync(request, SecurityRequestConverters::changePassword, options,
            RestHighLevelClient::convertExistsResponse, listener, emptySet());
    }

    /**
     * Change the password of a user of a native realm or built-in user asynchronously.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-change-password.html">
     * the docs</a> for more.
     *
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param request  the request with the user's new password
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #changePasswordAsync(ChangePasswordRequest, RequestOptions, ActionListener)} instead
     * @return cancellable that may be used to cancel the request
     */
    @Deprecated
    public Cancellable changePasswordAsync(RequestOptions options, ChangePasswordRequest request,
                                           ActionListener<Boolean> listener) {
        return changePasswordAsync(request, options, listener);
    }

    /**
     * Delete a role mapping.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delete-role-mapping.html">
     * the docs</a> for more.
     * @param request the request with the role mapping name to be deleted.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the delete role mapping call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public DeleteRoleMappingResponse deleteRoleMapping(DeleteRoleMappingRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::deleteRoleMapping, options,
                DeleteRoleMappingResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously retrieves roles from the native roles store.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-role.html">
     * the docs</a> for more.
     *
     * @param request  the request with the roles to get
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getRolesAsync(GetRolesRequest request, RequestOptions options, ActionListener<GetRolesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::getRoles, options,
            GetRolesResponse::fromXContent, listener, emptySet());
    }

    /**
     * Retrieves roles from the native roles store.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-role.html">
     * the docs</a> for more.
     *
     * @param request the request with the roles to get
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the get roles call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetRolesResponse getRoles(final GetRolesRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::getRoles, options,
            GetRolesResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously creates or updates a role in the native roles store.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-role.html">
     * the docs</a> for more.
     *
     * @param request  the request containing the role to create or update
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putRoleAsync(PutRoleRequest request, RequestOptions options, ActionListener<PutRoleResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::putRole, options,
                PutRoleResponse::fromXContent, listener, emptySet());
    }

    /**
     * Create or update a role in the native roles store.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-role.html">
     * the docs</a> for more.
     *
     * @param request the request containing the role to create or update
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the put role call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public PutRoleResponse putRole(final PutRoleRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::putRole, options,
            PutRoleResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously delete a role mapping.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delete-role-mapping.html">
     * the docs</a> for more.
     * @param request the request with the role mapping name to be deleted.
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteRoleMappingAsync(DeleteRoleMappingRequest request, RequestOptions options,
                                              ActionListener<DeleteRoleMappingResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request,
            SecurityRequestConverters::deleteRoleMapping, options,
                DeleteRoleMappingResponse::fromXContent, listener, emptySet());
    }

    /**
     * Removes role from the native realm.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delete-role.html">
     * the docs</a> for more.
     * @param request the request with the role to delete
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the delete role call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public DeleteRoleResponse deleteRole(DeleteRoleRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::deleteRole, options,
            DeleteRoleResponse::fromXContent, singleton(404));
    }

    /**
     * Removes role from the native realm.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delete-role.html">
     * the docs</a> for more.
     * @param request the request with the role to delete
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteRoleAsync(DeleteRoleRequest request, RequestOptions options,
                                       ActionListener<DeleteRoleResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::deleteRole, options,
            DeleteRoleResponse::fromXContent, listener, singleton(404));
    }

    /**
     * Creates an OAuth2 token.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-token.html">
     * the docs</a> for more.
     *
     * @param request the request for the token
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the create token call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public CreateTokenResponse createToken(CreateTokenRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::createToken, options,
            CreateTokenResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously creates an OAuth2 token.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-token.html">
     * the docs</a> for more.
     *
     * @param request the request for the token
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable createTokenAsync(CreateTokenRequest request, RequestOptions options,
                                        ActionListener<CreateTokenResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::createToken, options,
            CreateTokenResponse::fromXContent, listener, emptySet());
    }

    /**
     * Invalidates an OAuth2 token.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-invalidate-token.html">
     * the docs</a> for more.
     *
     * @param request the request to invalidate the token
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the create token call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public InvalidateTokenResponse invalidateToken(InvalidateTokenRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::invalidateToken, options,
            InvalidateTokenResponse::fromXContent, singleton(404));
    }

    /**
     * Asynchronously invalidates an OAuth2 token.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-invalidate-token.html">
     * the docs</a> for more.
     * @param request the request to invalidate the token
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable invalidateTokenAsync(InvalidateTokenRequest request, RequestOptions options,
                                            ActionListener<InvalidateTokenResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::invalidateToken, options,
            InvalidateTokenResponse::fromXContent, listener, singleton(404));
    }

    /**
     * Synchronously get builtin (cluster &amp; index) privilege(s).
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-builtin-privileges.html">
     * the docs</a> for more.
     *
     * @param options the request options (e.g. headers), use
     *                {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the get builtin privileges call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetBuiltinPrivilegesResponse getBuiltinPrivileges(final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(GetBuiltinPrivilegesRequest.INSTANCE,
            GetBuiltinPrivilegesRequest::getRequest, options, GetBuiltinPrivilegesResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get builtin (cluster &amp; index) privilege(s).
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-builtin-privileges.html">
     * the docs</a> for more.
     *
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getBuiltinPrivilegesAsync(final RequestOptions options,
                                                 final ActionListener<GetBuiltinPrivilegesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(GetBuiltinPrivilegesRequest.INSTANCE,
            GetBuiltinPrivilegesRequest::getRequest, options, GetBuiltinPrivilegesResponse::fromXContent,
            listener, emptySet());
    }

    /**
     * Synchronously get application privilege(s).
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-privileges.html">
     * the docs</a> for more.
     *
     * @param request {@link GetPrivilegesRequest} with the application name and the privilege name.
     *                If no application name is provided, information about all privileges for all applications is retrieved.
     *                If no privilege name is provided, information about all privileges of the specified application is retrieved.
     * @param options the request options (e.g. headers), use
     *                {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the get privileges call
     * @throws IOException in case there is a problem sending the request or
     *                     parsing back the response
     */
    public GetPrivilegesResponse getPrivileges(final GetPrivilegesRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::getPrivileges,
            options, GetPrivilegesResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get application privilege(s).
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-privileges.html">
     * the docs</a> for more.
     *  @param request  {@link GetPrivilegesRequest} with the application name and the privilege name.
     *                 If no application name is provided, information about all privileges for all applications is retrieved.
     *                 If no privilege name is provided, information about all privileges of the specified application is retrieved.
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getPrivilegesAsync(final GetPrivilegesRequest request, final RequestOptions options,
                                          final ActionListener<GetPrivilegesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::getPrivileges,
            options, GetPrivilegesResponse::fromXContent, listener, emptySet());
    }

    /**
     * Create or update application privileges.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-privileges.html">
     * the docs</a> for more.
     *
     * @param request the request to create or update application privileges
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the create or update application privileges call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public PutPrivilegesResponse putPrivileges(final PutPrivilegesRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::putPrivileges, options,
                PutPrivilegesResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously create or update application privileges.<br>
     * See <a href=
     * "https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-privileges.html">
     * the docs</a> for more.
     *
     * @param request the request to create or update application privileges
     * @param options the request options (e.g. headers), use
     * {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putPrivilegesAsync(final PutPrivilegesRequest request, final RequestOptions options,
                                          final ActionListener<PutPrivilegesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::putPrivileges, options,
                PutPrivilegesResponse::fromXContent, listener, emptySet());
    }

    /**
     * Removes application privilege(s)
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delete-privilege.html">
     * the docs</a> for more.
     *
     * @param request the request with the application privilege to delete
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the delete application privilege call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public DeletePrivilegesResponse deletePrivileges(DeletePrivilegesRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::deletePrivileges, options,
            DeletePrivilegesResponse::fromXContent, singleton(404));
    }

    /**
     * Asynchronously removes an application privilege
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delete-privilege.html">
     * the docs</a> for more.
     *
     * @param request  the request with the application privilege to delete
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deletePrivilegesAsync(DeletePrivilegesRequest request, RequestOptions options,
                                             ActionListener<DeletePrivilegesResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::deletePrivileges, options,
            DeletePrivilegesResponse::fromXContent, listener, singleton(404));
    }

    /**
     * Create an API Key.<br>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html">
     * the docs</a> for more.
     *
     * @param request the request to create a API key
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the create API key call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public CreateApiKeyResponse createApiKey(final CreateApiKeyRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::createApiKey, options,
                CreateApiKeyResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously creates an API key.<br>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html">
     * the docs</a> for more.
     *
     * @param request the request to create a API key
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable createApiKeyAsync(final CreateApiKeyRequest request, final RequestOptions options,
                                         final ActionListener<CreateApiKeyResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::createApiKey, options,
                CreateApiKeyResponse::fromXContent, listener, emptySet());
    }

    /**
     * Retrieve API Key(s) information.<br>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-api-key.html">
     * the docs</a> for more.
     *
     * @param request the request to retrieve API key(s)
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the create API key call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetApiKeyResponse getApiKey(final GetApiKeyRequest request, final RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::getApiKey, options,
                GetApiKeyResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously retrieve API Key(s) information.<br>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-api-key.html">
     * the docs</a> for more.
     *
     * @param request the request to retrieve API key(s)
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getApiKeyAsync(final GetApiKeyRequest request, final RequestOptions options,
                                      final ActionListener<GetApiKeyResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::getApiKey, options,
                GetApiKeyResponse::fromXContent, listener, emptySet());
    }

    /**
     * Invalidate API Key(s).<br>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-invalidate-api-key.html">
     * the docs</a> for more.
     *
     * @param request the request to invalidate API key(s)
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the invalidate API key call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public InvalidateApiKeyResponse invalidateApiKey(final InvalidateApiKeyRequest request, final RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::invalidateApiKey, options,
                InvalidateApiKeyResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously invalidates API key(s).<br>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-invalidate-api-key.html">
     * the docs</a> for more.
     *
     * @param request the request to invalidate API key(s)
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable invalidateApiKeyAsync(final InvalidateApiKeyRequest request, final RequestOptions options,
                                             final ActionListener<InvalidateApiKeyResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::invalidateApiKey, options,
                InvalidateApiKeyResponse::fromXContent, listener, emptySet());
    }

    /**
     * Get an Elasticsearch access token from an {@code X509Certificate} chain. The certificate chain is that of the client from a mutually
     * authenticated TLS session, and it is validated by the PKI realms with {@code delegation.enabled} toggled to {@code true}.<br>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delegate-pki-authentication.html"> the
     * docs</a> for more details.
     *
     * @param request the request containing the certificate chain
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response from the delegate-pki-authentication API key call
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public DelegatePkiAuthenticationResponse delegatePkiAuthentication(DelegatePkiAuthenticationRequest request, RequestOptions options)
            throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, SecurityRequestConverters::delegatePkiAuthentication, options,
                DelegatePkiAuthenticationResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get an Elasticsearch access token from an {@code X509Certificate} chain. The certificate chain is that of the client
     * from a mutually authenticated TLS session, and it is validated by the PKI realms with {@code delegation.enabled} toggled to
     * {@code true}.<br>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-delegate-pki-authentication.html"> the
     * docs</a> for more details.
     *
     * @param request the request containing the certificate chain
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable delegatePkiAuthenticationAsync(DelegatePkiAuthenticationRequest request, RequestOptions options,
            ActionListener<DelegatePkiAuthenticationResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, SecurityRequestConverters::delegatePkiAuthentication, options,
                DelegatePkiAuthenticationResponse::fromXContent, listener, emptySet());
    }
}
