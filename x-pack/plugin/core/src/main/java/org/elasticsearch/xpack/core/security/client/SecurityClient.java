/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequestBuilder;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleResponse;
import org.elasticsearch.xpack.core.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.core.security.action.role.GetRolesRequest;
import org.elasticsearch.xpack.core.security.action.role.GetRolesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateRequestBuilder;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateResponse;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationRequestBuilder;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequestBuilder;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequestBuilder;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordResponse;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserRequest;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserResponse;
import org.elasticsearch.xpack.core.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.core.security.action.user.GetUsersRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUsersRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledRequest;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import java.io.IOException;
import java.util.List;

/**
 * A wrapper to elasticsearch clients that exposes all Security related APIs
 */
public class SecurityClient {

    private final ElasticsearchClient client;

    public SecurityClient(ElasticsearchClient client) {
        this.client = client;
    }

    /****************
     * authc things *
     ****************/

    /**
     * Populates a {@link ClearRealmCacheRequest} to clear the realm caches.
     * @return {@link ClearRealmCacheRequestBuilder}
     */
    public ClearRealmCacheRequestBuilder prepareClearRealmCache() {
        return new ClearRealmCacheRequestBuilder(client);
    }

    /**
     * Clears the realm caches. It's possible to clear all user entries from all realms in the cluster or alternatively select the realms
     * (by their unique names) and/or users (by their usernames) that should be evicted.
     * @param request A {@link ClearRealmCacheRequest} specifying the realms and/or users
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void clearRealmCache(ClearRealmCacheRequest request, ActionListener<ClearRealmCacheResponse> listener) {
        client.execute(ClearRealmCacheAction.INSTANCE, request, listener);
    }

    /**
     * Clears the realm caches. It's possible to clear all user entries from all realms in the cluster or alternatively select the realms
     * (by their unique names) and/or users (by their usernames) that should be evicted.
     * @param request A {@link ClearRealmCacheRequest} specifying the realms and/or users
     * @return {@link ActionFuture}
     */
    public ActionFuture<ClearRealmCacheResponse> clearRealmCache(ClearRealmCacheRequest request) {
        return client.execute(ClearRealmCacheAction.INSTANCE, request);
    }

    /****************
     * authz things *
     ****************/

    /**
     * Initialises a {@link ClearRolesCacheRequest} to clear the roles cache.
     * @return {@link ClearRolesCacheRequestBuilder}
     */
    public ClearRolesCacheRequestBuilder prepareClearRolesCache() {
        return new ClearRolesCacheRequestBuilder(client);
    }

    /**
     * Clears the roles cache. This API only works for the native roles that are stored in an elasticsearch index. It is possible to clear
     * the cache of all roles or to specify the names of individual roles that should have their cache cleared.
     * @param request A {@link ClearRolesCacheRequest} which may specify the roles. If no roles are specified, all roles will be evicted
     *                from the cache.
     * @param listener A listener for action responses or failures
     */
    public void clearRolesCache(ClearRolesCacheRequest request, ActionListener<ClearRolesCacheResponse> listener) {
        client.execute(ClearRolesCacheAction.INSTANCE, request, listener);
    }

    /**
     * Clears the roles cache. This API only works for the native roles that are stored in an elasticsearch index. It is possible to clear
     * the cache of all roles or to specify the names of individual roles that should have their cache cleared.
     * @param request A {@link ClearRolesCacheRequest} which may specify the roles. If no roles are specified, all roles will be evicted
     *                from the cache.
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public ActionFuture<ClearRolesCacheResponse> clearRolesCache(ClearRolesCacheRequest request) {
        return client.execute(ClearRolesCacheAction.INSTANCE, request);
    }

    /****************************
     * Permissions / Privileges *
     ****************************/

    /**
     * Populates a {@link HasPrivilegesRequest} to check a user's privileges.
     * @param username The user to check
     * @return {@link HasPrivilegesRequestBuilder}
     */
    public HasPrivilegesRequestBuilder prepareHasPrivileges(String username) {
        return new HasPrivilegesRequestBuilder(client).username(username);
    }

    /**
     * Populates a {@link HasPrivilegesRequest} to check a user's privileges.
     * @param username The user to check
     * @param source The {@link BytesReference} from which to read the content of an incoming request; this will be parsed to give the set
     *               of privileges to check
     * @param xContentType The content type of {@code source}
     * @return {@link HasPrivilegesRequestBuilder}
     * @throws IOException if there was an error reading from {@code source}
     */
    public HasPrivilegesRequestBuilder prepareHasPrivileges(String username, BytesReference source, XContentType xContentType)
            throws IOException {
        return new HasPrivilegesRequestBuilder(client).source(username, source, xContentType);
    }

    /**
     * Checks whether the user has the given privilege(s).
     * @param request A {@link HasPrivilegesRequest} specifying the user and privileges to check
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void hasPrivileges(HasPrivilegesRequest request, ActionListener<HasPrivilegesResponse> listener) {
        client.execute(HasPrivilegesAction.INSTANCE, request, listener);
    }

    /*******************
     * User Management *
     *******************/

    /**
     * Populates a {@link GetUsersRequest} to get information about a user or users.
     * @param usernames The user(s) to get
     * @return {@link GetUsersRequestBuilder}
     */
    public GetUsersRequestBuilder prepareGetUsers(String... usernames) {
        return new GetUsersRequestBuilder(client).usernames(usernames);
    }

    /**
     * Gets information about a user or users such as roles, metadata, full name and email (if available).
     * @param request A {@link GetUsersRequest} specifying the user(s) to fetch
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void getUsers(GetUsersRequest request, ActionListener<GetUsersResponse> listener) {
        client.execute(GetUsersAction.INSTANCE, request, listener);
    }

    /**
     * Populates a {@link DeleteUserRequest} to delete a native user.
     * @param username The user to delete
     * @return {@link DeleteUserRequestBuilder}
     */
    public DeleteUserRequestBuilder prepareDeleteUser(String username) {
        return new DeleteUserRequestBuilder(client).username(username);
    }

    /**
     * Deletes the given user in the native realm.
     * @param request A {@link DeleteUserRequest} specifying the user to delete
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void deleteUser(DeleteUserRequest request, ActionListener<DeleteUserResponse> listener) {
        client.execute(DeleteUserAction.INSTANCE, request, listener);
    }

    /**
     * Populates a {@link PutUserRequest} to create or update a user in the native realm.
     * @param username The user to create/update
     * @param source The {@link BytesReference} from which to read the content of an incoming request containing the user's password, roles
     *               and any other data
     * @param xContentType The content type of {@code source}
     * @param hasher A password hashing function e.g. {@link Hasher.BCRYPT}
     * @return {@link PutUserRequestBuilder}
     * @throws IOException if there was an error reading from {@code source}
     */
    public PutUserRequestBuilder preparePutUser(String username, BytesReference source, XContentType xContentType, Hasher hasher)
        throws IOException {
        return new PutUserRequestBuilder(client).source(username, source, xContentType, hasher);
    }

    /**
     * Populates a {@link PutUserRequest} to create or update a user in the native realm.
     * @param username The user to create/update
     * @param password The user's password
     * @param hasher A password hashing function e.g. {@link Hasher.BCRYPT}
     * @param roles The user's roles
     * @return {@link PutUserRequestBuilder}
     */
    public PutUserRequestBuilder preparePutUser(String username, char[] password, Hasher hasher, String... roles) {
        return new PutUserRequestBuilder(client).username(username).password(password, hasher).roles(roles);
    }

    /**
     * Creates or updates a user in the native realm.
     * @param request A {@link PutUserRequest} with the user's name, password hash, roles and any other data
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void putUser(PutUserRequest request, ActionListener<PutUserResponse> listener) {
        client.execute(PutUserAction.INSTANCE, request, listener);
    }

    /**
     * Populates a {@link ChangePasswordRequest} with the username and password.
     * @param username The user whose password should be changed
     * @param password The new password. Note: the passed in char[] will be cleared by this method.
     * @param hasher A password hashing function e.g. {@link Hasher.BCRYPT}
     * @return {@link ChangePasswordRequest}
     */
    public ChangePasswordRequestBuilder prepareChangePassword(String username, char[] password, Hasher hasher) {
        return new ChangePasswordRequestBuilder(client).username(username).password(password, hasher);
    }

    /**
     * Populates a {@link ChangePasswordRequest} with the username and password.
     * @param username The user whose password should be changed
     * @param source The {@link BytesReference} from which to read the content of an incoming request; this will be parsed to get the new
     *               password.
     * @param xContentType The content type of {@code source}
     * @param hasher A password hashing function e.g. {@link Hasher.BCRYPT}
     * @return {@link ChangePasswordRequest}
     * @throws IOException if there was an error reading from {@code source}
     */
    public ChangePasswordRequestBuilder prepareChangePassword(String username, BytesReference source, XContentType xContentType,
                                                              Hasher hasher) throws IOException {
        return new ChangePasswordRequestBuilder(client).username(username).source(source, xContentType, hasher);
    }

    /**
     * Changes the user's password.
     * @param A {@link ChangePasswordRequest} with the username and password
     * @param An {@link ActionListener} to handle action responses or failures
     */
    public void changePassword(ChangePasswordRequest request, ActionListener<ChangePasswordResponse> listener) {
        client.execute(ChangePasswordAction.INSTANCE, request, listener);
    }

    /**
     * Populates a {@link SetEnabledRequest} with the username and the {@code enabled} flag.
     * @param username The username to enable/disable
     * @param enabled True if the user should be enabled, false otherwise
     * @return {@link SetEnabledRequestBuilder}
     */
    public SetEnabledRequestBuilder prepareSetEnabled(String username, boolean enabled) {
        return new SetEnabledRequestBuilder(client).username(username).enabled(enabled);
    }

    /**
     * Enables or disables a user.
     * @param request A {@link SetEnabledRequest} with the username and {@code enabled} flag
     * @param An {@link ActionListener} to handle action responses or failures
     */
    public void setEnabled(SetEnabledRequest request, ActionListener<SetEnabledResponse> listener) {
        client.execute(SetEnabledAction.INSTANCE, request, listener);
    }

    /*******************
     * Role Management *
     *******************/

    /**
     * Populates a {@link GetRolesRequest} to get information about a role or roles.
     * @param names The role(s) to get
     * @return {@link GetRolesRequestBuilder}
     */
    public GetRolesRequestBuilder prepareGetRoles(String... names) {
        return new GetRolesRequestBuilder(client).names(names);
    }

    /**
     * Gets the privileges associated with a role or roles.
     * @param request A {@link GetRolesRequest} specifying the role(s) to fetch
     * @param An {@link ActionListener} to handle action responses or failures
     */
    public void getRoles(GetRolesRequest request, ActionListener<GetRolesResponse> listener) {
        client.execute(GetRolesAction.INSTANCE, request, listener);
    }

    /**
     * Populates a {@link DeleteRoleRequest} to delete a role.
     * @param name The role to delete
     * @return {@link DeleteRoleRequestBuilder}
     */
    public DeleteRoleRequestBuilder prepareDeleteRole(String name) {
        return new DeleteRoleRequestBuilder(client).name(name);
    }

    /**
     * Deletes the given role.
     * @param request A {@link DeleteRoleRequest} specifying the role to delete
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void deleteRole(DeleteRoleRequest request, ActionListener<DeleteRoleResponse> listener) {
        client.execute(DeleteRoleAction.INSTANCE, request, listener);
    }

    /**
     * Populates a {@link PutRoleRequest} to create or update a role.
     * @param name The role to create/update
     * @return {@link PutRoleRequestBuilder}
     */
    public PutRoleRequestBuilder preparePutRole(String name) {
        return new PutRoleRequestBuilder(client).name(name);
    }

    /**
     * Populates a {@link PutRoleRequest} to create or update a role.
     * @param name The role to create/update
     * @param source The {@link BytesReference} from which to read the content of an incoming request containing the role's privileges and
     *               any other data
     * @param xContentType The content type of {@code source}
     * @return {@link PutRoleRequestBuilder}
     * @throws IOException if there was an error reading from {@code source}
     */
    public PutRoleRequestBuilder preparePutRole(String name, BytesReference source, XContentType xContentType) throws IOException {
        return new PutRoleRequestBuilder(client).source(name, source, xContentType);
    }

    /**
     * Creates or updates a role.
     * @param request A {@link PutRoleRequest} with the role's name and privileges
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void putRole(PutRoleRequest request, ActionListener<PutRoleResponse> listener) {
        client.execute(PutRoleAction.INSTANCE, request, listener);
    }

    /*****************
     * Role Mappings *
     *****************/

    /**
     * Populates a {@link GetRoleMappingsRequest} to retrieve role mappings from the native role mapping store.
     * @param names The role mapping(s) to get
     * @return {@link GetRoleMappingsRequestBuilder}
     *
     * @see org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping
     */
    public GetRoleMappingsRequestBuilder prepareGetRoleMappings(String... names) {
        return new GetRoleMappingsRequestBuilder(client, GetRoleMappingsAction.INSTANCE)
                .names(names);
    }

    /**
     * Gets the specific role mappings from the native role mapping store.
     * @param request A {@link GetRoleMappingsRequest} specifying the role mapping(s) to fetch
     * @param listener An {@link ActionListener} to handle action responses or failures
     *
     * @see org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping
     */
    public void getRoleMappings(GetRoleMappingsRequest request,
                                ActionListener<GetRoleMappingsResponse> listener) {
        client.execute(GetRoleMappingsAction.INSTANCE, request, listener);
    }

    /**
     * Populates a {@link PutRoleMappingRequest} to create or update a role mapping.
     * @param name The role mapping to create/update
     * @param source The {@link BytesReference} from which to read the content of an incoming request containing the mapping's roles, and
     *               the rules for applying them.
     * @param xContentType The content type of {@code source}
     * @return {@link PutRoleMappingRequestBuilder}
     * @throws IOException if there was an error reading from {@code source}
     *
     * @see org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping
     */
    public PutRoleMappingRequestBuilder preparePutRoleMapping(
            String name, BytesReference content, XContentType xContentType) throws IOException {
        return new PutRoleMappingRequestBuilder(client, PutRoleMappingAction.INSTANCE).source(name, content, xContentType);
    }

    /**
     * Populates a {@link DeleteRoleMappingRequest} to delete a role mapping.
     * @param name The role mapping to delete
     * @return {@link DeleteRoleMappingRequestBuilder}
     *
     * @see org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping
     */
    public DeleteRoleMappingRequestBuilder prepareDeleteRoleMapping(String name) {
        return new DeleteRoleMappingRequestBuilder(client, DeleteRoleMappingAction.INSTANCE)
                .name(name);
    }

    /**************************
     * Application Privileges *
     **************************/

    /**
     * Populates a {@link GetPrivilegesRequest} to retrieve one or more application privileges.
     * @param applicationName The name of the application
     * @param privileges The application privileges to fetch
     * @return {@link GetPrivilegesRequestBuilder}
     */
    public GetPrivilegesRequestBuilder prepareGetPrivileges(String applicationName, String[] privileges) {
        return new GetPrivilegesRequestBuilder(client, GetPrivilegesAction.INSTANCE).application(applicationName).privileges(privileges);
    }

    /**
     * Populates a {@link PutPrivilegesRequest} to create or update application privileges.
     * @param source The {@link BytesReference} from which to read the content of an incoming request containing the application and
     *               privileges.
     * @param xContentType The content type of {@code source}
     * @return {@link PutPrivilegesRequestBuilder}
     * @throws IOException if there was an error reading from {@code source}
     */
    public PutPrivilegesRequestBuilder preparePutPrivileges(BytesReference source, XContentType xContentType) throws IOException {
        return new PutPrivilegesRequestBuilder(client, PutPrivilegesAction.INSTANCE).source(source, xContentType);
    }

    /**
     * Populates a {@link DeletePrivilegesRequest} to delete application privileges.
     * @param applicationName The name of the application
     * @param privileges The privileges to delete
     * @return {@link DeletePrivilegesRequestBuilder}
     */
    public DeletePrivilegesRequestBuilder prepareDeletePrivileges(String applicationName, String[] privileges) {
        return new DeletePrivilegesRequestBuilder(client, DeletePrivilegesAction.INSTANCE)
            .application(applicationName)
            .privileges(privileges);
    }

    /********************
     * Token management *
     ********************/

    /**
     * Initialises a {@link CreateTokenRequest} to create an OAuth token.
     * @return {@link CreateTokenRequestBuilder}
     */
    public CreateTokenRequestBuilder prepareCreateToken() {
        return new CreateTokenRequestBuilder(client, CreateTokenAction.INSTANCE);
    }

    /**
     * Creates an OAuth token.
     * @param request A {@link CreateTokenRequest} with the credentials
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void createToken(CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        client.execute(CreateTokenAction.INSTANCE, request, listener);
    }

    /**
     * Populates an {@link InvalidateTokenRequest}
     * @param token The string representation of the token;
     * @return {@link InvalidateTokenRequestBuilder}
     * @see {@link org.elasticsearch.xpack.security.authc.TokenService#getUserTokenString}
     */
    public InvalidateTokenRequestBuilder prepareInvalidateToken(String token) {
        return new InvalidateTokenRequestBuilder(client).setTokenString(token);
    }

    /**
     * Invalidates the given token.
     * @param request An {@link InvalidateTokenRequest} specifying the token and its type
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void invalidateToken(InvalidateTokenRequest request, ActionListener<InvalidateTokenResponse> listener) {
        client.execute(InvalidateTokenAction.INSTANCE, request, listener);
    }

    /**
     * Populates a {@link SamlAuthenticateRequest} to authenticate using SAML assertions.
     * @param xmlContent The SAML response from the identity provider (decoded)
     * @param validIds The SAML request ID(s)
     * @return {@link SamlAuthenticateRequestBuilder}
     */
    public SamlAuthenticateRequestBuilder prepareSamlAuthenticate(byte[] xmlContent, List<String> validIds) {
        final SamlAuthenticateRequestBuilder builder = new SamlAuthenticateRequestBuilder(client);
        builder.saml(xmlContent);
        builder.validRequestIds(validIds);
        return builder;
    }

    /**
     * Authenticates using SAML assertions.
     * @param request A {@link SamlAuthenticateRequest} with the response from the identity provider
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void samlAuthenticate(SamlAuthenticateRequest request, ActionListener<SamlAuthenticateResponse> listener) {
        client.execute(SamlAuthenticateAction.INSTANCE, request, listener);
    }

    /**
     * Initialises a {@link SamlPrepareAuthenticationRequest}
     * @return {@link SamlPrepareAuthenticationRequestBuilder}
     */
    public SamlPrepareAuthenticationRequestBuilder prepareSamlPrepareAuthentication() {
        return new SamlPrepareAuthenticationRequestBuilder(client);
    }

    /**
     * Populates a {@link CreateTokenRequest} with a refresh token.
     * @param refreshToken The refresh token to use
     * @return {@link CreateTokenRequest} with the grant type set to <code>refresh_token</code>
     */
    public CreateTokenRequestBuilder prepareRefreshToken(String refreshToken) {
        return new CreateTokenRequestBuilder(client, RefreshTokenAction.INSTANCE)
                .setRefreshToken(refreshToken)
                .setGrantType("refresh_token");
    }

    /**
     * Makes an request to refresh the token.
     * @param request {@link CreateTokenRequest} with the refresh token
     * @param listener An {@link ActionListener} to handle action responses or failures
     */
    public void refreshToken(CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        client.execute(RefreshTokenAction.INSTANCE, request, listener);
    }
}
