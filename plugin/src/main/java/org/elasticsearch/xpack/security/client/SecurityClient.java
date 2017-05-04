/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheRequestBuilder;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheRequestBuilder;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.security.action.role.DeleteRoleRequestBuilder;
import org.elasticsearch.xpack.security.action.role.DeleteRoleResponse;
import org.elasticsearch.xpack.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.security.action.role.GetRolesRequest;
import org.elasticsearch.xpack.security.action.role.GetRolesRequestBuilder;
import org.elasticsearch.xpack.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.DeleteRoleMappingRequestBuilder;
import org.elasticsearch.xpack.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.security.action.rolemapping.GetRoleMappingsRequest;
import org.elasticsearch.xpack.security.action.rolemapping.GetRoleMappingsRequestBuilder;
import org.elasticsearch.xpack.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.PutRoleMappingRequestBuilder;
import org.elasticsearch.xpack.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.security.action.token.CreateTokenRequestBuilder;
import org.elasticsearch.xpack.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.security.action.token.InvalidateTokenRequestBuilder;
import org.elasticsearch.xpack.security.action.token.InvalidateTokenResponse;
import org.elasticsearch.xpack.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.security.action.user.ChangePasswordResponse;
import org.elasticsearch.xpack.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.security.action.user.DeleteUserRequest;
import org.elasticsearch.xpack.security.action.user.DeleteUserRequestBuilder;
import org.elasticsearch.xpack.security.action.user.DeleteUserResponse;
import org.elasticsearch.xpack.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.security.action.user.GetUsersRequest;
import org.elasticsearch.xpack.security.action.user.GetUsersRequestBuilder;
import org.elasticsearch.xpack.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.security.action.user.PutUserAction;
import org.elasticsearch.xpack.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.security.action.user.SetEnabledRequest;
import org.elasticsearch.xpack.security.action.user.SetEnabledRequestBuilder;
import org.elasticsearch.xpack.security.action.user.SetEnabledResponse;

import java.io.IOException;

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
     * Clears the realm caches. It's possible to clear all user entries from all realms in the cluster or alternatively
     * select the realms (by their unique names) and/or users (by their usernames) that should be evicted.
     */
    @SuppressWarnings("unchecked")
    public ClearRealmCacheRequestBuilder prepareClearRealmCache() {
        return new ClearRealmCacheRequestBuilder(client);
    }

    /**
     * Clears the realm caches. It's possible to clear all user entries from all realms in the cluster or alternatively
     * select the realms (by their unique names) and/or users (by their usernames) that should be evicted.
     */
    @SuppressWarnings("unchecked")
    public void clearRealmCache(ClearRealmCacheRequest request, ActionListener<ClearRealmCacheResponse> listener) {
        client.execute(ClearRealmCacheAction.INSTANCE, request, listener);
    }

    /**
     * Clears the realm caches. It's possible to clear all user entries from all realms in the cluster or alternatively
     * select the realms (by their unique names) and/or users (by their usernames) that should be evicted.
     */
    @SuppressWarnings("unchecked")
    public ActionFuture<ClearRealmCacheResponse> clearRealmCache(ClearRealmCacheRequest request) {
        return client.execute(ClearRealmCacheAction.INSTANCE, request);
    }

    /****************
     * authz things *
     ****************/

    /**
     * Clears the roles cache. This API only works for the naitve roles that are stored in an elasticsearch index. It is
     * possible to clear the cache of all roles or to specify the names of individual roles that should have their cache
     * cleared.
     */
    public ClearRolesCacheRequestBuilder prepareClearRolesCache() {
        return new ClearRolesCacheRequestBuilder(client);
    }

    /**
     * Clears the roles cache. This API only works for the naitve roles that are stored in an elasticsearch index. It is
     * possible to clear the cache of all roles or to specify the names of individual roles that should have their cache
     * cleared.
     */
    public void clearRolesCache(ClearRolesCacheRequest request, ActionListener<ClearRolesCacheResponse> listener) {
        client.execute(ClearRolesCacheAction.INSTANCE, request, listener);
    }

    /**
     * Clears the roles cache. This API only works for the naitve roles that are stored in an elasticsearch index. It is
     * possible to clear the cache of all roles or to specify the names of individual roles that should have their cache
     * cleared.
     */
    public ActionFuture<ClearRolesCacheResponse> clearRolesCache(ClearRolesCacheRequest request) {
        return client.execute(ClearRolesCacheAction.INSTANCE, request);
    }

    /**
     * Permissions / Privileges
     */
    public HasPrivilegesRequestBuilder prepareHasPrivileges(String username) {
        return new HasPrivilegesRequestBuilder(client).username(username);
    }

    public HasPrivilegesRequestBuilder prepareHasPrivileges(String username, BytesReference source, XContentType xContentType)
            throws IOException {
        return new HasPrivilegesRequestBuilder(client).source(username, source, xContentType);
    }

    public void hasPrivileges(HasPrivilegesRequest request, ActionListener<HasPrivilegesResponse> listener) {
        client.execute(HasPrivilegesAction.INSTANCE, request, listener);
    }

    /** User Management */

    public GetUsersRequestBuilder prepareGetUsers(String... usernames) {
        return new GetUsersRequestBuilder(client).usernames(usernames);
    }

    public void getUsers(GetUsersRequest request, ActionListener<GetUsersResponse> listener) {
        client.execute(GetUsersAction.INSTANCE, request, listener);
    }

    public DeleteUserRequestBuilder prepareDeleteUser(String username) {
        return new DeleteUserRequestBuilder(client).username(username);
    }

    public void deleteUser(DeleteUserRequest request, ActionListener<DeleteUserResponse> listener) {
        client.execute(DeleteUserAction.INSTANCE, request, listener);
    }

    public PutUserRequestBuilder preparePutUser(String username, BytesReference source, XContentType xContentType) throws IOException {
        return new PutUserRequestBuilder(client).source(username, source, xContentType);
    }

    public PutUserRequestBuilder preparePutUser(String username, char[] password, String... roles) {
        return new PutUserRequestBuilder(client).username(username).password(password).roles(roles);
    }

    public void putUser(PutUserRequest request, ActionListener<PutUserResponse> listener) {
        client.execute(PutUserAction.INSTANCE, request, listener);
    }

    /**
     * Populates the {@link ChangePasswordRequest} with the username and password. Note: the passed in char[] will be cleared by this
     * method.
     */
    public ChangePasswordRequestBuilder prepareChangePassword(String username, char[] password) {
        return new ChangePasswordRequestBuilder(client).username(username).password(password);
    }

    public ChangePasswordRequestBuilder prepareChangePassword(String username, BytesReference source, XContentType xContentType)
            throws IOException {
        return new ChangePasswordRequestBuilder(client).username(username).source(source, xContentType);
    }

    public void changePassword(ChangePasswordRequest request, ActionListener<ChangePasswordResponse> listener) {
        client.execute(ChangePasswordAction.INSTANCE, request, listener);
    }

    public SetEnabledRequestBuilder prepareSetEnabled(String username, boolean enabled) {
        return new SetEnabledRequestBuilder(client).username(username).enabled(enabled);
    }

    public void setEnabled(SetEnabledRequest request, ActionListener<SetEnabledResponse> listener) {
        client.execute(SetEnabledAction.INSTANCE, request, listener);
    }

    /** Role Management */

    public GetRolesRequestBuilder prepareGetRoles(String... names) {
        return new GetRolesRequestBuilder(client).names(names);
    }

    public void getRoles(GetRolesRequest request, ActionListener<GetRolesResponse> listener) {
        client.execute(GetRolesAction.INSTANCE, request, listener);
    }

    public DeleteRoleRequestBuilder prepareDeleteRole(String name) {
        return new DeleteRoleRequestBuilder(client).name(name);
    }

    public void deleteRole(DeleteRoleRequest request, ActionListener<DeleteRoleResponse> listener) {
        client.execute(DeleteRoleAction.INSTANCE, request, listener);
    }

    public PutRoleRequestBuilder preparePutRole(String name) {
        return new PutRoleRequestBuilder(client).name(name);
    }

    public PutRoleRequestBuilder preparePutRole(String name, BytesReference source, XContentType xContentType) throws IOException {
        return new PutRoleRequestBuilder(client).source(name, source, xContentType);
    }

    public void putRole(PutRoleRequest request, ActionListener<PutRoleResponse> listener) {
        client.execute(PutRoleAction.INSTANCE, request, listener);
    }

    /** Role Mappings */

    public GetRoleMappingsRequestBuilder prepareGetRoleMappings(String... names) {
        return new GetRoleMappingsRequestBuilder(client, GetRoleMappingsAction.INSTANCE)
                .names(names);
    }

    public void getRoleMappings(GetRoleMappingsRequest request,
                                ActionListener<GetRoleMappingsResponse> listener) {
        client.execute(GetRoleMappingsAction.INSTANCE, request, listener);
    }

    public PutRoleMappingRequestBuilder preparePutRoleMapping(
            String name, BytesReference content, XContentType xContentType) throws IOException {
        return new PutRoleMappingRequestBuilder(client, PutRoleMappingAction.INSTANCE).source(name, content, xContentType);
    }

    public DeleteRoleMappingRequestBuilder prepareDeleteRoleMapping(String name) {
        return new DeleteRoleMappingRequestBuilder(client, DeleteRoleMappingAction.INSTANCE)
                .name(name);
    }

    public CreateTokenRequestBuilder prepareCreateToken() {
        return new CreateTokenRequestBuilder(client);
    }

    public void createToken(CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        client.execute(CreateTokenAction.INSTANCE, request, listener);
    }

    public InvalidateTokenRequestBuilder prepareInvalidateToken(String token) {
        return new InvalidateTokenRequestBuilder(client).setTokenString(token);
    }

    public void invalidateToken(InvalidateTokenRequest request, ActionListener<InvalidateTokenResponse> listener) {
        client.execute(InvalidateTokenAction.INSTANCE, request, listener);
    }
}
