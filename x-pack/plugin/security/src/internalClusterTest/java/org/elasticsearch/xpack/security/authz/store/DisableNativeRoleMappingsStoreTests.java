/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class DisableNativeRoleMappingsStoreTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put("xpack.security.authc.native_role_mappings.enabled", "false");
        return builder.build();
    }

    public void testPutRoleMappingDisallowed() {
        // transport action
        NativeRoleMappingStore nativeRoleMappingStore = internalCluster().getInstance(NativeRoleMappingStore.class);
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        nativeRoleMappingStore.putRoleMapping(new PutRoleMappingRequest(), future);
        ExecutionException e = expectThrows(ExecutionException.class, future::get);
        assertThat(e.getMessage(), containsString("Native role mapping management is disabled"));
        // rest request
        Request request = new Request("POST", "_security/role_mapping/" + randomAlphaOfLength(8));
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        request.setOptions(options);
        ResponseException e2 = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(e2.getMessage(), containsString("Native role mapping management is not enabled in this Elasticsearch instance"));
        assertThat(e2.getResponse().getStatusLine().getStatusCode(), is(410)); // gone
    }

    public void testDeleteRoleMappingDisallowed() {
        // transport action
        NativeRoleMappingStore nativeRoleMappingStore = internalCluster().getInstance(NativeRoleMappingStore.class);
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        nativeRoleMappingStore.deleteRoleMapping(new DeleteRoleMappingRequest(), future);
        ExecutionException e = expectThrows(ExecutionException.class, future::get);
        assertThat(e.getMessage(), containsString("Native role mapping management is disabled"));
        // rest request
        Request request = new Request("DELETE", "_security/role_mapping/" + randomAlphaOfLength(8));
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        request.setOptions(options);
        ResponseException e2 = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(e2.getMessage(), containsString("Native role mapping management is not enabled in this Elasticsearch instance"));
        assertThat(e2.getResponse().getStatusLine().getStatusCode(), is(410)); // gone
    }

    public void testGetRoleMappingDisallowed() throws Exception {
        // transport action
        NativeRoleMappingStore nativeRoleMappingStore = internalCluster().getInstance(NativeRoleMappingStore.class);
        PlainActionFuture<List<ExpressionRoleMapping>> future = new PlainActionFuture<>();
        nativeRoleMappingStore.getRoleMappings(randomFrom(Set.of(randomAlphaOfLength(8)), null), future);
        assertThat(future.get(), emptyIterable());
        // rest request
        Request request = new Request("GET", "_security/role_mapping/" + randomAlphaOfLength(8));
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        request.setOptions(options);
        ResponseException e2 = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(e2.getMessage(), containsString("Native role mapping management is not enabled in this Elasticsearch instance"));
        assertThat(e2.getResponse().getStatusLine().getStatusCode(), is(410)); // gone
    }

    public void testResolveRoleMappings() throws Exception {
        NativeRoleMappingStore nativeRoleMappingStore = internalCluster().getInstance(NativeRoleMappingStore.class);
        UserRoleMapper.UserData userData = new UserRoleMapper.UserData(
            randomAlphaOfLength(4),
            null,
            randomFrom(Set.of(randomAlphaOfLength(4)), Set.of()),
            Map.of(),
            mock(RealmConfig.class)
        );
        PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
        nativeRoleMappingStore.resolveRoles(userData, future);
        assertThat(future.get(), emptyIterable());
    }
}
