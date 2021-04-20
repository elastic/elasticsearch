/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.service.ElasticServiceAccounts.ElasticServiceAccount;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticServiceAccountsTests extends ESTestCase {

    public void testElasticFleetPrivileges() {
        final Role role = Role.builder(ElasticServiceAccounts.ACCOUNTS.get("elastic/fleet-server").roleDescriptor(), null).build();
        final Authentication authentication = mock(Authentication.class);
        assertThat(role.cluster().check(CreateApiKeyAction.NAME,
            new CreateApiKeyRequest(randomAlphaOfLengthBetween(3, 8), null, null), authentication), is(true));
        assertThat(role.cluster().check(GetApiKeyAction.NAME, GetApiKeyRequest.forOwnedApiKeys(), authentication), is(true));
        assertThat(role.cluster().check(InvalidateApiKeyAction.NAME, InvalidateApiKeyRequest.forOwnedApiKeys(), authentication), is(true));

        assertThat(role.cluster().check(GetApiKeyAction.NAME, randomFrom(GetApiKeyRequest.forAllApiKeys()), authentication), is(false));
        assertThat(role.cluster().check(InvalidateApiKeyAction.NAME,
            InvalidateApiKeyRequest.usingUserName(randomAlphaOfLengthBetween(3, 16)), authentication), is(false));

        List.of(
            "logs-" + randomAlphaOfLengthBetween(1, 20),
            "metrics-" + randomAlphaOfLengthBetween(1, 20),
            "traces-" + randomAlphaOfLengthBetween(1, 20),
            "synthetics-" + randomAlphaOfLengthBetween(1, 20),
            ".logs-endpoint.diagnostic.collection-" + randomAlphaOfLengthBetween(1, 20))
            .stream().map(this::mockIndexAbstraction)
            .forEach(index -> {
                assertThat(role.indices().allowedIndicesMatcher(AutoPutMappingAction.NAME).test(index), is(true));
                assertThat(role.indices().allowedIndicesMatcher(AutoCreateAction.NAME).test(index), is(true));
                assertThat(role.indices().allowedIndicesMatcher(DeleteAction.NAME).test(index), is(true));
                assertThat(role.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(index), is(true));
                assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(index), is(true));
                assertThat(role.indices().allowedIndicesMatcher(BulkAction.NAME).test(index), is(true));
                assertThat(role.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(index), is(false));
                assertThat(role.indices().allowedIndicesMatcher(GetAction.NAME).test(index), is(false));
                assertThat(role.indices().allowedIndicesMatcher(MultiGetAction.NAME).test(index), is(false));
                assertThat(role.indices().allowedIndicesMatcher(SearchAction.NAME).test(index), is(false));
                assertThat(role.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(index), is(false));
                assertThat(role.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(index), is(false));
            });

        final String dotFleetIndexName = ".fleet-" + randomAlphaOfLengthBetween(1, 20);
        final IndexAbstraction dotFleetIndex = mockIndexAbstraction(dotFleetIndexName);
        assertThat(role.indices().allowedIndicesMatcher(DeleteAction.NAME).test(dotFleetIndex), is(true));
        assertThat(role.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(dotFleetIndex), is(true));
        assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(dotFleetIndex), is(true));
        assertThat(role.indices().allowedIndicesMatcher(BulkAction.NAME).test(dotFleetIndex), is(true));
        assertThat(role.indices().allowedIndicesMatcher(GetAction.NAME).test(dotFleetIndex), is(true));
        assertThat(role.indices().allowedIndicesMatcher(MultiGetAction.NAME).test(dotFleetIndex), is(true));
        assertThat(role.indices().allowedIndicesMatcher(SearchAction.NAME).test(dotFleetIndex), is(true));
        assertThat(role.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(dotFleetIndex), is(true));
        assertThat(role.indices().allowedIndicesMatcher(IndicesStatsAction.NAME).test(dotFleetIndex), is(true));
        assertThat(role.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(dotFleetIndex), is(false));
        assertThat(role.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(dotFleetIndex), is(false));
        assertThat(role.indices().allowedIndicesMatcher("indices:foo").test(dotFleetIndex), is(false));
        // TODO: more tests when role descriptor is finalised for elastic/fleet-server
    }

    public void testElasticServiceAccount() {
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String principal = ElasticServiceAccounts.NAMESPACE + "/" + serviceName;
        final RoleDescriptor roleDescriptor1 = new RoleDescriptor(principal, null, null, null);
        final ElasticServiceAccount serviceAccount = new ElasticServiceAccount(
            serviceName, roleDescriptor1);
        assertThat(serviceAccount.id(), equalTo(new ServiceAccount.ServiceAccountId(ElasticServiceAccounts.NAMESPACE, serviceName)));
        assertThat(serviceAccount.roleDescriptor(), equalTo(roleDescriptor1));
        assertThat(serviceAccount.asUser(), equalTo(new User(principal, Strings.EMPTY_ARRAY,
            "Service account - " + principal, null,
            Map.of("_elastic_service_account", true),
            true)));

        final NullPointerException e1 =
            expectThrows(NullPointerException.class, () -> new ElasticServiceAccount(serviceName, null));
        assertThat(e1.getMessage(), containsString("Role descriptor cannot be null"));

        final RoleDescriptor roleDescriptor2 = new RoleDescriptor(randomAlphaOfLengthBetween(6, 16),
            null, null, null);
        final IllegalArgumentException e2 =
            expectThrows(IllegalArgumentException.class, () -> new ElasticServiceAccount(serviceName, roleDescriptor2));
        assertThat(e2.getMessage(), containsString(
            "the provided role descriptor [" + roleDescriptor2.getName()
                + "] must have the same name as the service account [" + principal + "]"));
    }

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(randomFrom(IndexAbstraction.Type.CONCRETE_INDEX,
            IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM));
        return mock;
    }
}
