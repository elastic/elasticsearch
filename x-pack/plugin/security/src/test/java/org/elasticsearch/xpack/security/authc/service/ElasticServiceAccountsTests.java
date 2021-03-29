/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

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

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ElasticServiceAccountsTests extends ESTestCase {

    public void testElasticFleetPrivileges() {
        final Role role = Role.builder(ElasticServiceAccounts.ACCOUNTS.get("elastic/fleet").roleDescriptor(), null).build();
        final Authentication authentication = mock(Authentication.class);
        assertThat(role.cluster().check(CreateApiKeyAction.NAME,
            new CreateApiKeyRequest(randomAlphaOfLengthBetween(3, 8), null, null), authentication), is(true));
        assertThat(role.cluster().check(GetApiKeyAction.NAME, GetApiKeyRequest.forOwnedApiKeys(), authentication), is(true));
        assertThat(role.cluster().check(InvalidateApiKeyAction.NAME, InvalidateApiKeyRequest.forOwnedApiKeys(), authentication), is(true));

        assertThat(role.cluster().check(GetApiKeyAction.NAME, randomFrom(GetApiKeyRequest.forAllApiKeys()), authentication), is(false));
        assertThat(role.cluster().check(InvalidateApiKeyAction.NAME,
            InvalidateApiKeyRequest.usingUserName(randomAlphaOfLengthBetween(3, 16)), authentication), is(false));

        // TODO: more tests when role descriptor is finalised for elastic/fleet
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
}
