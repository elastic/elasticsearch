/*
 *
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ManageOwnApiKeyClusterPrivilegeTests extends ESTestCase {

    public void testAuthenticationWithApiKeyAllowsAccessToApiKeyActionsWhenItIsItself() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final User userJoe = new User("joe");
        final Authentication authentication = AuthenticationTests.randomApiKeyAuthentication(userJoe, apiKeyId);
        final TransportRequest getApiKeyRequest = GetApiKeyRequest.builder()
            .apiKeyId(apiKeyId)
            .ownedByAuthenticatedUser(randomBoolean())
            .build();
        final TransportRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean());
        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));
        assertFalse(clusterPermission.check("cluster:admin/something", mock(TransportRequest.class), authentication));
    }

    public void testAuthenticationWithApiKeyAllowsDeniesGetApiKeyWithLimitedByWhenItIsItself() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();
        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final User userJoe = new User("joe");
        final Authentication authentication = AuthenticationTests.randomApiKeyAuthentication(userJoe, apiKeyId);
        assertFalse(
            clusterPermission.check(
                "cluster:admin/xpack/security/api_key/get",
                GetApiKeyRequest.builder().apiKeyId(apiKeyId).withLimitedBy().build(),
                authentication
            )
        );
    }

    public void testAuthenticationForUpdateApiKeyAllowsAll() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();
        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        final TransportRequest updateApiKeyRequest = UpdateApiKeyRequest.usingApiKeyId(apiKeyId);

        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/update", updateApiKeyRequest, authentication));
    }

    public void testAuthenticationForBulkUpdateApiKeyAllowsAll() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();
        final List<String> apiKeyIds = randomList(1, 5, () -> randomAlphaOfLengthBetween(4, 7));
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        final TransportRequest bulkUpdateApiKeyRequest = new BulkUpdateApiKeyRequest(apiKeyIds, null, null);

        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/update", bulkUpdateApiKeyRequest, authentication));
    }

    public void testAuthenticationWithApiKeyDeniesAccessToApiKeyActionsWhenItIsNotOwner() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final User userJoe = new User("joe");
        final Authentication authentication = AuthenticationTests.randomApiKeyAuthentication(userJoe, randomAlphaOfLength(20));
        final TransportRequest getApiKeyRequest = GetApiKeyRequest.builder()
            .apiKeyId(apiKeyId)
            .ownedByAuthenticatedUser(randomBoolean())
            .withLimitedBy(randomBoolean())
            .build();
        final TransportRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean());

        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));
    }

    public void testAuthenticationWithUserAllowsAccessToApiKeyActionsWhenItIsOwner() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final Authentication.RealmRef realmRef = AuthenticationTests.randomRealmRef(randomBoolean());
        final Authentication authentication = AuthenticationTests.randomAuthentication(new User("joe"), realmRef);

        TransportRequest getApiKeyRequest = GetApiKeyRequest.builder()
            .realmName(realmRef.getName())
            .userName("joe")
            .withLimitedBy(randomBoolean())
            .build();
        TransportRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName(realmRef.getName(), "joe");
        TransportRequest updateApiKeyRequest = UpdateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(10));
        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));
        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/update", updateApiKeyRequest, authentication));

        assertFalse(clusterPermission.check("cluster:admin/something", mock(TransportRequest.class), authentication));

        getApiKeyRequest = GetApiKeyRequest.builder().realmName(realmRef.getName()).userName("jane").withLimitedBy(randomBoolean()).build();
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName(realmRef.getName(), "jane");
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));

        RealmDomain realmDomain = realmRef.getDomain();
        final String otherRealmName;
        if (realmDomain != null) {
            for (RealmConfig.RealmIdentifier realmIdentifier : realmDomain.realms()) {
                getApiKeyRequest = GetApiKeyRequest.builder()
                    .realmName(realmIdentifier.getName())
                    .userName("joe")
                    .withLimitedBy(randomBoolean())
                    .build();
                assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
                invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName(realmIdentifier.getName(), "joe");
                assertTrue(
                    clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication)
                );
            }
            otherRealmName = randomValueOtherThanMany(
                realmName -> realmDomain.realms().stream().map(ri -> ri.getName()).anyMatch(realmName::equals),
                () -> randomAlphaOfLengthBetween(2, 10)
            );
        } else {
            otherRealmName = randomValueOtherThan(realmRef.getName(), () -> randomAlphaOfLengthBetween(2, 10));
        }
        getApiKeyRequest = GetApiKeyRequest.builder().realmName(otherRealmName).userName("joe").withLimitedBy(randomBoolean()).build();
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName(otherRealmName, "joe");
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));
    }

    public void testAuthenticationWithUserAllowsAccessToApiKeyActionsWhenItIsOwner_WithOwnerFlagOnly() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final boolean isRunAs = randomBoolean();
        final User userJoe = new User("joe");
        final Authentication.RealmRef realmRef = new Authentication.RealmRef("realm1", "realm1", randomAlphaOfLengthBetween(3, 8));
        final Authentication authentication;
        if (isRunAs) {
            final User runByUser = new User("not-joe");
            if (randomBoolean()) {
                authentication = Authentication.newRealmAuthentication(runByUser, realmRef).runAs(userJoe, realmRef);
            } else {
                authentication = AuthenticationTests.randomApiKeyAuthentication(runByUser, randomAlphaOfLength(20))
                    .runAs(userJoe, realmRef);
            }
        } else {
            authentication = AuthenticationTests.randomAuthentication(userJoe, realmRef);
        }

        final TransportRequest getApiKeyRequest = GetApiKeyRequest.builder()
            .ownedByAuthenticatedUser()
            .withLimitedBy(randomBoolean())
            .build();
        final TransportRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.forOwnedApiKeys();

        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));
        assertFalse(clusterPermission.check("cluster:admin/something", mock(TransportRequest.class), authentication));
    }

    public void testAuthenticationWithUserDeniesAccessToApiKeyActionsWhenItIsNotOwner() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final boolean isRunAs = randomBoolean();
        final User userJoe = new User("joe");
        final Authentication.RealmRef realmRef = new Authentication.RealmRef("realm1", "realm1", randomAlphaOfLengthBetween(3, 8));
        final Authentication authentication;
        if (isRunAs) {
            final User runByUser = new User("not-joe");
            if (randomBoolean()) {
                authentication = Authentication.newRealmAuthentication(runByUser, realmRef).runAs(userJoe, realmRef);
            } else {
                authentication = AuthenticationTests.randomApiKeyAuthentication(runByUser, randomAlphaOfLength(20))
                    .runAs(userJoe, realmRef);
            }
        } else {
            authentication = AuthenticationTests.randomAuthentication(userJoe, realmRef);
        }

        final TransportRequest getApiKeyRequest = randomFrom(
            GetApiKeyRequest.builder().realmName("realm1").userName(randomAlphaOfLength(7)).withLimitedBy(randomBoolean()).build(),
            GetApiKeyRequest.builder().realmName(randomAlphaOfLength(5)).userName("joe").withLimitedBy(randomBoolean()).build(),
            GetApiKeyRequest.builder().realmName(randomAlphaOfLength(5)).userName(randomAlphaOfLength(7)).build()
        );
        final TransportRequest invalidateApiKeyRequest = randomFrom(
            InvalidateApiKeyRequest.usingRealmAndUserName("realm1", randomAlphaOfLength(7)),
            InvalidateApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), "joe"),
            new InvalidateApiKeyRequest(randomAlphaOfLength(5), randomAlphaOfLength(7), null, false, null)
        );

        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));
    }

    public void testGetAndInvalidateApiKeyWillRespectRunAsUser() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final Authentication authentication = Authentication.newRealmAuthentication(
            new User("user_a"),
            new Authentication.RealmRef("realm_a", "realm_a_type", randomAlphaOfLengthBetween(3, 8))
        ).runAs(new User("user_b"), new Authentication.RealmRef("realm_b", "realm_b_type", randomAlphaOfLengthBetween(3, 8)));

        assertTrue(
            clusterPermission.check(
                "cluster:admin/xpack/security/api_key/get",
                GetApiKeyRequest.builder().realmName("realm_b").userName("user_b").withLimitedBy(randomBoolean()).build(),
                authentication
            )
        );
        assertTrue(
            clusterPermission.check(
                "cluster:admin/xpack/security/api_key/invalidate",
                InvalidateApiKeyRequest.usingRealmAndUserName("realm_b", "user_b"),
                authentication
            )
        );
    }

    public void testCheckQueryApiKeyRequest() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest(null, null, null, null, null, randomBoolean());
        if (randomBoolean()) {
            queryApiKeyRequest.setFilterForCurrentUser();
        }
        assertThat(
            clusterPermission.check(
                QueryApiKeyAction.NAME,
                queryApiKeyRequest,
                randomValueOtherThanMany(Authentication::isApiKey, () -> AuthenticationTestHelper.builder().build())
            ),
            is(queryApiKeyRequest.isFilterForCurrentUser())
        );
    }

    public void testAuthenticationWithApiKeyAllowsDeniesQueryApiKeyWithLimitedBy() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final boolean withLimitedBy = randomBoolean();
        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest(null, null, null, null, null, withLimitedBy);
        queryApiKeyRequest.setFilterForCurrentUser();
        assertThat(
            clusterPermission.check(QueryApiKeyAction.NAME, queryApiKeyRequest, AuthenticationTestHelper.builder().apiKey().build(false)),
            is(false == queryApiKeyRequest.withLimitedBy())
        );
    }

    public void testCheckGrantApiKeyRequestDenied() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
        grantApiKeyRequest.setApiKeyRequest(new CreateApiKeyRequest());

        assertFalse(clusterPermission.check(GrantApiKeyAction.NAME, grantApiKeyRequest, AuthenticationTestHelper.builder().build()));
    }
}
