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
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ManageOwnApiKeyClusterPrivilegeTests extends ESTestCase {

    public void testAuthenticationWithApiKeyAllowsAccessToApiKeyActionsWhenItIsOwner() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(AuthenticationField.API_KEY_ID_KEY, apiKeyId);
        metadata.put(AuthenticationField.API_KEY_NAME_KEY, randomAlphaOfLengthBetween(1, 16));
        final User userJoe = new User("joe");
        final Authentication authentication = createMockAuthentication(userJoe, "_es_api_key", AuthenticationType.API_KEY, metadata);
        final TransportRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean());
        final TransportRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean());

        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));
        assertFalse(clusterPermission.check("cluster:admin/something", mock(TransportRequest.class), authentication));
    }

    public void testAuthenticationWithApiKeyDeniesAccessToApiKeyActionsWhenItIsNotOwner() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
        final HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(AuthenticationField.API_KEY_ID_KEY, randomAlphaOfLength(7));
        metadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLengthBetween(1, 16));
        final User userJoe = new User("joe");
        final Authentication authentication = createMockAuthentication(userJoe, "_es_api_key", AuthenticationType.API_KEY, metadata);
        final TransportRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean());
        final TransportRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean());

        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));
    }

    public void testAuthenticationWithUserAllowsAccessToApiKeyActionsWhenItIsOwner() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final Authentication.RealmRef realmRef = AuthenticationTests.randomRealmRef(randomBoolean());
        final Authentication authentication = AuthenticationTests.randomAuthentication(new User("joe"), realmRef);

        TransportRequest getApiKeyRequest = GetApiKeyRequest.usingRealmAndUserName(realmRef.getName(), "joe");
        TransportRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName(realmRef.getName(), "joe");
        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));

        assertFalse(clusterPermission.check("cluster:admin/something", mock(TransportRequest.class), authentication));

        getApiKeyRequest = GetApiKeyRequest.usingRealmAndUserName(realmRef.getName(), "jane");
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName(realmRef.getName(), "jane");
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));

        RealmDomain realmDomain = realmRef.getDomain();
        final String otherRealmName;
        if (realmDomain != null) {
            for (RealmConfig.RealmIdentifier realmIdentifier : realmDomain.realms()) {
                getApiKeyRequest = GetApiKeyRequest.usingRealmAndUserName(realmIdentifier.getName(), "joe");
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
        getApiKeyRequest = GetApiKeyRequest.usingRealmAndUserName(otherRealmName, "joe");
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/get", getApiKeyRequest, authentication));
        invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName(otherRealmName, "joe");
        assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", invalidateApiKeyRequest, authentication));
    }

    public void testAuthenticationWithUserAllowsAccessToApiKeyActionsWhenItIsOwner_WithOwnerFlagOnly() {
        final ClusterPermission clusterPermission = ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder())
            .build();

        final boolean isRunAs = randomBoolean();
        final User userJoe = new User("joe");
        final Authentication authentication = createMockAuthentication(
            isRunAs ? new User(userJoe, new User("not-joe")) : userJoe,
            "realm1",
            isRunAs ? randomFrom(AuthenticationType.REALM, AuthenticationType.API_KEY) : AuthenticationType.REALM,
            Map.of()
        );
        final TransportRequest getApiKeyRequest = GetApiKeyRequest.forOwnedApiKeys();
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
        final Authentication authentication = createMockAuthentication(
            isRunAs ? new User(userJoe, new User("not-joe")) : userJoe,
            "realm1",
            isRunAs ? randomFrom(AuthenticationType.REALM, AuthenticationType.API_KEY) : AuthenticationType.REALM,
            Map.of()
        );
        final TransportRequest getApiKeyRequest = randomFrom(
            GetApiKeyRequest.usingRealmAndUserName("realm1", randomAlphaOfLength(7)),
            GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), "joe"),
            new GetApiKeyRequest(randomAlphaOfLength(5), randomAlphaOfLength(7), null, null, false)
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

        final Authentication authentication = createMockRunAsAuthentication(
            "user_a",
            "realm_a",
            "realm_a_type",
            "user_b",
            "realm_b",
            "realm_b_type"
        );

        assertTrue(
            clusterPermission.check(
                "cluster:admin/xpack/security/api_key/get",
                GetApiKeyRequest.usingRealmAndUserName("realm_b", "user_b"),
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

        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest();
        if (randomBoolean()) {
            queryApiKeyRequest.setFilterForCurrentUser();
        }
        assertThat(
            clusterPermission.check(QueryApiKeyAction.NAME, queryApiKeyRequest, mock(Authentication.class)),
            is(queryApiKeyRequest.isFilterForCurrentUser())
        );
    }

    private Authentication createMockAuthentication(
        User user,
        String realmName,
        AuthenticationType authenticationType,
        Map<String, Object> metadata
    ) {
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getSourceRealm()).thenReturn(authenticatedBy);
        when(authentication.getAuthenticationType()).thenReturn(authenticationType);
        when(authenticatedBy.getName()).thenReturn(realmName);
        when(authenticatedBy.getType()).thenReturn(realmName);
        when(authentication.getMetadata()).thenReturn(metadata);
        when(authentication.isAuthenticatedAsApiKey()).thenCallRealMethod();
        when(authentication.isApiKey()).thenCallRealMethod();
        return authentication;
    }

    private Authentication createMockRunAsAuthentication(
        String username,
        String realmName,
        String realmType,
        String runAsUsername,
        String runAsRealmName,
        String runAsRealmType
    ) {
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authenticatedBy.getName()).thenReturn(realmName);
        when(authenticatedBy.getType()).thenReturn(realmType);
        final Authentication.RealmRef lookedUpBy = mock(Authentication.RealmRef.class);
        when(lookedUpBy.getName()).thenReturn(runAsRealmName);
        when(lookedUpBy.getType()).thenReturn(runAsRealmType);
        final User user = new User(runAsUsername, new String[0], new User(username));
        final Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getSourceRealm()).thenReturn(lookedUpBy);
        when(authentication.getMetadata()).thenReturn(Map.of());
        when(authentication.getAuthenticationType()).thenReturn(AuthenticationType.REALM);
        return authentication;
    }
}
