/*
 *
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ManageOwnApiKeyClusterPrivilegeTests extends ESTestCase {

    public void testActionRequestAuthenticationBasedPredicateWhenAuthenticatingWithApiKey() {
        final ClusterPermission clusterPermission =
            ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder()).build();
        {
            final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
            final Authentication authentication = createMockAuthentication("_es_api_key", "_es_api_key",
                Map.of("_security_api_key_id", apiKeyId));
            final TransportRequest request = randomFrom(GetApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean()),
                InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean()));

            assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/get", request, authentication));
            assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", request, authentication));
            assertFalse(clusterPermission.check("cluster:admin/something", request, authentication));
        }
        {
            final String apiKeyId = randomAlphaOfLengthBetween(4, 7);
            final Authentication authentication = createMockAuthentication("_es_api_key", "_es_api_key",
                Map.of("_security_api_key_id", randomAlphaOfLength(7)));
            final TransportRequest request = randomFrom(GetApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean()),
                InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, randomBoolean()));

            assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/get", request, authentication));
            assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", request, authentication));
        }
    }

    public void testActionRequestAuthenticationBasedPredicateWhenRequestContainsUsernameAndRealmName() {
        final ClusterPermission clusterPermission =
            ManageOwnApiKeyClusterPrivilege.INSTANCE.buildPermission(ClusterPermission.builder()).build();
        {
            final Authentication authentication = createMockAuthentication("realm1", "native", Map.of());
            final TransportRequest request = randomFrom(GetApiKeyRequest.usingRealmAndUserName("realm1", "joe"),
                InvalidateApiKeyRequest.usingRealmAndUserName("realm1", "joe"));

            assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/get", request, authentication));
            assertTrue(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", request, authentication));
            assertFalse(clusterPermission.check("cluster:admin/something", request, authentication));
        }
        {
            final Authentication authentication = createMockAuthentication("realm1", "native", Map.of());
            final TransportRequest request = randomFrom(
                GetApiKeyRequest.usingRealmAndUserName("realm1", randomAlphaOfLength(7)),
                GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), "joe"),
                InvalidateApiKeyRequest.usingRealmAndUserName("realm1", randomAlphaOfLength(7)),
                InvalidateApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), "joe"));

            assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/get", request, authentication));
            assertFalse(clusterPermission.check("cluster:admin/xpack/security/api_key/invalidate", request, authentication));
        }
    }

    private Authentication createMockAuthentication(String realmName, String realmType, Map<String, Object> metadata) {
        final User user = new User("joe");
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getName()).thenReturn(realmName);
        when(authenticatedBy.getType()).thenReturn(realmType);
        when(authentication.getMetadata()).thenReturn(metadata);
        return authentication;
    }
}
