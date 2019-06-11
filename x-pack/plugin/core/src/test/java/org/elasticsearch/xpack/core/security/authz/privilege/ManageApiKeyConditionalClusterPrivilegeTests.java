/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ManageApiKeyConditionalClusterPrivilegeTests extends ESTestCase {
    private static final String CREATE_ACTION = "cluster:admin/xpack/security/api_key/create";
    private static final String GET_ACTION = "cluster:admin/xpack/security/api_key/get";
    private static final String INVALIDATE_ACTION = "cluster:admin/xpack/security/api_key/invalidate";

    private User user;
    private Authentication authentication = mock(Authentication.class);
    private Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);

    @Before
    public void setup() {
        user = new User("user1");
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getName()).thenReturn("realm1");
        when(authenticatedBy.getType()).thenReturn("kerberos");
    }

    public void testManageAllPrivilege() {
        final ManageApiKeyConditionalClusterPrivilege condPrivilege = ManageApiKeyConditionalPrivilegesBuilder.manageApiKeysUnrestricted();

        boolean accessAllowed = checkAccess(condPrivilege, CREATE_ACTION, new CreateApiKeyRequest(), authentication);
        assertThat(accessAllowed, is(true));

        accessAllowed = checkAccess(condPrivilege, GET_ACTION,
                GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), randomAlphaOfLength(5)), authentication);
        assertThat(accessAllowed, is(true));

        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION,
                InvalidateApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), randomAlphaOfLength(5)), authentication);
        assertThat(accessAllowed, is(true));

        // When request does not have user or realm name and conditional api key privileges for manage is used then it should be denied.
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5)), authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5)),
                authentication);
        assertThat(accessAllowed, is(true));
    }

    public void testManagePrivilegeOwnerOnly() {
        final ManageApiKeyConditionalClusterPrivilege condPrivilege = ManageApiKeyConditionalPrivilegesBuilder.manageApiKeysOnlyForOwner();

        boolean accessAllowed = checkAccess(condPrivilege, CREATE_ACTION, new CreateApiKeyRequest(), authentication);
        assertThat(accessAllowed, is(true));

        // Username and realm name is always required to evaluate condition if authenticated by user
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName("realm1", "user1"), authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName("realm1", randomAlphaOfLength(4)),
                authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(4), "user1"),
                authentication);
        assertThat(accessAllowed, is(false));

        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingRealmAndUserName("realm1", "user1"),
                authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION,
                InvalidateApiKeyRequest.usingRealmAndUserName("realm2", randomAlphaOfLength(4)), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyName("api-key-name"), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyName("api-key-name"),
                authentication);
        assertThat(accessAllowed, is(false));

        // API key id is always required to evaluate condition if authenticated by API key id
        when(authenticatedBy.getName()).thenReturn("_es_api_key");
        when(authenticatedBy.getType()).thenReturn("_es_api_key");
        when(authentication.getMetadata()).thenReturn(Map.of("_security_api_key_id", "user1-api-key-id"));

        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyId("user1-api-key-id"), authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5)), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyId("user1-api-key-id"),
                authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5)),
                authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyName("api-key-name"), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyName("api-key-name"),
                authentication);
        assertThat(accessAllowed, is(false));
    }

    private boolean checkAccess(ManageApiKeyConditionalClusterPrivilege privilege, String action, TransportRequest request,
            Authentication authentication) {
        return privilege.getPrivilege().predicate().test(action) && privilege.getRequestPredicate().test(request, authentication);
    }

    public static class ManageApiKeyConditionalPrivilegesBuilder {
        private Set<String> actions = new HashSet<>();
        private boolean restrictActionsToAuthenticatedUser;

        public ManageApiKeyConditionalPrivilegesBuilder allowCreate() {
            actions.add(CREATE_ACTION);
            return this;
        }

        public ManageApiKeyConditionalPrivilegesBuilder allowGet() {
            actions.add(GET_ACTION);
            return this;
        }

        public ManageApiKeyConditionalPrivilegesBuilder restrictActionsToAuthenticatedUser() {
            this.restrictActionsToAuthenticatedUser = true;
            return this;
        }

        public static ManageApiKeyConditionalPrivilegesBuilder builder() {
            return new ManageApiKeyConditionalPrivilegesBuilder();
        }

        public static ManageApiKeyConditionalClusterPrivilege manageApiKeysUnrestricted() {
            return new ManageApiKeyConditionalClusterPrivilege(Set.of("cluster:admin/xpack/security/api_key/*"), false);
        }

        public static ManageApiKeyConditionalClusterPrivilege manageApiKeysOnlyForOwner() {
            return (ManageApiKeyConditionalClusterPrivilege) DefaultConditionalClusterPrivilege.MANAGE_OWN_API_KEY
                    .conditionalClusterPrivilege();
        }

        public ManageApiKeyConditionalClusterPrivilege build() {
            return new ManageApiKeyConditionalClusterPrivilege(actions, restrictActionsToAuthenticatedUser);
        }
    }
}
