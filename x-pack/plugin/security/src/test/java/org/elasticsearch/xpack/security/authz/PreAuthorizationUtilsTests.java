/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationContext;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.ParentActionAuthorization;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.RBACEngine.RBACAuthorizationInfo;

import java.util.Optional;

import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.elasticsearch.xpack.security.authz.PreAuthorizationUtils.maybeSkipChildrenActionAuthorization;
import static org.elasticsearch.xpack.security.authz.PreAuthorizationUtils.shouldRemoveParentAuthorizationFromThreadContext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link PreAuthorizationUtils}.
 */
public class PreAuthorizationUtilsTests extends ESTestCase {

    public void testMaybeSkipChildrenActionAuthorizationAddsParentAuthorizationHeader() {
        String action = SearchAction.NAME;

        Role role = Role.builder(RESTRICTED_INDICES, "test-role").add(IndexPrivilege.READ, "test-*").build();

        AuthorizationContext parentAuthorizationContext = createAuthorizationContext(action, role, IndicesAccessControl.allowAll());
        SecurityContext securityContext = new SecurityContext(Settings.EMPTY, new ThreadContext(Settings.EMPTY));

        maybeSkipChildrenActionAuthorization(securityContext, parentAuthorizationContext);
        assertThat(securityContext.getParentAuthorization(), notNullValue());
        assertThat(securityContext.getParentAuthorization().action(), equalTo(action));
    }

    public void testMaybeSkipChildrenActionAuthorizationDoesNotAddHeaderForRandomAction() {
        String action = "indices:data/" + randomAlphaOfLengthBetween(3, 8);

        Role role = Role.builder(RESTRICTED_INDICES, "test-role").add(IndexPrivilege.READ, "test-*").build();

        AuthorizationContext parentAuthorizationContext = createAuthorizationContext(action, role, IndicesAccessControl.allowAll());
        SecurityContext securityContext = new SecurityContext(Settings.EMPTY, new ThreadContext(Settings.EMPTY));

        maybeSkipChildrenActionAuthorization(securityContext, parentAuthorizationContext);
        assertThat(securityContext.getParentAuthorization(), nullValue());
    }

    public void testShouldRemoveParentAuthorizationFromThreadContext() {
        final String parentAction = SearchAction.NAME;
        SecurityContext securityContextWithParentAuthorization = new SecurityContext(Settings.EMPTY, new ThreadContext(Settings.EMPTY));
        securityContextWithParentAuthorization.setParentAuthorization(new ParentActionAuthorization(parentAction));

        // We should not remove the parent authorization when child action is white-listed
        assertThat(
            shouldRemoveParentAuthorizationFromThreadContext(
                Optional.empty(),
                randomWhitelistedChildAction(parentAction),
                securityContextWithParentAuthorization
            ),
            equalTo(false)
        );

        // We should not remove when there is nothing to be removed
        assertThat(
            shouldRemoveParentAuthorizationFromThreadContext(
                Optional.ofNullable(randomBoolean() ? "my_remote_cluster" : null),
                randomWhitelistedChildAction(parentAction),
                new SecurityContext(Settings.EMPTY, new ThreadContext(Settings.EMPTY))
            ),
            equalTo(false)
        );

        // Even-though the child action is white-listed for the parent action,
        // we expect to remove parent authorization when targeting remote cluster
        assertThat(
            shouldRemoveParentAuthorizationFromThreadContext(
                Optional.of("my_remote_cluster"),
                randomWhitelistedChildAction(parentAction),
                securityContextWithParentAuthorization
            ),
            equalTo(true)
        );

        // The parent authorization should be removed in either case:
        // - we are sending a transport request to a remote cluster
        // - or the child action is not white-listed for the parent
        assertThat(
            shouldRemoveParentAuthorizationFromThreadContext(
                Optional.ofNullable(randomBoolean() ? "my_remote_cluster" : null),
                randomAlphaOfLengthBetween(3, 8),
                securityContextWithParentAuthorization
            ),
            equalTo(true)
        );
    }

    public void testShouldPreAuthorizeChildByParentAction() {
        final String parentAction = SearchAction.NAME;
        final String childAction = randomWhitelistedChildAction(parentAction);

        ParentActionAuthorization parentAuthorization = new ParentActionAuthorization(parentAction);
        Authentication authentication = Authentication.newRealmAuthentication(
            new User("username1", "role1"),
            new RealmRef("realm1", "native", "node1")
        );
        RequestInfo requestInfo = new RequestInfo(authentication, new SearchRequest("test-index"), childAction, null, parentAuthorization);

        Role role = Role.builder(RESTRICTED_INDICES, "role1").add(IndexPrivilege.READ, "test-*").build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        assertThat(PreAuthorizationUtils.shouldPreAuthorizeChildByParentAction(requestInfo, authzInfo), equalTo(true));
    }

    public void testShouldPreAuthorizeChildByParentActionWhenParentAndChildAreSame() {
        final String parentAction = SearchAction.NAME;
        final String childAction = parentAction;

        ParentActionAuthorization parentAuthorization = new ParentActionAuthorization(parentAction);
        Authentication authentication = Authentication.newRealmAuthentication(
            new User("username1", "role1"),
            new RealmRef("realm1", "native", "node1")
        );
        RequestInfo requestInfo = new RequestInfo(authentication, new SearchRequest("test-index"), childAction, null, parentAuthorization);

        Role role = Role.builder(RESTRICTED_INDICES, "role1").add(IndexPrivilege.READ, "test-*").build();
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);

        assertThat(PreAuthorizationUtils.shouldPreAuthorizeChildByParentAction(requestInfo, authzInfo), equalTo(false));
    }

    private String randomWhitelistedChildAction(String parentAction) {
        return randomFrom(PreAuthorizationUtils.CHILD_ACTIONS_PRE_AUTHORIZED_BY_PARENT.get(parentAction));
    }

    private AuthorizationContext createAuthorizationContext(String action, Role role, IndicesAccessControl accessControl) {
        RBACAuthorizationInfo authzInfo = new RBACAuthorizationInfo(role, null);
        return new AuthorizationContext(action, authzInfo, accessControl);
    }

}
