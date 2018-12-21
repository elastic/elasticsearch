/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.junit.Before;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class RBACEngineTests extends ESTestCase {

    private RBACEngine engine;

    @Before
    public void createEngine() {
        engine = new RBACEngine(Settings.EMPTY, mock(CompositeRolesStore.class));
    }

    public void testSameUserPermission() {
        final User user = new User("joe");
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ?
            new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request() :
            new AuthenticateRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));

        assertThat(request, instanceOf(UserRequest.class));
        assertTrue(engine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowNonMatchingUsername() {
        final User authUser = new User("admin", new String[]{"bar"});
        final User user = new User("joe", null, authUser);
        final boolean changePasswordRequest = randomBoolean();
        final String username = randomFrom("", "joe" + randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(3, 10));
        final TransportRequest request = changePasswordRequest ?
            new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() :
            new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));

        when(authentication.getUser()).thenReturn(user);
        final Authentication.RealmRef lookedUpBy = mock(Authentication.RealmRef.class);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));
        // this should still fail since the username is still different
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));

        if (request instanceof ChangePasswordRequest) {
            ((ChangePasswordRequest) request).username("joe");
        } else {
            ((AuthenticateRequest) request).username("joe");
        }
        assertTrue(engine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowOtherActions() {
        final User user = mock(User.class);
        final TransportRequest request = mock(TransportRequest.class);
        final String action = randomFrom(PutUserAction.NAME, DeleteUserAction.NAME, ClusterHealthAction.NAME, ClusterStateAction.NAME,
            ClusterStatsAction.NAME, GetLicenseAction.NAME);
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        final boolean runAs = randomBoolean();
        when(authentication.getUser()).thenReturn(user);
        when(user.authenticatedUser()).thenReturn(runAs ? new User("authUser") : user);
        when(user.isRunAs()).thenReturn(runAs);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
            .thenReturn(randomAlphaOfLengthBetween(4, 12));

        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
        verifyZeroInteractions(user, request, authentication);
    }

    public void testSameUserPermissionRunAsChecksAuthenticatedBy() {
        final User authUser = new User("admin", new String[]{"bar"});
        final String username = "joe";
        final User user = new User(username, null, authUser);
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ?
            new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() :
            new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        final Authentication.RealmRef lookedUpBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));
        assertTrue(engine.checkSameUserPermissions(action, request, authentication));

        when(authentication.getUser()).thenReturn(authUser);
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForOtherRealms() {
        final User user = new User("joe");
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType()).thenReturn(randomFrom(LdapRealmSettings.LDAP_TYPE, FileRealmSettings.TYPE,
            LdapRealmSettings.AD_TYPE, PkiRealmSettings.TYPE,
            randomAlphaOfLengthBetween(4, 12)));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
        verify(authenticatedBy).getType();
        verify(authentication).getAuthenticatedBy();
        verify(authentication, times(2)).getUser();
        verifyNoMoreInteractions(authenticatedBy, authentication);
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForLookedUpByOtherRealms() {
        final User authUser = new User("admin", new String[]{"bar"});
        final User user = new User("joe", null, authUser);
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);
        final Authentication.RealmRef lookedUpBy = mock(Authentication.RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType()).thenReturn(randomFrom(LdapRealmSettings.LDAP_TYPE, FileRealmSettings.TYPE,
            LdapRealmSettings.AD_TYPE, PkiRealmSettings.TYPE,
            randomAlphaOfLengthBetween(4, 12)));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(engine.checkSameUserPermissions(action, request, authentication));
        verify(authentication).getLookedUpBy();
        verify(authentication, times(2)).getUser();
        verify(lookedUpBy).getType();
        verifyNoMoreInteractions(authentication, lookedUpBy, authenticatedBy);
    }
}
