/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequest.Empty;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;

import java.util.Collections;

import static org.elasticsearch.mock.orig.Mockito.verifyNoMoreInteractions;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.security.authz.AuthorizationService.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.security.authz.AuthorizationService.ORIGINATING_ACTION_KEY;
import static org.elasticsearch.xpack.security.authz.AuthorizationServiceTests.authzInfoRoles;
import static org.elasticsearch.xpack.security.authz.SecuritySearchOperationListener.ensureAuthenticatedUserIsSame;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SecuritySearchOperationListenerTests extends ESTestCase {

    public void testUnlicensed() {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthAllowed()).thenReturn(false);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        AuditTrailService auditTrailService = mock(AuditTrailService.class);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.scrollContext()).thenReturn(new ScrollContext());

        SecuritySearchOperationListener listener = new SecuritySearchOperationListener(securityContext, licenseState, auditTrailService);
        listener.onNewScrollContext(searchContext);
        listener.validateSearchContext(searchContext, Empty.INSTANCE);
        verify(licenseState, times(2)).isAuthAllowed();
        verifyZeroInteractions(auditTrailService, searchContext);
    }

    public void testOnNewContextSetsAuthentication() throws Exception {
        TestScrollSearchContext testSearchContext = new TestScrollSearchContext();
        testSearchContext.scrollContext(new ScrollContext());
        final Scroll scroll = new Scroll(TimeValue.timeValueSeconds(2L));
        testSearchContext.scrollContext().scroll = scroll;
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        AuditTrailService auditTrailService = mock(AuditTrailService.class);
        Authentication authentication = new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
        authentication.writeToContext(threadContext);

        SecuritySearchOperationListener listener = new SecuritySearchOperationListener(securityContext, licenseState, auditTrailService);
        listener.onNewScrollContext(testSearchContext);

        Authentication contextAuth = testSearchContext.scrollContext().getFromContext(AuthenticationField.AUTHENTICATION_KEY);
        assertEquals(authentication, contextAuth);
        assertEquals(scroll, testSearchContext.scrollContext().scroll);

        verify(licenseState).isAuthAllowed();
        verifyZeroInteractions(auditTrailService);
    }

    public void testValidateSearchContext() throws Exception {
        TestScrollSearchContext testSearchContext = new TestScrollSearchContext();
        testSearchContext.scrollContext(new ScrollContext());
        testSearchContext.scrollContext().putInContext(AuthenticationField.AUTHENTICATION_KEY,
                new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null));
        testSearchContext.scrollContext().scroll = new Scroll(TimeValue.timeValueSeconds(2L));
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        AuditTrailService auditTrailService = mock(AuditTrailService.class);

        SecuritySearchOperationListener listener = new SecuritySearchOperationListener(securityContext, licenseState, auditTrailService);
        try (StoredContext ignore = threadContext.newStoredContext(false)) {
            Authentication authentication = new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
            authentication.writeToContext(threadContext);
            listener.validateSearchContext(testSearchContext, Empty.INSTANCE);
            verify(licenseState).isAuthAllowed();
            verifyZeroInteractions(auditTrailService);
        }

        try (StoredContext ignore = threadContext.newStoredContext(false)) {
            final String nodeName = randomAlphaOfLengthBetween(1, 8);
            final String realmName = randomAlphaOfLengthBetween(1, 16);
            Authentication authentication = new Authentication(new User("test", "role"), new RealmRef(realmName, "file", nodeName), null);
            authentication.writeToContext(threadContext);
            listener.validateSearchContext(testSearchContext, Empty.INSTANCE);
            verify(licenseState, times(2)).isAuthAllowed();
            verifyZeroInteractions(auditTrailService);
        }

        try (StoredContext ignore = threadContext.newStoredContext(false)) {
            final String nodeName = randomBoolean() ? "node" : randomAlphaOfLengthBetween(1, 8);
            final String realmName = randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16);
            final String type = randomAlphaOfLengthBetween(5, 16);
            Authentication authentication = new Authentication(new User("test", "role"), new RealmRef(realmName, type, nodeName), null);
            authentication.writeToContext(threadContext);
            threadContext.putTransient(ORIGINATING_ACTION_KEY, "action");
            threadContext.putTransient(AUTHORIZATION_INFO_KEY,
                (AuthorizationInfo) () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, authentication.getUser().roles()));
            final InternalScrollSearchRequest request = new InternalScrollSearchRequest();
            SearchContextMissingException expected =
                    expectThrows(SearchContextMissingException.class, () -> listener.validateSearchContext(testSearchContext, request));
            assertEquals(testSearchContext.id(), expected.id());
            verify(licenseState, times(3)).isAuthAllowed();
            verify(auditTrailService).accessDenied(eq(null), eq(authentication), eq("action"), eq(request),
                authzInfoRoles(authentication.getUser().roles()));
        }

        // another user running as the original user
        try (StoredContext ignore = threadContext.newStoredContext(false)) {
            final String nodeName = randomBoolean() ? "node" : randomAlphaOfLengthBetween(1, 8);
            final String realmName = randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16);
            final String type = randomAlphaOfLengthBetween(5, 16);
            User user = new User(new User("test", "role"), new User("authenticated", "runas"));
            Authentication authentication = new Authentication(user, new RealmRef(realmName, type, nodeName),
                    new RealmRef(randomAlphaOfLengthBetween(1, 16), "file", nodeName));
            authentication.writeToContext(threadContext);
            threadContext.putTransient(ORIGINATING_ACTION_KEY, "action");
            final InternalScrollSearchRequest request = new InternalScrollSearchRequest();
            listener.validateSearchContext(testSearchContext, request);
            verify(licenseState, times(4)).isAuthAllowed();
            verifyNoMoreInteractions(auditTrailService);
        }

        // the user that authenticated for the run as request
        try (StoredContext ignore = threadContext.newStoredContext(false)) {
            final String nodeName = randomBoolean() ? "node" : randomAlphaOfLengthBetween(1, 8);
            final String realmName = randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16);
            final String type = randomAlphaOfLengthBetween(5, 16);
            Authentication authentication =
                    new Authentication(new User("authenticated", "runas"), new RealmRef(realmName, type, nodeName), null);
            authentication.writeToContext(threadContext);
            threadContext.putTransient(ORIGINATING_ACTION_KEY, "action");
            threadContext.putTransient(AUTHORIZATION_INFO_KEY,
                (AuthorizationInfo) () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, authentication.getUser().roles()));
            final InternalScrollSearchRequest request = new InternalScrollSearchRequest();
            SearchContextMissingException expected =
                    expectThrows(SearchContextMissingException.class, () -> listener.validateSearchContext(testSearchContext, request));
            assertEquals(testSearchContext.id(), expected.id());
            verify(licenseState, times(5)).isAuthAllowed();
            verify(auditTrailService).accessDenied(eq(null), eq(authentication), eq("action"), eq(request),
                authzInfoRoles(authentication.getUser().roles()));
        }
    }

    public void testEnsuredAuthenticatedUserIsSame() {
        Authentication original = new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
        Authentication current =
                randomBoolean() ? original : new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
        long id = randomLong();
        final String action = randomAlphaOfLength(4);
        TransportRequest request = Empty.INSTANCE;
        AuditTrailService auditTrail = mock(AuditTrailService.class);

        final String auditId = randomAlphaOfLengthBetween(8, 20);
        ensureAuthenticatedUserIsSame(original, current, auditTrail, id, action, request, auditId,
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles()));
        verifyZeroInteractions(auditTrail);

        // original user being run as
        User user = new User(new User("test", "role"), new User("authenticated", "runas"));
        current = new Authentication(user, new RealmRef("realm", "file", "node"),
                new RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"));
        ensureAuthenticatedUserIsSame(original, current, auditTrail, id, action, request, auditId,
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles()));
        verifyZeroInteractions(auditTrail);

        // both user are run as
        current = new Authentication(user, new RealmRef("realm", "file", "node"),
                new RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"));
        Authentication runAs = current;
        ensureAuthenticatedUserIsSame(runAs, current, auditTrail, id, action, request, auditId,
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles()));
        verifyZeroInteractions(auditTrail);

        // different authenticated by type
        Authentication differentRealmType =
                new Authentication(new User("test", "role"), new RealmRef("realm", randomAlphaOfLength(5), "node"), null);
        SearchContextMissingException e = expectThrows(SearchContextMissingException.class,
                () -> ensureAuthenticatedUserIsSame(original, differentRealmType, auditTrail, id, action, request, auditId,
                    () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles())));
        assertEquals(id, e.id());
        verify(auditTrail).accessDenied(eq(auditId), eq(differentRealmType), eq(action), eq(request),
            authzInfoRoles(original.getUser().roles()));

        // wrong user
        Authentication differentUser =
                new Authentication(new User("test2", "role"), new RealmRef("realm", "realm", "node"), null);
        e = expectThrows(SearchContextMissingException.class,
                () -> ensureAuthenticatedUserIsSame(original, differentUser, auditTrail, id, action, request, auditId,
                    () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles())));
        assertEquals(id, e.id());
        verify(auditTrail).accessDenied(eq(auditId), eq(differentUser), eq(action), eq(request),
            authzInfoRoles(original.getUser().roles()));

        // run as different user
        Authentication diffRunAs = new Authentication(new User(new User("test2", "role"), new User("authenticated", "runas")),
                new RealmRef("realm", "file", "node1"), new RealmRef("realm", "file", "node1"));
        e = expectThrows(SearchContextMissingException.class,
                () -> ensureAuthenticatedUserIsSame(original, diffRunAs, auditTrail, id, action, request, auditId,
                    () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles())));
        assertEquals(id, e.id());
        verify(auditTrail).accessDenied(eq(auditId), eq(diffRunAs), eq(action), eq(request), authzInfoRoles(original.getUser().roles()));

        // run as different looked up by type
        Authentication runAsDiffType = new Authentication(user, new RealmRef("realm", "file", "node"),
                new RealmRef(randomAlphaOfLengthBetween(1, 16), randomAlphaOfLengthBetween(5, 12), "node"));
        e = expectThrows(SearchContextMissingException.class,
                () -> ensureAuthenticatedUserIsSame(runAs, runAsDiffType, auditTrail, id, action, request, auditId,
                    () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles())));
        assertEquals(id, e.id());
        verify(auditTrail).accessDenied(eq(auditId), eq(runAsDiffType), eq(action), eq(request),
            authzInfoRoles(original.getUser().roles()));
    }

    static class TestScrollSearchContext extends TestSearchContext {

        private ScrollContext scrollContext;

        TestScrollSearchContext() {
            super(null);
        }

        @Override
        public ScrollContext scrollContext() {
            return scrollContext;
        }

        @Override
        public SearchContext scrollContext(ScrollContext scrollContext) {
            this.scrollContext = scrollContext;
            return this;
        }
    }
}
