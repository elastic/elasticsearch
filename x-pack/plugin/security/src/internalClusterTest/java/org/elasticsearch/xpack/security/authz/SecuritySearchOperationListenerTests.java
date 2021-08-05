/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequest.Empty;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;

import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ORIGINATING_ACTION_KEY;
import static org.elasticsearch.xpack.security.authz.AuthorizationServiceTests.authzInfoRoles;
import static org.elasticsearch.xpack.security.authz.SecuritySearchOperationListener.ensureAuthenticatedUserIsSame;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SecuritySearchOperationListenerTests extends ESSingleNodeTestCase {
    private IndexService indexService;
    private IndexShard shard;

    @Before
    public void setupShard() {
        indexService = createIndex("index");
        shard = indexService.getShard(0);
    }

    public void testUnlicensed() {
        final ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.scroll()).thenReturn(new Scroll(TimeValue.timeValueMinutes(between(1, 10))));
        try (LegacyReaderContext readerContext =
                 new LegacyReaderContext(new ShardSearchContextId(UUIDs.randomBase64UUID(), 0L), indexService, shard,
                     shard.acquireSearcherSupplier(), shardSearchRequest, Long.MAX_VALUE)) {
            XPackLicenseState licenseState = mock(XPackLicenseState.class);
            when(licenseState.isSecurityEnabled()).thenReturn(false);
            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
            AuditTrailService auditTrailService = mock(AuditTrailService.class);
            SearchContext searchContext = mock(SearchContext.class);

            SecuritySearchOperationListener listener =
                new SecuritySearchOperationListener(securityContext, licenseState, auditTrailService);
            listener.onNewScrollContext(readerContext);
            listener.validateReaderContext(readerContext, Empty.INSTANCE);
            verify(licenseState, times(2)).isSecurityEnabled();
            verifyZeroInteractions(auditTrailService, searchContext);
        }
    }

    public void testOnNewContextSetsAuthentication() throws Exception {
        final ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.scroll()).thenReturn(new Scroll(TimeValue.timeValueMinutes(between(1, 10))));
        try (LegacyReaderContext readerContext =
                 new LegacyReaderContext(new ShardSearchContextId(UUIDs.randomBase64UUID(), 0L),
                     indexService, shard, shard.acquireSearcherSupplier(), shardSearchRequest, Long.MAX_VALUE)) {
            XPackLicenseState licenseState = mock(XPackLicenseState.class);
            when(licenseState.isSecurityEnabled()).thenReturn(true);
            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
            AuditTrailService auditTrailService = mock(AuditTrailService.class);
            Authentication authentication = new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
            authentication.writeToContext(threadContext);
            IndicesAccessControl indicesAccessControl = mock(IndicesAccessControl.class);
            threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);

            SecuritySearchOperationListener listener =
                new SecuritySearchOperationListener(securityContext, licenseState, auditTrailService);
            listener.onNewScrollContext(readerContext);

            Authentication contextAuth = readerContext.getFromContext(AuthenticationField.AUTHENTICATION_KEY);
            assertEquals(authentication, contextAuth);
            assertThat(readerContext.getFromContext(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));

            verify(licenseState).isSecurityEnabled();
            verifyZeroInteractions(auditTrailService);
        }
    }

    public void testValidateSearchContext() throws Exception {
        final ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.scroll()).thenReturn(new Scroll(TimeValue.timeValueMinutes(between(1, 10))));
        try (LegacyReaderContext readerContext =
                 new LegacyReaderContext(new ShardSearchContextId(UUIDs.randomBase64UUID(), 0L), indexService, shard,
                     shard.acquireSearcherSupplier(), shardSearchRequest, Long.MAX_VALUE)) {
            readerContext.putInContext(AuthenticationField.AUTHENTICATION_KEY,
                new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null));
            final IndicesAccessControl indicesAccessControl = mock(IndicesAccessControl.class);
            readerContext.putInContext(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);
            XPackLicenseState licenseState = mock(XPackLicenseState.class);
            when(licenseState.isSecurityEnabled()).thenReturn(true);
            when(licenseState.checkFeature(Feature.SECURITY_AUDITING)).thenReturn(true);
            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
            AuditTrail auditTrail = mock(AuditTrail.class);
            AuditTrailService auditTrailService =
                new AuditTrailService(Collections.singletonList(auditTrail), licenseState);

            SecuritySearchOperationListener listener =
                new SecuritySearchOperationListener(securityContext, licenseState, auditTrailService);
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                Authentication authentication = new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
                authentication.writeToContext(threadContext);
                listener.validateReaderContext(readerContext, Empty.INSTANCE);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verify(licenseState).isSecurityEnabled();
                verifyZeroInteractions(auditTrail);
            }

            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                final String nodeName = randomAlphaOfLengthBetween(1, 8);
                final String realmName = randomAlphaOfLengthBetween(1, 16);
                Authentication authentication =
                    new Authentication(new User("test", "role"), new RealmRef(realmName, "file", nodeName), null);
                authentication.writeToContext(threadContext);
                listener.validateReaderContext(readerContext, Empty.INSTANCE);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verify(licenseState, times(2)).isSecurityEnabled();
                verifyZeroInteractions(auditTrail);
            }

            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                final String nodeName = randomBoolean() ? "node" : randomAlphaOfLengthBetween(1, 8);
                final String realmName = randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16);
                final String type = randomAlphaOfLengthBetween(5, 16);
                Authentication authentication =
                    new Authentication(new User("test", "role"), new RealmRef(realmName, type, nodeName), null);
                authentication.writeToContext(threadContext);
                threadContext.putTransient(ORIGINATING_ACTION_KEY, "action");
                threadContext.putTransient(AUTHORIZATION_INFO_KEY,
                    (AuthorizationInfo) () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, authentication.getUser().roles()));
                final InternalScrollSearchRequest request = new InternalScrollSearchRequest();
                SearchContextMissingException expected = expectThrows(SearchContextMissingException.class,
                    () -> listener.validateReaderContext(readerContext, request));
                assertEquals(readerContext.id(), expected.contextId());
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), nullValue());
                verify(licenseState, Mockito.atLeast(3)).isSecurityEnabled();
                verify(auditTrail).accessDenied(eq(null), eq(authentication), eq("action"), eq(request),
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
                listener.validateReaderContext(readerContext, request);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verify(licenseState, Mockito.atLeast(4)).isSecurityEnabled();
                verifyNoMoreInteractions(auditTrail);
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
                SearchContextMissingException expected = expectThrows(SearchContextMissingException.class,
                    () -> listener.validateReaderContext(readerContext, request));
                assertEquals(readerContext.id(), expected.contextId());
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), nullValue());
                verify(licenseState, Mockito.atLeast(5)).isSecurityEnabled();
                verify(auditTrail).accessDenied(eq(null), eq(authentication), eq("action"), eq(request),
                    authzInfoRoles(authentication.getUser().roles()));
            }
        }
    }

    public void testEnsuredAuthenticatedUserIsSame() {
        Authentication original = new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
        Authentication current =
                randomBoolean() ? original : new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
        ShardSearchContextId contextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomLong());
        final String action = randomAlphaOfLength(4);
        TransportRequest request = Empty.INSTANCE;
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(Feature.SECURITY_AUDITING)).thenReturn(true);
        AuditTrail auditTrail = mock(AuditTrail.class);
        AuditTrailService auditTrailService = new AuditTrailService(Collections.singletonList(auditTrail), licenseState);

        final String auditId = randomAlphaOfLengthBetween(8, 20);
        ensureAuthenticatedUserIsSame(original, current, auditTrailService, contextId, action, request, auditId,
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles()));
        verifyZeroInteractions(auditTrail);

        // original user being run as
        User user = new User(new User("test", "role"), new User("authenticated", "runas"));
        current = new Authentication(user, new RealmRef("realm", "file", "node"),
                new RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"));
        ensureAuthenticatedUserIsSame(original, current, auditTrailService, contextId, action, request, auditId,
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles()));
        verifyZeroInteractions(auditTrail);

        // both user are run as
        current = new Authentication(user, new RealmRef("realm", "file", "node"),
                new RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"));
        Authentication runAs = current;
        ensureAuthenticatedUserIsSame(runAs, current, auditTrailService, contextId, action, request, auditId,
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles()));
        verifyZeroInteractions(auditTrail);

        // different authenticated by type
        Authentication differentRealmType =
                new Authentication(new User("test", "role"), new RealmRef("realm", randomAlphaOfLength(5), "node"), null);
        SearchContextMissingException e = expectThrows(SearchContextMissingException.class,
                () -> ensureAuthenticatedUserIsSame(original, differentRealmType, auditTrailService, contextId, action, request, auditId,
                    () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles())));
        assertEquals(contextId, e.contextId());
        verify(auditTrail).accessDenied(eq(auditId), eq(differentRealmType), eq(action), eq(request),
            authzInfoRoles(original.getUser().roles()));

        // wrong user
        Authentication differentUser =
                new Authentication(new User("test2", "role"), new RealmRef("realm", "realm", "node"), null);
        e = expectThrows(SearchContextMissingException.class,
                () -> ensureAuthenticatedUserIsSame(original, differentUser, auditTrailService, contextId, action, request, auditId,
                    () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles())));
        assertEquals(contextId, e.contextId());
        verify(auditTrail).accessDenied(eq(auditId), eq(differentUser), eq(action), eq(request),
            authzInfoRoles(original.getUser().roles()));

        // run as different user
        Authentication diffRunAs = new Authentication(new User(new User("test2", "role"), new User("authenticated", "runas")),
                new RealmRef("realm", "file", "node1"), new RealmRef("realm", "file", "node1"));
        e = expectThrows(SearchContextMissingException.class,
                () -> ensureAuthenticatedUserIsSame(original, diffRunAs, auditTrailService, contextId, action, request, auditId,
                    () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles())));
        assertEquals(contextId, e.contextId());
        verify(auditTrail).accessDenied(eq(auditId), eq(diffRunAs), eq(action), eq(request), authzInfoRoles(original.getUser().roles()));

        // run as different looked up by type
        Authentication runAsDiffType = new Authentication(user, new RealmRef("realm", "file", "node"),
                new RealmRef(randomAlphaOfLengthBetween(1, 16), randomAlphaOfLengthBetween(5, 12), "node"));
        e = expectThrows(SearchContextMissingException.class,
                () -> ensureAuthenticatedUserIsSame(runAs, runAsDiffType, auditTrailService, contextId, action, request, auditId,
                    () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, original.getUser().roles())));
        assertEquals(contextId, e.contextId());
        verify(auditTrail).accessDenied(eq(auditId), eq(runAsDiffType), eq(action), eq(request),
            authzInfoRoles(original.getUser().roles()));
    }
}
