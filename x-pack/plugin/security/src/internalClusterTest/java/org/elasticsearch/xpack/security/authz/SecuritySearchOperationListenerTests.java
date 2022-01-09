/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.LegacyReaderContext;
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
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ORIGINATING_ACTION_KEY;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.security.authz.AuthorizationServiceTests.authzInfoRoles;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SecuritySearchOperationListenerTests extends ESSingleNodeTestCase {
    private IndexService indexService;
    private IndexShard shard;

    @Before
    public void setupShard() {
        indexService = createIndex("index");
        shard = indexService.getShard(0);
    }

    public void testOnNewContextSetsAuthentication() throws Exception {
        final ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.scroll()).thenReturn(new Scroll(TimeValue.timeValueMinutes(between(1, 10))));
        try (
            LegacyReaderContext readerContext = new LegacyReaderContext(
                new ShardSearchContextId(UUIDs.randomBase64UUID(), 0L),
                indexService,
                shard,
                shard.acquireSearcherSupplier(),
                shardSearchRequest,
                Long.MAX_VALUE
            )
        ) {
            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
            AuditTrailService auditTrailService = mock(AuditTrailService.class);
            Authentication authentication = new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
            authentication.writeToContext(threadContext);
            IndicesAccessControl indicesAccessControl = mock(IndicesAccessControl.class);
            threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);

            SecuritySearchOperationListener listener = new SecuritySearchOperationListener(securityContext, auditTrailService);
            listener.onNewScrollContext(readerContext);

            Authentication contextAuth = readerContext.getFromContext(AuthenticationField.AUTHENTICATION_KEY);
            assertEquals(authentication, contextAuth);
            assertThat(readerContext.getFromContext(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));

            verifyNoMoreInteractions(auditTrailService);
        }
    }

    public void testValidateSearchContext() throws Exception {
        final ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.scroll()).thenReturn(new Scroll(TimeValue.timeValueMinutes(between(1, 10))));
        try (
            LegacyReaderContext readerContext = new LegacyReaderContext(
                new ShardSearchContextId(UUIDs.randomBase64UUID(), 0L),
                indexService,
                shard,
                shard.acquireSearcherSupplier(),
                shardSearchRequest,
                Long.MAX_VALUE
            )
        ) {
            readerContext.putInContext(
                AuthenticationField.AUTHENTICATION_KEY,
                new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null)
            );
            final IndicesAccessControl indicesAccessControl = mock(IndicesAccessControl.class);
            readerContext.putInContext(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);
            MockLicenseState licenseState = mock(MockLicenseState.class);
            when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
            AuditTrail auditTrail = mock(AuditTrail.class);
            AuditTrailService auditTrailService = new AuditTrailService(Collections.singletonList(auditTrail), licenseState);

            SecuritySearchOperationListener listener = new SecuritySearchOperationListener(securityContext, auditTrailService);
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                Authentication authentication = new Authentication(new User("test", "role"), new RealmRef("realm", "file", "node"), null);
                authentication.writeToContext(threadContext);
                listener.validateReaderContext(readerContext, Empty.INSTANCE);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verifyNoMoreInteractions(auditTrail);
            }

            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                final String nodeName = randomAlphaOfLengthBetween(1, 8);
                final String realmName = randomAlphaOfLengthBetween(1, 16);
                Authentication authentication = new Authentication(
                    new User("test", "role"),
                    new RealmRef(realmName, "file", nodeName),
                    null
                );
                authentication.writeToContext(threadContext);
                listener.validateReaderContext(readerContext, Empty.INSTANCE);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verifyNoMoreInteractions(auditTrail);
            }

            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                final String nodeName = randomBoolean() ? "node" : randomAlphaOfLengthBetween(1, 8);
                final String realmName = randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16);
                final String type = randomAlphaOfLengthBetween(5, 16);
                Authentication authentication = new Authentication(new User("test", "role"), new RealmRef(realmName, type, nodeName), null);
                authentication.writeToContext(threadContext);
                threadContext.putTransient(ORIGINATING_ACTION_KEY, "action");
                threadContext.putTransient(
                    AUTHORIZATION_INFO_KEY,
                    (AuthorizationInfo) () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, authentication.getUser().roles())
                );
                final InternalScrollSearchRequest request = new InternalScrollSearchRequest();
                SearchContextMissingException expected = expectThrows(
                    SearchContextMissingException.class,
                    () -> listener.validateReaderContext(readerContext, request)
                );
                assertEquals(readerContext.id(), expected.contextId());
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), nullValue());
                verify(auditTrail).accessDenied(
                    eq(null),
                    eq(authentication),
                    eq("action"),
                    eq(request),
                    authzInfoRoles(authentication.getUser().roles())
                );
            }

            // another user running as the original user
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                final String nodeName = randomBoolean() ? "node" : randomAlphaOfLengthBetween(1, 8);
                final String realmName = randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16);
                final String type = randomAlphaOfLengthBetween(5, 16);
                User user = new User(new User("test", "role"), new User("authenticated", "runas"));
                Authentication authentication = new Authentication(
                    user,
                    new RealmRef(realmName, type, nodeName),
                    new RealmRef(randomAlphaOfLengthBetween(1, 16), "file", nodeName)
                );
                authentication.writeToContext(threadContext);
                threadContext.putTransient(ORIGINATING_ACTION_KEY, "action");
                final InternalScrollSearchRequest request = new InternalScrollSearchRequest();
                listener.validateReaderContext(readerContext, request);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verifyNoMoreInteractions(auditTrail);
            }

            // the user that authenticated for the run as request
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                final String nodeName = randomBoolean() ? "node" : randomAlphaOfLengthBetween(1, 8);
                final String realmName = randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16);
                final String type = randomAlphaOfLengthBetween(5, 16);
                Authentication authentication = new Authentication(
                    new User("authenticated", "runas"),
                    new RealmRef(realmName, type, nodeName),
                    null
                );
                authentication.writeToContext(threadContext);
                threadContext.putTransient(ORIGINATING_ACTION_KEY, "action");
                threadContext.putTransient(
                    AUTHORIZATION_INFO_KEY,
                    (AuthorizationInfo) () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, authentication.getUser().roles())
                );
                final InternalScrollSearchRequest request = new InternalScrollSearchRequest();
                SearchContextMissingException expected = expectThrows(
                    SearchContextMissingException.class,
                    () -> listener.validateReaderContext(readerContext, request)
                );
                assertEquals(readerContext.id(), expected.contextId());
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), nullValue());
                verify(auditTrail).accessDenied(
                    eq(null),
                    eq(authentication),
                    eq("action"),
                    eq(request),
                    authzInfoRoles(authentication.getUser().roles())
                );
            }
        }
    }

    public void testValidateCreatorAuthentication() throws Exception {
        final ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.scroll()).thenReturn(new Scroll(TimeValue.timeValueMinutes(between(1, 10))));
        try (
            LegacyReaderContext readerContext = new LegacyReaderContext(
                new ShardSearchContextId(UUIDs.randomBase64UUID(), 0L),
                indexService,
                shard,
                shard.acquireSearcherSupplier(),
                shardSearchRequest,
                Long.MAX_VALUE
            )
        ) {
            final IndicesAccessControl indicesAccessControl = mock(IndicesAccessControl.class);
            readerContext.putInContext(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);
            final MockLicenseState licenseState = mock(MockLicenseState.class);
            when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
            final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
            final AuditTrail auditTrail = mock(AuditTrail.class);
            final AuditTrailService auditTrailService = new AuditTrailService(Collections.singletonList(auditTrail), licenseState);
            final SecuritySearchOperationListener listener = new SecuritySearchOperationListener(securityContext, auditTrailService);

            // current and original are the same
            Authentication original = new Authentication(
                new User("test", "role"),
                new RealmRef("realm", "file", randomAlphaOfLengthBetween(4, 10)),
                null
            );
            readerContext.putInContext(AuthenticationField.AUTHENTICATION_KEY, original);
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                Authentication current = randomBoolean()
                    ? original
                    : new Authentication(new User("test", "role"), new RealmRef("realm", "file", randomAlphaOfLengthBetween(4, 10)), null);
                current.writeToContext(threadContext);
                listener.validateReaderContext(readerContext, Empty.INSTANCE);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verifyNoMoreInteractions(auditTrail);
            }

            // current is being run as
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                Authentication current = new Authentication(
                    new User(new User("test", "role"), new User("authenticated", "runas")),
                    new RealmRef(randomAlphaOfLengthBetween(1, 16), randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(4, 10)),
                    new RealmRef("realm", "file", randomAlphaOfLengthBetween(4, 10))
                );
                current.writeToContext(threadContext);
                listener.validateReaderContext(readerContext, Empty.INSTANCE);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verifyNoMoreInteractions(auditTrail);
            }

            // original is being run as
            original = new Authentication(
                new User(new User("test", "role"), new User("authenticated", "runas")),
                new RealmRef(randomAlphaOfLengthBetween(1, 16), "file", randomAlphaOfLengthBetween(4, 10)),
                new RealmRef("realm", "file", randomAlphaOfLengthBetween(4, 10))
            );
            readerContext.putInContext(AuthenticationField.AUTHENTICATION_KEY, original);
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                Authentication current = new Authentication(
                    new User("test", "role"),
                    new RealmRef("realm", "file", randomAlphaOfLengthBetween(4, 10)),
                    null
                );
                current.writeToContext(threadContext);
                listener.validateReaderContext(readerContext, Empty.INSTANCE);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verifyNoMoreInteractions(auditTrail);
            }

            // current and original are being run as
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                Authentication current = new Authentication(
                    new User(new User("test", "role"), new User("authenticated", "runas")),
                    new RealmRef(randomAlphaOfLengthBetween(1, 16), randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(4, 10)),
                    new RealmRef("realm", "file", randomAlphaOfLengthBetween(4, 10))
                );
                current.writeToContext(threadContext);
                listener.validateReaderContext(readerContext, Empty.INSTANCE);
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), is(indicesAccessControl));
                verifyNoMoreInteractions(auditTrail);
            }

            // different authenticated by type
            original = randomFrom(
                new Authentication(new User("test", "role"), new RealmRef("realm", "originalRealmType", "node"), null),
                new Authentication(
                    new User(new User("test", "role"), new User("authenticated_original", "runas_original")),
                    new RealmRef("realm1", "type1", "node"),
                    new RealmRef("realm", "originalRealmType", "node")
                )
            );
            readerContext.putInContext(AuthenticationField.AUTHENTICATION_KEY, original);
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                Authentication differentRealmType = new Authentication(
                    new User("test", "role"),
                    new RealmRef("realm", "currentRealmType", "node"),
                    null
                );
                differentRealmType.writeToContext(threadContext);
                String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
                threadContext.putTransient(ORIGINATING_ACTION_KEY, "origin_action");
                AuthorizationInfo mockAuthorizationInfo = mock(AuthorizationInfo.class);
                threadContext.putTransient(AUTHORIZATION_INFO_KEY, mockAuthorizationInfo);
                TransportRequest transportRequest = mock(TransportRequest.class);
                SearchContextMissingException e = expectThrows(
                    SearchContextMissingException.class,
                    () -> listener.validateReaderContext(readerContext, transportRequest)
                );
                assertThat(e.contextId(), is(readerContext.id()));
                verify(auditTrail).accessDenied(
                    eq(requestId),
                    eq(differentRealmType),
                    eq("origin_action"),
                    eq(transportRequest),
                    eq(mockAuthorizationInfo)
                );
            }

            // run as different looked up by type
            original = new Authentication(
                new User(new User("test", "role"), new User("authenticated_original", "runas_original")),
                new RealmRef("realm1", "type1", "node"),
                new RealmRef("realm", "originalRealmType", "node")
            );
            readerContext.putInContext(AuthenticationField.AUTHENTICATION_KEY, original);
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                Authentication current = randomFrom(
                    new Authentication(
                        new User(new User("test", "role"), new User("authenticated_original", "runas_original")),
                        new RealmRef("realm1", "type1", "node"),
                        new RealmRef("realm", "currentRealmType", "node")
                    ),
                    new Authentication(new User("test", "role"), new RealmRef("realm", "currentRealmType", "node"), null)
                );
                current.writeToContext(threadContext);
                String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
                threadContext.putTransient(ORIGINATING_ACTION_KEY, "origin_action");
                AuthorizationInfo mockAuthorizationInfo = mock(AuthorizationInfo.class);
                threadContext.putTransient(AUTHORIZATION_INFO_KEY, mockAuthorizationInfo);
                TransportRequest transportRequest = mock(TransportRequest.class);
                SearchContextMissingException e = expectThrows(
                    SearchContextMissingException.class,
                    () -> listener.validateReaderContext(readerContext, transportRequest)
                );
                assertThat(e.contextId(), is(readerContext.id()));
                verify(auditTrail).accessDenied(
                    eq(requestId),
                    eq(current),
                    eq("origin_action"),
                    eq(transportRequest),
                    eq(mockAuthorizationInfo)
                );
            }

            // wrong user
            original = randomFrom(
                new Authentication(new User("test", "role"), new RealmRef("realm", "realm_type", "node"), null),
                new Authentication(
                    new User(new User("test", "role"), new User("authenticated_original", "runas_original")),
                    new RealmRef("realm1", "type1", "node"),
                    new RealmRef("realm", "realm_type", "node")
                )
            );
            readerContext.putInContext(AuthenticationField.AUTHENTICATION_KEY, original);
            try (StoredContext ignore = threadContext.newStoredContext(false)) {
                Authentication current = randomFrom(
                    new Authentication(
                        new User(new User("testWrong", "role"), new User("authenticated_original", "runas_original")),
                        new RealmRef("realm1", "type1", "node"),
                        new RealmRef("realm", "realm_type", "node")
                    ),
                    new Authentication(new User("testWrong", "role"), new RealmRef("realm", "realm_type", "node"), null)
                );
                current.writeToContext(threadContext);
                String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
                threadContext.putTransient(ORIGINATING_ACTION_KEY, "origin_action");
                AuthorizationInfo mockAuthorizationInfo = mock(AuthorizationInfo.class);
                threadContext.putTransient(AUTHORIZATION_INFO_KEY, mockAuthorizationInfo);
                TransportRequest transportRequest = mock(TransportRequest.class);
                SearchContextMissingException e = expectThrows(
                    SearchContextMissingException.class,
                    () -> listener.validateReaderContext(readerContext, transportRequest)
                );
                assertThat(e.contextId(), is(readerContext.id()));
                verify(auditTrail).accessDenied(
                    eq(requestId),
                    eq(current),
                    eq("origin_action"),
                    eq(transportRequest),
                    eq(mockAuthorizationInfo)
                );
            }
        }
    }
}
