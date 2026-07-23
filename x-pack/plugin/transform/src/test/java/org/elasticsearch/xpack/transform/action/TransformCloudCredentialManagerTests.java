/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService.CloudGrantApiKeyResult;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransformCloudCredentialManagerTests extends ESTestCase {

    private static final String TRANSFORM_ID = "transform-1";

    public void testRevokeAndCloseIsNoOpForNull() {
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var auditor = mock(TransformAuditor.class);
        var manager = newManager(apiKeyService, mock(TransformConfigManager.class), auditor);

        manager.revokeAndClose(TRANSFORM_ID, null);

        verifyNoInteractions(apiKeyService, auditor);
    }

    public void testRevokeAndCloseSkipsRevokeButClosesWhenFeatureFlagOff() {
        assumeFalse("Only relevant if feature flag is OFF", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();

        newManager(apiKeyService, mock(TransformConfigManager.class), auditor).revokeAndClose(TRANSFORM_ID, credential);

        verifyNoInteractions(apiKeyService, auditor);
        expectThrows(IllegalStateException.class, () -> credential.internalApiKey().length());
    }

    public void testRevokeAndCloseInvokesApiAndAuditsOnSuccess() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();
        var credId = credential.id();
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());

        newManager(apiKeyService, mock(TransformConfigManager.class), auditor).revokeAndClose(TRANSFORM_ID, credential);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        verify(auditor).info(eq(TRANSFORM_ID), eq("revoked cloud credential [" + credId + "]"));
        verify(auditor, never()).warning(any(), any());
        expectThrows(IllegalStateException.class, () -> credential.internalApiKey().length());
    }

    public void testRevokeAndCloseAuditsAndClosesOnRevokeFailure() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();
        var credId = credential.id();
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("UIAM unavailable"));
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());

        newManager(apiKeyService, mock(TransformConfigManager.class), auditor).revokeAndClose(TRANSFORM_ID, credential);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        // warning row surfaces the UIAM-key-leak to the user
        verify(auditor).warning(eq(TRANSFORM_ID), eq("failed to revoke cloud credential [" + credId + "]: UIAM unavailable"));
        verify(auditor, never()).info(any(), any());
        // close still ran via runAfter — best-effort revoke must not block resource cleanup
        expectThrows(IllegalStateException.class, () -> credential.internalApiKey().length());
    }

    public void testLoadAndRevokeByTokenIdIsNoopWhenTokenIdNull() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));

        newManager(apiKeyService, configManager, auditor).loadAndRevokeByTokenId(TRANSFORM_ID, null, future);

        verifyNoInteractions(apiKeyService, configManager, auditor);
    }

    public void testLoadAndRevokeByTokenIdIsNoopWhenFlagOff() {
        assumeFalse("Only relevant if feature flag is OFF", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));

        newManager(apiKeyService, configManager, auditor).loadAndRevokeByTokenId(TRANSFORM_ID, "some-token", future);

        verifyNoInteractions(apiKeyService, configManager, auditor);
    }

    public void testLoadAndRevokeByTokenIdRevokesWhenCredentialPresent() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();
        doAnswer(invocation -> {
            ActionListener<PersistedCloudCredential> l = invocation.getArgument(2);
            l.onResponse(credential);
            return null;
        }).when(configManager).getTransformCloudCredentialByTokenId(eq(credential.id()), eq(true), any());
        doAnswer(invocation -> {
            ActionListener<Void> l = invocation.getArgument(1);
            l.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadAndRevokeByTokenId(TRANSFORM_ID, credential.id(), future);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        verify(auditor).info(eq(TRANSFORM_ID), eq("revoked cloud credential [" + credential.id() + "]"));
        expectThrows(IllegalStateException.class, () -> credential.internalApiKey().length());
    }

    public void testLoadAndRevokeByTokenIdProceedsOnLoadFailure() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        doAnswer(invocation -> {
            ActionListener<PersistedCloudCredential> l = invocation.getArgument(2);
            l.onFailure(new RuntimeException("system index unavailable"));
            return null;
        }).when(configManager).getTransformCloudCredentialByTokenId(eq("missing"), eq(true), any());

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadAndRevokeByTokenId(TRANSFORM_ID, "missing", future);

        // no credential loaded -> no revoke call and no audit
        verifyNoInteractions(apiKeyService, auditor);
    }

    public void testLoadRevokeAndDeleteByTokenIdHappyPath() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();
        doAnswer(invocation -> {
            ActionListener<PersistedCloudCredential> l = invocation.getArgument(2);
            l.onResponse(credential);
            return null;
        }).when(configManager).getTransformCloudCredentialByTokenId(eq(credential.id()), eq(true), any());
        doAnswer(invocation -> {
            ActionListener<Void> l = invocation.getArgument(1);
            l.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onResponse(true);
            return null;
        }).when(configManager).deleteCloudCredentialByTokenId(eq(credential.id()), any());

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadRevokeAndDeleteByTokenId(TRANSFORM_ID, credential.id(), future);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        verify(configManager).deleteCloudCredentialByTokenId(eq(credential.id()), any());
        verify(auditor).info(eq(TRANSFORM_ID), eq("revoked cloud credential [" + credential.id() + "]"));
    }

    public void testLoadRevokeAndDeleteByTokenIdContinuesOnDeleteFailure() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();
        doAnswer(invocation -> {
            ActionListener<PersistedCloudCredential> l = invocation.getArgument(2);
            l.onResponse(credential);
            return null;
        }).when(configManager).getTransformCloudCredentialByTokenId(eq(credential.id()), eq(true), any());
        doAnswer(invocation -> {
            ActionListener<Void> l = invocation.getArgument(1);
            l.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onFailure(new RuntimeException("system index unavailable"));
            return null;
        }).when(configManager).deleteCloudCredentialByTokenId(eq(credential.id()), any());

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadRevokeAndDeleteByTokenId(TRANSFORM_ID, credential.id(), future);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        verify(configManager).deleteCloudCredentialByTokenId(eq(credential.id()), any());
    }

    public void testLoadRevokeAndDeleteByTokenIdIsNoopWhenTokenIdNull() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadRevokeAndDeleteByTokenId(TRANSFORM_ID, null, future);

        verifyNoInteractions(apiKeyService, configManager, auditor);
    }

    public void testLoadRevokeAndDeleteByTokenIdIsNoopWhenFlagOff() {
        assumeFalse("Only relevant if feature flag is OFF", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadRevokeAndDeleteByTokenId(TRANSFORM_ID, "some-token", future);

        verifyNoInteractions(apiKeyService, configManager, auditor);
    }

    public void testMintAndPersistReturnsNewTokenIdAndAudits() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        runMintAndPersistAuditTest("minted cloud credential [new-id]");
    }

    public void testWrapWithUiamIfPresentReturnsRawClientWhenCredentialIsNull() {
        var rawClient = mock(Client.class);
        // Use the real Noop manager so its default wrapClient(Client, @Nullable CloudCredential) overload
        // returns the delegate as-is when credential is null. A mock would short-circuit the default method
        // and return null, hiding the actual contract we want to assert.
        var manager = new TransformCloudCredentialManager(
            mock(ThreadPool.class),
            null,
            new CloudCredentialManager.Noop(),
            mock(InternalCloudApiKeyService.class),
            mock(TransformConfigManager.class),
            mock(TransformAuditor.class)
        );

        assertThat(manager.wrapWithUiamIfPresent(rawClient, null), sameInstance(rawClient));
    }

    public void testWrapWithUiamIfPresentDelegatesToCredentialManagerWhenCredentialPresent() {
        var rawClient = mock(Client.class);
        var wrappedClient = mock(Client.class);
        var credential = new CloudCredential(new SecureString("user-cred".toCharArray()));
        var credentialManager = mock(CloudCredentialManager.class);
        when(credentialManager.wrapClient(rawClient, credential)).thenReturn(wrappedClient);
        var manager = new TransformCloudCredentialManager(
            mock(ThreadPool.class),
            null,
            credentialManager,
            mock(InternalCloudApiKeyService.class),
            mock(TransformConfigManager.class),
            mock(TransformAuditor.class)
        );

        var actual = manager.wrapWithUiamIfPresent(rawClient, credential);

        assertThat(actual, sameInstance(wrappedClient));
        verify(credentialManager).wrapClient(rawClient, credential);
    }

    public void testWrapWithPersistedIfPresentReturnsRawClientWhenPersistedIsNull() {
        var rawClient = mock(Client.class);
        var manager = new TransformCloudCredentialManager(
            mock(ThreadPool.class),
            null,
            new CloudCredentialManager.Noop(),
            mock(InternalCloudApiKeyService.class),
            mock(TransformConfigManager.class),
            mock(TransformAuditor.class)
        );

        assertThat(manager.wrapWithPersistedIfPresent(rawClient, null), sameInstance(rawClient));
    }

    public void testWrapWithPersistedIfPresentDelegatesToCredentialManagerWhenPersistedPresent() {
        var rawClient = mock(Client.class);
        var wrappedClient = mock(Client.class);
        var persisted = new PersistedCloudCredential("id", new SecureString("key".toCharArray()));
        var credentialManager = mock(CloudCredentialManager.class);
        when(credentialManager.wrapClient(rawClient, persisted)).thenReturn(wrappedClient);
        var manager = new TransformCloudCredentialManager(
            mock(ThreadPool.class),
            null,
            credentialManager,
            mock(InternalCloudApiKeyService.class),
            mock(TransformConfigManager.class),
            mock(TransformAuditor.class)
        );

        var actual = manager.wrapWithPersistedIfPresent(rawClient, persisted);

        assertThat(actual, sameInstance(wrappedClient));
        verify(credentialManager).wrapClient(rawClient, persisted);
    }

    public void testCurrentCallerCredentialPrefersSecondaryAuth() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        var threadContext = new ThreadContext(Settings.EMPTY);
        var threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        var securityContext = new SecurityContext(Settings.EMPTY, threadContext);

        // Same serialized auth headers used in SecondaryAuthorizationUtilsTests: JOHN is the primary
        // (think: Kibana service account), BILL is the secondary (the user). When we extract under
        // useSecondaryAuthIfAvailable, the AUTHENTICATION_KEY header should be BILL.
        var primaryAuthHeader =
            "45XtAwAXdHJhbnNmb3JtX2FkbWluX25vX2RhdGEBD3RyYW5zZm9ybV9hZG1pbgoAAAABAA5qYXZhUmVzdFRlc3QtMA5kZWZhdWx0X25hdGl2ZQZuYXRpdmUAAAAA";
        var secondaryAuthHeader =
            "45XtAwARdHJhbnNmb3JtX2FkbWluXzICD3RyYW5zZm9ybV9hZG1pbhJ0ZXN0X2RhdGFfYWNjZXNzXzIKAAAAAQAOamF2YVJlc3RUZXN0LTAOZGVmYXVsdF9uYXRpd"
                + "mUGbmF0aXZlAAAAAA==";
        threadContext.setHeaders(
            Tuple.tuple(
                Map.of(
                    AuthenticationField.AUTHENTICATION_KEY,
                    primaryAuthHeader,
                    SecondaryAuthentication.THREAD_CTX_KEY,
                    secondaryAuthHeader
                ),
                Map.of()
            )
        );

        var credentialManager = mock(CloudCredentialManager.class);
        var capturedAuthHeader = new AtomicReference<String>();
        var userCredential = new CloudCredential(new SecureString("user-secondary-cred".toCharArray()));
        when(credentialManager.hasCloudManagedCredential(any())).thenAnswer(invocation -> {
            // Capture which AUTHENTICATION_KEY is visible at the moment of extraction: under
            // useSecondaryAuthIfAvailable this is the secondary (BILL); without it would be JOHN.
            capturedAuthHeader.set(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY));
            return true;
        });
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(userCredential);

        var manager = new TransformCloudCredentialManager(
            threadPool,
            securityContext,
            credentialManager,
            mock(InternalCloudApiKeyService.class),
            mock(TransformConfigManager.class),
            mock(TransformAuditor.class)
        );

        var extracted = manager.currentCallerCredential();
        assertThat(extracted, sameInstance(userCredential));
        assertThat("extraction must run under secondary auth", capturedAuthHeader.get(), equalTo(secondaryAuthHeader));
        // Outer context is restored after useSecondaryAuthIfAvailable returns.
        assertThat(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY), equalTo(primaryAuthHeader));
        assertThat(threadContext.getHeader(SecondaryAuthentication.THREAD_CTX_KEY), equalTo(secondaryAuthHeader));
    }

    private void runMintAndPersistAuditTest(String expectedAuditMessage) {
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var threadPool = mock(ThreadPool.class);
        var credentialManager = mock(CloudCredentialManager.class);
        // caller has a UIAM credential, supplied explicitly (as the coordinating node would)
        var callerCredential = new CloudCredential(new SecureString("caller-cred".toCharArray()));

        // grant returns a fresh persisted credential with a known id
        var newPersisted = new PersistedCloudCredential("new-id", new SecureString("new-key".toCharArray()));
        doAnswer(invocation -> {
            ActionListener<CloudGrantApiKeyResult> l = invocation.getArgument(2);
            l.onResponse(new CloudGrantApiKeyResult(newPersisted, null));
            return null;
        }).when(apiKeyService).grantCloudAuthentication(eq(callerCredential), eq("transform:" + TRANSFORM_ID), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(2);
            l.onResponse(true);
            return null;
        }).when(configManager).putTransformCloudCredential(eq(TRANSFORM_ID), eq(newPersisted), any());

        var manager = new TransformCloudCredentialManager(threadPool, null, credentialManager, apiKeyService, configManager, auditor);

        var capturedTokenId = new AtomicReference<String>();
        var future = ActionTestUtils.<String>assertNoFailureListener(capturedTokenId::set);
        // mintAndPersist only borrows callerCredential; the caller (this test, standing in for the
        // coordinating-node doExecute that would have extracted it) is responsible for closing it.
        try (callerCredential) {
            manager.mintAndPersist(TRANSFORM_ID, callerCredential, future);
        }

        verify(auditor).info(eq(TRANSFORM_ID), eq(expectedAuditMessage));
        assertThat(capturedTokenId.get(), equalTo("new-id"));
    }

    public void testMintAndPersistRevokesAtUiamWhenPersistFails() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var threadPool = mock(ThreadPool.class);
        var credentialManager = mock(CloudCredentialManager.class);
        var callerCredential = new CloudCredential(new SecureString("caller-cred".toCharArray()));

        // grant succeeds and returns a freshly-minted credential
        var mintedCredential = new PersistedCloudCredential("minted-id", new SecureString("minted-key".toCharArray()));
        doAnswer(invocation -> {
            ActionListener<CloudGrantApiKeyResult> l = invocation.getArgument(2);
            l.onResponse(new CloudGrantApiKeyResult(mintedCredential, null));
            return null;
        }).when(apiKeyService).grantCloudAuthentication(eq(callerCredential), eq("transform:" + TRANSFORM_ID), any());

        // persist fails
        var persistFailure = new RuntimeException("index-unavailable");
        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(2);
            l.onFailure(persistFailure);
            return null;
        }).when(configManager).putTransformCloudCredential(eq(TRANSFORM_ID), eq(mintedCredential), any());

        // revoke is wired to succeed
        doAnswer(invocation -> {
            ActionListener<Void> l = invocation.getArgument(1);
            l.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(mintedCredential), any());

        var manager = new TransformCloudCredentialManager(threadPool, null, credentialManager, apiKeyService, configManager, auditor);

        var capturedFailure = new AtomicReference<Exception>();
        // mintAndPersist only borrows callerCredential; the caller (this test, standing in for the
        // coordinating-node doExecute that would have extracted it) is responsible for closing it.
        try (callerCredential) {
            manager.mintAndPersist(TRANSFORM_ID, callerCredential, ActionTestUtils.assertNoSuccessListener(capturedFailure::set));
        }

        // the persist failure must propagate to the caller
        assertThat(capturedFailure.get(), sameInstance(persistFailure));
        // revoke must be invoked at UIAM so the orphaned token doesn't persist forever
        verify(apiKeyService).revokeCloudAuthentication(eq(mintedCredential), any());
        // the minted credential's SecureString must be closed
        expectThrows(IllegalStateException.class, () -> mintedCredential.internalApiKey().length());
    }

    public void testRevokeCloseAndDeleteRevokesAndDeletesStorageDoc() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();

        doAnswer(invocation -> {
            ActionListener<Void> l = invocation.getArgument(1);
            l.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onResponse(true);
            return null;
        }).when(configManager).deleteCloudCredentialByTokenId(eq(credential.id()), any());

        newManager(apiKeyService, configManager, auditor).revokeCloseAndDelete(TRANSFORM_ID, credential);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        verify(configManager).deleteCloudCredentialByTokenId(eq(credential.id()), any());
        expectThrows(IllegalStateException.class, () -> credential.internalApiKey().length());
    }

    public void testRevokeCloseAndDeleteLeavesStorageDocOnRevokeFailure() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());

        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();

        doAnswer(invocation -> {
            ActionListener<Void> l = invocation.getArgument(1);
            l.onFailure(new RuntimeException("revoke-failed"));
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());

        newManager(apiKeyService, configManager, auditor).revokeCloseAndDelete(TRANSFORM_ID, credential);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        // storage doc left in place so the startup sweep or delete API can retry
        verify(configManager, never()).deleteCloudCredentialByTokenId(any(), any());
        expectThrows(IllegalStateException.class, () -> credential.internalApiKey().length());
    }

    private static TransformCloudCredentialManager newManager(
        InternalCloudApiKeyService apiKeyService,
        TransformConfigManager configManager,
        TransformAuditor auditor
    ) {
        return new TransformCloudCredentialManager(
            mock(ThreadPool.class),
            null,
            mock(CloudCredentialManager.class),
            apiKeyService,
            configManager,
            auditor
        );
    }

    private static PersistedCloudCredential newCredential() {
        var credential = new PersistedCloudCredential(randomAlphaOfLengthBetween(4, 12), new SecureString("v".toCharArray()));
        assertThat(credential.internalApiKey().length(), is(1));
        return credential;
    }
}
