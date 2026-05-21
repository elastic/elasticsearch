/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
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

    public void testLoadAndRevokeIsNoopWhenFlagOff() {
        assumeFalse("Only relevant if feature flag is OFF", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));

        newManager(apiKeyService, configManager, auditor).loadAndRevoke(TRANSFORM_ID, future);

        verifyNoInteractions(apiKeyService, configManager, auditor);
    }

    public void testLoadAndRevokeRevokesWhenCredentialPresent() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();
        doAnswer(invocation -> {
            ActionListener<PersistedCloudCredential> l = invocation.getArgument(2);
            l.onResponse(credential);
            return null;
        }).when(configManager).getTransformCloudCredential(eq(TRANSFORM_ID), eq(true), any());
        doAnswer(invocation -> {
            ActionListener<Void> l = invocation.getArgument(1);
            l.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadAndRevoke(TRANSFORM_ID, future);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        verify(auditor).info(eq(TRANSFORM_ID), eq("revoked cloud credential [" + credential.id() + "]"));
        expectThrows(IllegalStateException.class, () -> credential.internalApiKey().length());
    }

    public void testLoadAndRevokeProceedsOnLoadFailure() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        doAnswer(invocation -> {
            ActionListener<PersistedCloudCredential> l = invocation.getArgument(2);
            l.onFailure(new RuntimeException("system index unavailable"));
            return null;
        }).when(configManager).getTransformCloudCredential(eq(TRANSFORM_ID), eq(true), any());

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadAndRevoke(TRANSFORM_ID, future);

        // no credential loaded -> no revoke call and no audit
        verifyNoInteractions(apiKeyService, auditor);
    }

    public void testLoadRevokeAndDeleteHappyPath() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();
        doAnswer(invocation -> {
            ActionListener<PersistedCloudCredential> l = invocation.getArgument(2);
            l.onResponse(credential);
            return null;
        }).when(configManager).getTransformCloudCredential(eq(TRANSFORM_ID), eq(true), any());
        doAnswer(invocation -> {
            ActionListener<Void> l = invocation.getArgument(1);
            l.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onResponse(true);
            return null;
        }).when(configManager).deleteTransformCloudCredential(eq(TRANSFORM_ID), any());

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadRevokeAndDelete(TRANSFORM_ID, future);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        verify(configManager).deleteTransformCloudCredential(eq(TRANSFORM_ID), any());
        verify(auditor).info(eq(TRANSFORM_ID), eq("revoked cloud credential [" + credential.id() + "]"));
    }

    public void testLoadRevokeAndDeleteContinuesOnDeleteFailure() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var credential = newCredential();
        doAnswer(invocation -> {
            ActionListener<PersistedCloudCredential> l = invocation.getArgument(2);
            l.onResponse(credential);
            return null;
        }).when(configManager).getTransformCloudCredential(eq(TRANSFORM_ID), eq(true), any());
        doAnswer(invocation -> {
            ActionListener<Void> l = invocation.getArgument(1);
            l.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onFailure(new RuntimeException("system index unavailable"));
            return null;
        }).when(configManager).deleteTransformCloudCredential(eq(TRANSFORM_ID), any());

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadRevokeAndDelete(TRANSFORM_ID, future);

        verify(apiKeyService).revokeCloudAuthentication(eq(credential), any());
        verify(configManager).deleteTransformCloudCredential(eq(TRANSFORM_ID), any());
    }

    public void testLoadRevokeAndDeleteIsNoopWhenFlagOff() {
        assumeFalse("Only relevant if feature flag is OFF", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        newManager(apiKeyService, configManager, auditor).loadRevokeAndDelete(TRANSFORM_ID, future);

        verifyNoInteractions(apiKeyService, configManager, auditor);
    }

    public void testMintAndPersistAuditsFirstMintWhenNoPriorCredential() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        runMintAndPersistAuditTest(null, "minted cloud credential [new-id]");
    }

    public void testMintAndPersistAuditsRotationWhenPriorCredentialExists() {
        assumeTrue("Only relevant if feature flag is enabled", TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled());
        var prior = new PersistedCloudCredential("old-id", new SecureString("v".toCharArray()));
        runMintAndPersistAuditTest(prior, "rotated cloud credential, new [new-id], previous [old-id]");
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

    private void runMintAndPersistAuditTest(PersistedCloudCredential prior, String expectedAuditMessage) {
        var apiKeyService = mock(InternalCloudApiKeyService.class);
        var configManager = mock(TransformConfigManager.class);
        var auditor = mock(TransformAuditor.class);
        var threadPool = mock(ThreadPool.class);
        var credentialManager = mock(CloudCredentialManager.class);
        var threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        // caller has a UIAM credential
        var callerCredential = new CloudCredential(new SecureString("caller-cred".toCharArray()));
        when(credentialManager.hasCloudManagedCredential(threadContext)).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(threadContext)).thenReturn(callerCredential);

        // mintAndPersist loads prior first for rekey detection
        doAnswer(invocation -> {
            ActionListener<PersistedCloudCredential> l = invocation.getArgument(2);
            l.onResponse(prior);
            return null;
        }).when(configManager).getTransformCloudCredential(eq(TRANSFORM_ID), eq(true), any());

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

        var future = ActionTestUtils.<Void>assertNoFailureListener(r -> assertNull(r));
        manager.mintAndPersist(TRANSFORM_ID, future);

        verify(auditor).info(eq(TRANSFORM_ID), eq(expectedAuditMessage));
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
