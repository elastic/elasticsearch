/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.search.crossproject.NoMatchingProjectException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.ml.datafeed.CredentialTransitions.Change;
import org.elasticsearch.xpack.ml.datafeed.CredentialTransitions.Intent;
import org.elasticsearch.xpack.ml.datafeed.CredentialTransitions.TransitionContext;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.security.cloud.CloudCredentialTestUtils.randomCloudCredentialEncryptedData;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CredentialTransitionsTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    private static TransitionContext ctx(
        boolean crossProjectEnabled,
        boolean callerHasCloudCredential,
        boolean envelopeExists,
        boolean affectsCrossProjectSearchSurface
    ) {
        return new TransitionContext(
            crossProjectEnabled,
            callerHasCloudCredential,
            envelopeExists,
            affectsCrossProjectSearchSurface,
            false
        );
    }

    private static TransitionContext ctxWithForceRekeying(
        boolean crossProjectEnabled,
        boolean callerHasCloudCredential,
        boolean envelopeExists,
        boolean affectsCrossProjectSearchSurface,
        boolean forceRekeying
    ) {
        return new TransitionContext(
            crossProjectEnabled,
            callerHasCloudCredential,
            envelopeExists,
            affectsCrossProjectSearchSurface,
            forceRekeying
        );
    }

    public void testCpsDisabledShouldDecideKeep() {
        assertThat(CredentialTransitions.decideForUpdate(ctx(false, true, true, true)), equalTo(Intent.KEEP));
    }

    public void testNoCloudCallerWithEnvelopeShouldDecideClear() {
        assertThat(CredentialTransitions.decideForUpdate(ctx(true, false, true, false)), equalTo(Intent.CLEAR));
    }

    public void testNoCloudCallerWithoutEnvelopeShouldDecideKeep() {
        assertThat(CredentialTransitions.decideForUpdate(ctx(true, false, false, true)), equalTo(Intent.KEEP));
    }

    public void testCloudCallerOnConfigRequiringInternalWithoutEnvelopeShouldDecideReplace() {
        assertThat(CredentialTransitions.decideForUpdate(ctx(true, true, false, false)), equalTo(Intent.REPLACE));
    }

    public void testCloudCallerOnConfigRequiringInternalWithEnvelopeNoSurfaceChangeShouldDecideKeep() {
        assertThat(CredentialTransitions.decideForUpdate(ctx(true, true, true, false)), equalTo(Intent.KEEP));
    }

    public void testCloudCallerOnConfigRequiringInternalWithEnvelopeAndSurfaceChangeShouldDecideReplace() {
        assertThat(CredentialTransitions.decideForUpdate(ctx(true, true, true, true)), equalTo(Intent.REPLACE));
    }

    public void testCloudCallerWithEnvelopeAndForceRekeyingWithoutSurfaceChangeShouldDecideReplace() {
        assertThat(CredentialTransitions.decideForUpdate(ctxWithForceRekeying(true, true, true, false, true)), equalTo(Intent.REPLACE));
    }

    public void testForceRekeyingWithoutCloudCallerWithEnvelopeShouldDecideClear() {
        assertThat(CredentialTransitions.decideForUpdate(ctxWithForceRekeying(true, false, true, false, true)), equalTo(Intent.CLEAR));
    }

    public void testForceRekeyingWithCpsDisabledShouldDecideKeep() {
        assertThat(CredentialTransitions.decideForUpdate(ctxWithForceRekeying(false, true, true, false, true)), equalTo(Intent.KEEP));
    }

    public void testCreateWithCpsDisabledShouldDecideKeep() {
        assertThat(CredentialTransitions.decideForCreate(ctx(false, true, false, false)), equalTo(Intent.KEEP));
    }

    public void testCreateWithNoCloudCallerShouldDecideKeep() {
        assertThat(CredentialTransitions.decideForCreate(ctx(true, false, false, false)), equalTo(Intent.KEEP));
    }

    public void testCreateWithCloudCallerAndCpsEnabledShouldDecideReplace() {
        assertThat(CredentialTransitions.decideForCreate(ctx(true, true, false, false)), equalTo(Intent.REPLACE));
    }

    public void testKeepAndClearAreSingletons() {
        assertThat(Change.KEEP, sameInstance(Change.KEEP));
        assertThat(Change.CLEAR, sameInstance(Change.CLEAR));
    }

    public void testMintShouldHoldHook() {
        BiConsumer<DatafeedConfig, ActionListener<CredentialTransitions.MintedCredential>> hook = (config, listener) -> {};
        Change.Mint mint = new Change.Mint(hook);
        assertThat(mint.mintHook(), notNullValue());
        assertThat(mint.mintHook(), equalTo(hook));
    }

    @SuppressWarnings("unchecked")
    private static void stubGrantFailsAfterValidate(InternalCloudApiKeyService apiKeyService, RuntimeException failure) {
        doAnswer(invocation -> {
            ActionListener<?> listener = invocation.getArgument(2);
            listener.onFailure(failure);
            return null;
        }).when(apiKeyService).grantCloudAuthentication(nullable(CloudCredential.class), anyString(), any());
    }

    @SuppressWarnings("unchecked")
    public void testValidateSearchBeforeMintWhenCloudCredentialPresentShouldUseWrappedClient() {
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client delegateClient = mock(Client.class);
        Client wrappedClient = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(delegateClient.threadPool()).thenReturn(threadPool);

        CloudCredential callerCredential = new CloudCredential(new SecureString("caller".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(same(threadContext))).thenReturn(callerCredential);
        when(credentialManager.wrapClient(same(delegateClient), eq(callerCredential))).thenReturn(wrappedClient);
        when(wrappedClient.threadPool()).thenReturn(threadPool);

        mockSearchProbeSucceeds(wrappedClient);
        RuntimeException grantFailure = new RuntimeException("stop after validate");
        stubGrantFailsAfterValidate(apiKeyService, grantFailure);

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            delegateClient,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df", "job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        ClusterState clusterState = mock(ClusterState.class);

        AtomicReference<Exception> failure = new AtomicReference<>();
        transitions.executePut(
            Intent.REPLACE,
            request,
            clusterState,
            threadPool,
            null,
            (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
            ActionListener.wrap(ignored -> fail("expected mint failure"), failure::set)
        );

        assertThat(failure.get(), equalTo(grantFailure));
        verify(credentialManager).wrapClient(same(delegateClient), eq(callerCredential));
        verify(wrappedClient).execute(same(TransportSearchAction.TYPE), any(SearchRequest.class), any());
        verify(delegateClient, never()).execute(same(TransportSearchAction.TYPE), any(SearchRequest.class), any());
    }

    @SuppressWarnings("unchecked")
    public void testValidateSearchBeforeMintWhenNoCloudCredentialShouldUseBareClient() {
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        // no stub for extractCloudManagedCredential: the mock returns null, i.e. the caller is not cloud-managed
        when(credentialManager.wrapClient(same(client), nullable(CloudCredential.class))).thenReturn(client);

        mockSearchProbeSucceeds(client);
        RuntimeException grantFailure = new RuntimeException("stop after validate");
        stubGrantFailsAfterValidate(apiKeyService, grantFailure);

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df", "job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        ClusterState clusterState = mock(ClusterState.class);

        AtomicReference<Exception> failure = new AtomicReference<>();
        transitions.executePut(
            Intent.REPLACE,
            request,
            clusterState,
            threadPool,
            null,
            (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
            ActionListener.wrap(ignored -> fail("expected mint failure"), failure::set)
        );

        assertThat(failure.get(), equalTo(grantFailure));
        // extract is consulted (atomic capture) and yields null, so the bare client is used
        verify(credentialManager, atLeastOnce()).extractCloudManagedCredential(same(threadContext));
        verify(credentialManager).wrapClient(same(client), nullable(CloudCredential.class));
        verify(client).execute(same(TransportSearchAction.TYPE), any(SearchRequest.class), any());
    }

    @SuppressWarnings("unchecked")
    public void testValidateSearchBeforeMintWhenCarriedCredentialPresentShouldPreferCarriedOverThreadContext() {
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        CloudCredential carriedCredential = new CloudCredential(new SecureString("carried".toCharArray()));
        CloudCredential threadCredential = new CloudCredential(new SecureString("thread".toCharArray()));
        // tripwire: if the code wrongly extracted from the thread context it would see threadCredential
        when(credentialManager.extractCloudManagedCredential(same(threadContext))).thenReturn(threadCredential);
        when(credentialManager.wrapClient(same(client), eq(carriedCredential))).thenReturn(client);

        mockSearchProbeSucceeds(client);
        RuntimeException grantFailure = new RuntimeException("stop after validate");
        stubGrantFailsAfterValidate(apiKeyService, grantFailure);

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df", "job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        request.setCloudCredential(carriedCredential);
        ClusterState clusterState = mock(ClusterState.class);

        AtomicReference<Exception> failure = new AtomicReference<>();
        transitions.executePut(
            Intent.REPLACE,
            request,
            clusterState,
            threadPool,
            null,
            (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
            ActionListener.wrap(ignored -> fail("expected mint failure"), failure::set)
        );

        assertThat(failure.get(), equalTo(grantFailure));
        verify(credentialManager).wrapClient(same(client), eq(carriedCredential));
        verify(credentialManager, never()).extractCloudManagedCredential(any());
    }

    @SuppressWarnings("unchecked")
    public void testMintWithCarriedCredentialShouldGrantEvenWhenThreadContextLacksTransient() {
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        CloudCredential carriedCredential = new CloudCredential(new SecureString("carried".toCharArray()));
        // no stub for extractCloudManagedCredential: the thread context lacks the transient, the carried credential wins
        when(credentialManager.wrapClient(same(client), eq(carriedCredential))).thenReturn(client);

        mockSearchProbeSucceeds(client);
        doAnswer(invocation -> {
            CloudCredential callerCredential = invocation.getArgument(0);
            assertThat(callerCredential, equalTo(carriedCredential));
            ActionListener<?> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("stop after grant"));
            return null;
        }).when(apiKeyService).grantCloudAuthentication(eq(carriedCredential), anyString(), any());

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df", "job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        request.setCloudCredential(carriedCredential);
        ClusterState clusterState = mock(ClusterState.class);

        AtomicReference<Exception> failure = new AtomicReference<>();
        transitions.executePut(
            Intent.REPLACE,
            request,
            clusterState,
            threadPool,
            null,
            (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
            ActionListener.wrap(ignored -> fail("expected mint failure"), failure::set)
        );

        assertThat(failure.get().getMessage(), equalTo("stop after grant"));
        verify(apiKeyService).grantCloudAuthentication(eq(carriedCredential), eq("datafeed:df"), any());
    }

    @SuppressWarnings("unchecked")
    public void testValidateSearchBeforeMintWhenProbeReturnsNoMatchingProjectShouldDeferToRuntime() {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        CloudCredential callerCredential = new CloudCredential(new SecureString("caller".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(same(threadContext))).thenReturn(callerCredential);
        when(credentialManager.wrapClient(same(client), eq(callerCredential))).thenReturn(client);

        mockSearchProbeFails(client, new NoMatchingProjectException("_alias:nonexistent-project-*"));
        RuntimeException grantFailure = new RuntimeException("stop after validate");
        stubGrantFailsAfterValidate(apiKeyService, grantFailure);

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df", "job");
        builder.setIndices(List.of("logs-*"));
        builder.setProjectRouting("_alias:nonexistent-project-*");
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        ClusterState clusterState = mock(ClusterState.class);

        AtomicReference<Exception> failure = new AtomicReference<>();
        transitions.executePut(
            Intent.REPLACE,
            request,
            clusterState,
            threadPool,
            null,
            (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
            ActionListener.wrap(ignored -> fail("expected mint failure"), failure::set)
        );

        assertThat(failure.get(), equalTo(grantFailure));
        verify(apiKeyService).grantCloudAuthentication(nullable(CloudCredential.class), eq("datafeed:df"), any());
    }

    @SuppressWarnings("unchecked")
    public void testValidateSearchBeforeMintWhenProbeReturnsNoMatchingProjectForQualifiedIndexShouldFail() {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        CloudCredential callerCredential = new CloudCredential(new SecureString("caller".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(same(threadContext))).thenReturn(callerCredential);
        when(credentialManager.wrapClient(same(client), eq(callerCredential))).thenReturn(client);

        NoMatchingProjectException noMatchingProject = new NoMatchingProjectException("nonexistent_project", "_alias:*");
        mockSearchProbeFails(client, noMatchingProject);

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df", "job");
        builder.setIndices(List.of("nonexistent_project:logs-*", "logs-*"));
        builder.setProjectRouting("_alias:*");
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        ClusterState clusterState = mock(ClusterState.class);

        AtomicReference<Exception> failure = new AtomicReference<>();
        transitions.executePut(
            Intent.REPLACE,
            request,
            clusterState,
            threadPool,
            null,
            (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
            ActionListener.wrap(ignored -> fail("expected probe failure"), failure::set)
        );

        assertThat(failure.get(), instanceOf(NoMatchingProjectException.class));
        assertThat(failure.get().getMessage(), containsString("Cannot update datafeed [df]"));
        assertThat(failure.get().getMessage(), containsString("_alias:*"));
        assertThat(failure.get().getMessage(), containsString("nonexistent_project"));
        assertThat(failure.get().getCause(), equalTo(noMatchingProject));
        verify(apiKeyService, never()).grantCloudAuthentication(any(), anyString(), any());
    }

    @SuppressWarnings("unchecked")
    public void testValidateSearchBeforeMintWhenProbeFailsWithSecurityErrorShouldFail() {
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        CloudCredential callerCredential = new CloudCredential(new SecureString("caller".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(same(threadContext))).thenReturn(callerCredential);
        when(credentialManager.wrapClient(same(client), eq(callerCredential))).thenReturn(client);

        ElasticsearchSecurityException securityFailure = new ElasticsearchSecurityException("action not permitted");
        mockSearchProbeFails(client, securityFailure);

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df", "job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        ClusterState clusterState = mock(ClusterState.class);

        AtomicReference<Exception> failure = new AtomicReference<>();
        transitions.executePut(
            Intent.REPLACE,
            request,
            clusterState,
            threadPool,
            null,
            (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
            ActionListener.wrap(ignored -> fail("expected probe failure"), failure::set)
        );

        assertThat(failure.get(), equalTo(securityFailure));
        verify(apiKeyService, never()).grantCloudAuthentication(any(), anyString(), any());
    }

    @SuppressWarnings("unchecked")
    public void testExecutePutRewritesAuthenticationForOlderMinTransportVersion() throws Exception {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        CloudCredential callerCredential = new CloudCredential(new SecureString("caller".toCharArray()));
        when(credentialManager.hasCloudManagedCredential(same(threadContext))).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(same(threadContext))).thenReturn(callerCredential);
        when(credentialManager.wrapClient(same(client), eq(callerCredential))).thenReturn(client);

        mockSearchProbeSucceeds(client);
        PersistedCloudCredential persisted = new PersistedCloudCredential("minted-id", randomCloudCredentialEncryptedData());
        Authentication mintedAuth = AuthenticationTestHelper.builder().build();
        TransportVersion subjectVersion = mintedAuth.getEffectiveSubject().getTransportVersion();
        TransportVersion olderMinVersion = TransportVersion.fromId(subjectVersion.id() - 1);
        assertFalse("test pre-condition: olderMinVersion must not support subjectVersion", olderMinVersion.supports(subjectVersion));

        doAnswer(invocation -> {
            ActionListener<InternalCloudApiKeyService.CloudGrantApiKeyResult> listener = invocation.getArgument(2);
            listener.onResponse(new InternalCloudApiKeyService.CloudGrantApiKeyResult(persisted, mintedAuth));
            return null;
        }).when(apiKeyService).grantCloudAuthentication(nullable(CloudCredential.class), anyString(), any());

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df", "job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getMinTransportVersion()).thenReturn(olderMinVersion);

        AtomicReference<Map<String, String>> capturedHeaders = new AtomicReference<>();
        transitions.executePut(Intent.REPLACE, request, clusterState, threadPool, null, (req, headers, state, listener) -> {
            capturedHeaders.set(headers);
            listener.onResponse(new PutDatafeedAction.Response(req.getDatafeed()));
        }, ActionListener.wrap(ignored -> {}, e -> fail("unexpected failure: " + e)));

        Authentication decoded = AuthenticationContextSerializer.decode(capturedHeaders.get().get(AuthenticationField.AUTHENTICATION_KEY));
        assertThat(decoded.getEffectiveSubject().getTransportVersion(), equalTo(olderMinVersion));
    }

    @SuppressWarnings("unchecked")
    public void testExecutePutPersistsMintedAuthenticationHeaders() throws Exception {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, "caller-should-be-replaced");
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        CloudCredential callerCredential = new CloudCredential(new SecureString("caller".toCharArray()));
        when(credentialManager.hasCloudManagedCredential(same(threadContext))).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(same(threadContext))).thenReturn(callerCredential);
        when(credentialManager.wrapClient(same(client), eq(callerCredential))).thenReturn(client);

        mockSearchProbeSucceeds(client);
        PersistedCloudCredential persisted = new PersistedCloudCredential("minted-id", randomCloudCredentialEncryptedData());
        Authentication mintedAuth = AuthenticationTestHelper.builder().build();
        doAnswer(invocation -> {
            ActionListener<InternalCloudApiKeyService.CloudGrantApiKeyResult> listener = invocation.getArgument(2);
            listener.onResponse(new InternalCloudApiKeyService.CloudGrantApiKeyResult(persisted, mintedAuth));
            return null;
        }).when(apiKeyService).grantCloudAuthentication(nullable(CloudCredential.class), anyString(), any());

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df", "job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getMinTransportVersion()).thenReturn(TransportVersion.current());

        AtomicReference<Map<String, String>> persistedHeaders = new AtomicReference<>();
        AtomicReference<PersistedCloudCredential> persistedCred = new AtomicReference<>();
        transitions.executePut(Intent.REPLACE, request, clusterState, threadPool, null, (req, headers, state, listener) -> {
            persistedHeaders.set(headers);
            persistedCred.set(req.getDatafeed().getCloudInternalCredential());
            listener.onResponse(new PutDatafeedAction.Response(req.getDatafeed()));
        }, ActionListener.wrap(ignored -> {}, e -> fail("unexpected failure: " + e)));

        assertThat(persistedCred.get(), equalTo(persisted));
        assertThat(persistedHeaders.get().get(AuthenticationField.AUTHENTICATION_KEY), equalTo(mintedAuth.encode()));
    }

    public void testMintFailureLogThrottleShouldLogFirstFailureAndSuppressWithinInterval() {
        CredentialTransitions.MintFailureLogThrottle throttle = new CredentialTransitions.MintFailureLogThrottle();
        CredentialTransitions.MintFailureLogDecision first = throttle.recordFailure(0);
        assertThat(first.shouldLog(), is(true));
        assertThat(first.suppressedCount(), equalTo(0));

        CredentialTransitions.MintFailureLogDecision second = throttle.recordFailure(1);
        assertThat(second.shouldLog(), is(false));

        CredentialTransitions.MintFailureLogDecision third = throttle.recordFailure(2);
        assertThat(third.shouldLog(), is(false));
    }

    public void testMintFailureLogThrottleShouldReportSuppressedCountAtIntervalBoundary() {
        CredentialTransitions.MintFailureLogThrottle throttle = new CredentialTransitions.MintFailureLogThrottle();
        throttle.recordFailure(0);
        throttle.recordFailure(1);
        throttle.recordFailure(2);

        CredentialTransitions.MintFailureLogDecision atBoundary = throttle.recordFailure(
            CredentialTransitions.MINT_FAILURE_LOG_INTERVAL.millis()
        );
        assertThat(atBoundary.shouldLog(), is(true));
        assertThat(atBoundary.suppressedCount(), equalTo(2));
    }

    public void testMintFailureLogThrottleShouldBoundDistinctDatafeedBurst() {
        CredentialTransitions.MintFailureLogThrottle throttle = new CredentialTransitions.MintFailureLogThrottle();
        int logCount = 0;
        for (int i = 0; i < 1_000; i++) {
            if (throttle.recordFailure(0).shouldLog()) {
                logCount++;
            }
        }
        assertThat(logCount, equalTo(1));
    }

    public void testMintFailureLogThrottleShouldMakeOneDecisionUnderConcurrency() throws Exception {
        CredentialTransitions.MintFailureLogThrottle throttle = new CredentialTransitions.MintFailureLogThrottle();
        int threads = 20;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        AtomicInteger logCount = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            workers[t] = new Thread(() -> {
                try {
                    barrier.await();
                    if (throttle.recordFailure(0).shouldLog()) {
                        logCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failure.set(e);
                }
            });
            workers[t].start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertThat(failure.get(), equalTo(null));
        assertThat(logCount.get(), equalTo(1));
    }

    @SuppressWarnings("unchecked")
    public void testMintFailureLoggingShouldPreserveFailurePropagationWhenSuppressed() {
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(threadPool.relativeTimeInMillis()).thenReturn(1_000L);
        when(client.threadPool()).thenReturn(threadPool);

        CloudCredential callerCredential = new CloudCredential(new SecureString("caller".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(same(threadContext))).thenReturn(callerCredential);
        when(credentialManager.wrapClient(same(client), eq(callerCredential))).thenReturn(client);

        mockSearchProbeSucceeds(client);
        RuntimeException grantFailure = new RuntimeException("grant failed");
        stubGrantFailsAfterValidate(apiKeyService, grantFailure);

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        ClusterState clusterState = mock(ClusterState.class);
        AtomicReference<Exception> firstFailure = new AtomicReference<>();
        AtomicReference<Exception> secondFailure = new AtomicReference<>();

        Runnable mintTwoDatafeeds = () -> {
            DatafeedConfig.Builder firstBuilder = new DatafeedConfig.Builder("df-1", "job");
            firstBuilder.setIndices(List.of("logs-*"));
            transitions.executePut(
                Intent.REPLACE,
                new PutDatafeedAction.Request(firstBuilder.build()),
                clusterState,
                threadPool,
                null,
                (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
                ActionListener.wrap(ignored -> fail("expected mint failure"), firstFailure::set)
            );

            DatafeedConfig.Builder secondBuilder = new DatafeedConfig.Builder("df-2", "job");
            secondBuilder.setIndices(List.of("logs-*"));
            transitions.executePut(
                Intent.REPLACE,
                new PutDatafeedAction.Request(secondBuilder.build()),
                clusterState,
                threadPool,
                null,
                (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
                ActionListener.wrap(ignored -> fail("expected mint failure"), secondFailure::set)
            );
        };

        MockLog.assertThatLogger(
            mintTwoDatafeeds,
            CredentialTransitions.class,
            new MockLog.PatternSeenEventExpectation(
                "first mint failure logged at ERROR",
                CredentialTransitions.class.getCanonicalName(),
                Level.ERROR,
                ".*\\[df-1\\].*Failed to mint internal cloud API key for CPS datafeed.*"
            ),
            new MockLog.PatternNotSeenEventExpectation(
                "second mint failure suppressed within interval",
                CredentialTransitions.class.getCanonicalName(),
                Level.ERROR,
                ".*\\[df-2\\].*Failed to mint internal cloud API key for CPS datafeed.*"
            )
        );

        assertThat(firstFailure.get(), equalTo(grantFailure));
        assertThat(secondFailure.get(), equalTo(grantFailure));
    }

    @SuppressWarnings("unchecked")
    public void testMintFailureLoggingShouldIncludeSuppressedCountAfterInterval() {
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L, 1L, 2L, CredentialTransitions.MINT_FAILURE_LOG_INTERVAL.millis());
        when(client.threadPool()).thenReturn(threadPool);

        CloudCredential callerCredential = new CloudCredential(new SecureString("caller".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(same(threadContext))).thenReturn(callerCredential);
        when(credentialManager.wrapClient(same(client), eq(callerCredential))).thenReturn(client);

        mockSearchProbeSucceeds(client);
        RuntimeException grantFailure = new RuntimeException("grant failed");
        stubGrantFailsAfterValidate(apiKeyService, grantFailure);

        CredentialTransitions transitions = new CredentialTransitions(
            mock(AnomalyDetectionAuditor.class),
            () -> apiKeyService,
            () -> credentialManager,
            client,
            xContentRegistry(),
            mock(DatafeedConfigProvider.class),
            new CrossProjectModeDecider(Settings.EMPTY)
        );

        ClusterState clusterState = mock(ClusterState.class);
        Runnable mintThreeTimes = () -> {
            for (String datafeedId : List.of("df-1", "df-2", "df-3", "df-4")) {
                DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedId, "job");
                builder.setIndices(List.of("logs-*"));
                transitions.executePut(
                    Intent.REPLACE,
                    new PutDatafeedAction.Request(builder.build()),
                    clusterState,
                    threadPool,
                    null,
                    (req, headers, state, listener) -> listener.onFailure(new IllegalStateException("persist should not run")),
                    ActionListener.wrap(ignored -> fail("expected mint failure"), e -> {})
                );
            }
        };

        MockLog.assertThatLogger(
            mintThreeTimes,
            CredentialTransitions.class,
            new MockLog.PatternSeenEventExpectation(
                "first mint failure logged at ERROR",
                CredentialTransitions.class.getCanonicalName(),
                Level.ERROR,
                ".*\\[df-1\\].*Failed to mint internal cloud API key for CPS datafeed.*"
            ),
            new MockLog.PatternSeenEventExpectation(
                "interval boundary logs suppressed count",
                CredentialTransitions.class.getCanonicalName(),
                Level.ERROR,
                ".*\\[df-4\\].*Failed to mint internal cloud API key for CPS datafeed;"
                    + " suppressed \\[2\\] additional CPS datafeed mint failures since the previous report.*"
            ),
            new MockLog.PatternNotSeenEventExpectation(
                "intermediate mint failures suppressed within interval",
                CredentialTransitions.class.getCanonicalName(),
                Level.ERROR,
                ".*\\[df-2\\].*Failed to mint internal cloud API key for CPS datafeed.*"
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static void mockSearchProbeSucceeds(Client client) {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            SearchResponse response = mock(SearchResponse.class);
            when(response.status()).thenReturn(org.elasticsearch.rest.RestStatus.OK);
            when(response.getClusters()).thenReturn(SearchResponse.Clusters.EMPTY);
            when(response.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(TransportSearchAction.TYPE), any(SearchRequest.class), any());
    }

    @SuppressWarnings("unchecked")
    private static void mockSearchProbeFails(Client client, Exception failure) {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onFailure(failure);
            return null;
        }).when(client).execute(same(TransportSearchAction.TYPE), any(SearchRequest.class), any());
    }
}
