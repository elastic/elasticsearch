/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.ml.datafeed.CredentialTransitions.Change;
import org.elasticsearch.xpack.ml.datafeed.CredentialTransitions.Intent;
import org.elasticsearch.xpack.ml.datafeed.CredentialTransitions.TransitionContext;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.same;
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
        return new TransitionContext(crossProjectEnabled, callerHasCloudCredential, envelopeExists, affectsCrossProjectSearchSurface);
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
        BiConsumer<DatafeedConfig, ActionListener<PersistedCloudCredential>> hook = (config, listener) -> {};
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
        when(credentialManager.hasCloudManagedCredential(same(threadContext))).thenReturn(true);
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
            mock(DatafeedConfigProvider.class)
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

        when(credentialManager.hasCloudManagedCredential(same(threadContext))).thenReturn(false);
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
            mock(DatafeedConfigProvider.class)
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
        verify(credentialManager, never()).extractCloudManagedCredential(any());
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
        when(credentialManager.hasCloudManagedCredential(same(threadContext))).thenReturn(true);
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
            mock(DatafeedConfigProvider.class)
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
        when(credentialManager.hasCloudManagedCredential(same(threadContext))).thenReturn(false);
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
            mock(DatafeedConfigProvider.class)
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
}
