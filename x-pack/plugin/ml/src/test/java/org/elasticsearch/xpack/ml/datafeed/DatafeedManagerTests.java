/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.crossproject.ProjectRoutingResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.ml.MachineLearningExtension;
import org.elasticsearch.xpack.ml.datafeed.CredentialTransitions.Change;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedManagerTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    private static MachineLearningExtension mockMlExtension(
        CloudCredentialManager credentialManager,
        InternalCloudApiKeyService apiKeyService
    ) {
        MachineLearningExtension ext = mock(MachineLearningExtension.class);
        when(ext.getCloudCredentialManager()).thenReturn(credentialManager);
        when(ext.getCloudApiKeyService()).thenReturn(apiKeyService);
        return ext;
    }

    private static AnomalyDetectionAuditor mockAuditor() {
        return mock(AnomalyDetectionAuditor.class);
    }

    /** Ensures {@link DatafeedManager} treats the config as CPS-capable for credential minting. */
    private static void withCpsSearchSurface(DatafeedConfig.Builder builder) {
        builder.setProjectRouting("_alias:_origin");
    }

    private DatafeedManager newDatafeedManager(
        DatafeedConfigProvider datafeedConfigProvider,
        JobConfigProvider jobConfigProvider,
        Settings settings,
        Client client,
        MachineLearningExtension mlExtension,
        AnomalyDetectionAuditor auditor
    ) {
        return new DatafeedManager(datafeedConfigProvider, jobConfigProvider, xContentRegistry(), settings, client, mlExtension, auditor);
    }

    private static void mockGrantSucceeds(InternalCloudApiKeyService apiKeyService, PersistedCloudCredential persisted) {
        Authentication authentication = AuthenticationTestHelper.builder().build();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<InternalCloudApiKeyService.CloudGrantApiKeyResult> listener = (ActionListener<
                InternalCloudApiKeyService.CloudGrantApiKeyResult>) invocation.getArguments()[2];
            listener.onResponse(new InternalCloudApiKeyService.CloudGrantApiKeyResult(persisted, authentication));
            return null;
        }).when(apiKeyService).grantCloudAuthentication(any(CloudCredential.class), anyString(), any());
    }

    private static void stubWrapClientForValidateProbe(CloudCredentialManager credentialManager, Client client) {
        when(credentialManager.wrapClient(same(client), nullable(CloudCredential.class))).thenReturn(client);
    }

    @SuppressWarnings("unchecked")
    private static void mockSearchProbeSucceeds(CloudCredentialManager credentialManager, Client client) {
        stubWrapClientForValidateProbe(credentialManager, client);
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
    private static void mockSearchProbeOkWithSkippedClusterSecurityFailure(CloudCredentialManager credentialManager, Client client) {
        stubWrapClientForValidateProbe(credentialManager, client);
        ShardSearchFailure failure = new ShardSearchFailure(new ElasticsearchSecurityException("action denied", RestStatus.FORBIDDEN));
        SearchResponse.Cluster cluster = new SearchResponse.Cluster(
            "linked_project",
            "linked:logs",
            false,
            SearchResponse.Cluster.Status.SKIPPED,
            null,
            null,
            null,
            null,
            List.of(failure),
            null,
            false,
            null
        );
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(Map.of("linked_project", cluster));
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            SearchResponse response = mock(SearchResponse.class);
            when(response.status()).thenReturn(RestStatus.OK);
            when(response.getClusters()).thenReturn(clusters);
            when(response.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(TransportSearchAction.TYPE), any(SearchRequest.class), any());
    }

    @SuppressWarnings("unchecked")
    private static void mockSearchProbeFails(CloudCredentialManager credentialManager, Client client, Exception failure) {
        stubWrapClientForValidateProbe(credentialManager, client);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onFailure(failure);
            return null;
        }).when(client).execute(same(TransportSearchAction.TYPE), any(SearchRequest.class), any());
    }

    @SuppressWarnings("unchecked")
    private static void mockRevokeSucceeds(InternalCloudApiKeyService apiKeyService) {
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(any(PersistedCloudCredential.class), any());
    }

    @SuppressWarnings("unchecked")
    private static void mockRevokeFails(InternalCloudApiKeyService apiKeyService, Exception failure) {
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onFailure(failure);
            return null;
        }).when(apiKeyService).revokeCloudAuthentication(any(PersistedCloudCredential.class), any());
    }

    private static void mockGrantFails(InternalCloudApiKeyService apiKeyService, Exception e) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<InternalCloudApiKeyService.CloudGrantApiKeyResult> listener = (ActionListener<
                InternalCloudApiKeyService.CloudGrantApiKeyResult>) invocation.getArguments()[2];
            listener.onFailure(e);
            return null;
        }).when(apiKeyService).grantCloudAuthentication(any(CloudCredential.class), anyString(), any());
    }

    @SuppressWarnings("unchecked")
    private static void stubGetDatafeedConfig(DatafeedConfigProvider provider, DatafeedConfig config) {
        doAnswer(invocation -> {
            ActionListener<DatafeedConfig.Builder> listener = (ActionListener<DatafeedConfig.Builder>) invocation.getArguments()[2];
            listener.onResponse(new DatafeedConfig.Builder(config));
            return null;
        }).when(provider).getDatafeedConfig(eq(config.getId()), isNull(), any());
    }

    /**
     * Simulates the {@link DatafeedConfigProvider#updateDatefeedConfigInternal} behaviour for the
     * {@link CredentialTransitions.Change.Mint} arm: applies the update to {@code storedConfig},
     * invokes the mint hook with the resulting config, then either returns a success tuple or
     * propagates {@code persistFailureOrNull} after the hook completes.
     */
    @SuppressWarnings("unchecked")
    private static void stubUpdateDatefeedConfigInvokesMintHook(
        DatafeedConfigProvider mock,
        DatafeedConfig storedConfig,
        RuntimeException persistFailureOrNull
    ) {
        doAnswer(invocation -> {
            CredentialTransitions.Change.Mint mint = invocation.getArgument(3);
            DatafeedUpdate update = invocation.getArgument(1);
            Map<String, String> headers = invocation.getArgument(2);
            DatafeedConfig applied = update.apply(storedConfig, headers, mockClusterStateForUpdate());

            ActionListener<PersistedCloudCredential> credentialListener = ActionListener.wrap(newCred -> {
                if (persistFailureOrNull != null) {
                    invocation.<ActionListener<Tuple<DatafeedConfig, PersistedCloudCredential>>>getArgument(5)
                        .onFailure(persistFailureOrNull);
                } else {
                    DatafeedConfig persisted = new DatafeedConfig.Builder(applied).setCloudInternalCredential(newCred).build();
                    invocation.<ActionListener<Tuple<DatafeedConfig, PersistedCloudCredential>>>getArgument(5)
                        .onResponse(Tuple.tuple(persisted, storedConfig.getCloudInternalCredential()));
                }
            }, e -> invocation.<ActionListener<Tuple<DatafeedConfig, PersistedCloudCredential>>>getArgument(5).onFailure(e));

            mint.mintHook().accept(applied, credentialListener);
            return null;
        }).when(mock).updateDatefeedConfig(anyString(), any(), any(), any(CredentialTransitions.Change.Mint.class), any(), any());
    }

    @SuppressWarnings("unchecked")
    private static void stubUpdateDatefeedConfigCapturesUpdateAndInvokesMintHook(
        DatafeedConfigProvider mock,
        DatafeedConfig storedConfig,
        AtomicReference<DatafeedUpdate> capturedUpdate
    ) {
        doAnswer(invocation -> {
            capturedUpdate.set(invocation.getArgument(1));
            CredentialTransitions.Change.Mint mint = invocation.getArgument(3);
            DatafeedUpdate update = invocation.getArgument(1);
            Map<String, String> headers = invocation.getArgument(2);
            DatafeedConfig applied = update.apply(storedConfig, headers, mockClusterStateForUpdate());

            ActionListener<PersistedCloudCredential> credentialListener = ActionListener.wrap(newCred -> {
                DatafeedConfig persisted = new DatafeedConfig.Builder(applied).setCloudInternalCredential(newCred).build();
                invocation.<ActionListener<Tuple<DatafeedConfig, PersistedCloudCredential>>>getArgument(5)
                    .onResponse(Tuple.tuple(persisted, storedConfig.getCloudInternalCredential()));
            }, e -> invocation.<ActionListener<Tuple<DatafeedConfig, PersistedCloudCredential>>>getArgument(5).onFailure(e));

            mint.mintHook().accept(applied, credentialListener);
            return null;
        }).when(mock).updateDatefeedConfig(anyString(), any(), any(), any(CredentialTransitions.Change.Mint.class), any(), any());
    }

    private static void stubUpdateMigrationPath(
        DatafeedConfigProvider datafeedConfigProvider,
        JobConfigProvider jobConfigProvider,
        CloudCredentialManager credentialManager,
        InternalCloudApiKeyService apiKeyService,
        Client client,
        ThreadPool threadPool,
        DatafeedConfig storedConfig,
        AtomicReference<DatafeedUpdate> capturedUpdate
    ) {
        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(new CloudCredential(new SecureString("t".toCharArray())));
        when(client.threadPool()).thenReturn(threadPool);
        mockSearchProbeSucceeds(credentialManager, client);
        mockGrantSucceeds(apiKeyService, new PersistedCloudCredential("minted-key-id", new SecureString("secret".toCharArray())));
        stubGetDatafeedConfig(datafeedConfigProvider, storedConfig);
        stubUpdateDatefeedConfigCapturesUpdateAndInvokesMintHook(datafeedConfigProvider, storedConfig, capturedUpdate);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(Boolean.TRUE);
            return null;
        }).when(jobConfigProvider).validateDatafeedJob(any(), any());
    }

    private static ClusterState mockClusterStateWithNoTasks() {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getProject()).thenReturn(projectMetadata);
        when(projectMetadata.custom(any())).thenReturn(null);
        when(projectMetadata.getIndicesLookup()).thenReturn(Collections.emptySortedMap());
        return clusterState;
    }

    @SuppressWarnings("unchecked")
    private static void stubClientForSecurityPutPath(Client client, ThreadPool threadPool) {
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocation -> {
            ActionListener<GetRollupIndexCapsAction.Response> listener = invocation.getArgument(2);
            listener.onResponse(new GetRollupIndexCapsAction.Response());
            return null;
        }).when(client).execute(same(GetRollupIndexCapsAction.INSTANCE), any(), any());
        doAnswer(invocation -> {
            ActionListener<HasPrivilegesResponse> listener = invocation.getArgument(2);
            listener.onResponse(new HasPrivilegesResponse());
            return null;
        }).when(client).execute(same(HasPrivilegesAction.INSTANCE), any(), any());
    }

    private static SecurityContext mockSecurityContextWithUser(String principal) {
        SecurityContext securityContext = mock(SecurityContext.class);
        User user = mock(User.class);
        when(user.principal()).thenReturn(principal);
        when(securityContext.getUser()).thenReturn(user);
        return securityContext;
    }

    private static Settings cpsWithSecurityEnabledSettings() {
        return Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", true).build();
    }

    @SuppressWarnings("unchecked")
    public void testPutDatafeedWithSecurityAndSecondaryAuthShouldGrantUnderSecondaryPrincipal() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", true).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        CloudCredential extractedCredential = new CloudCredential(new SecureString("caller-token".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(extractedCredential);

        PersistedCloudCredential persisted = new PersistedCloudCredential("minted-key-id-2", new SecureString("secret".toCharArray()));
        mockGrantSucceeds(apiKeyService, persisted);

        stubClientForSecurityPutPath(client, threadPool);
        mockSearchProbeSucceeds(credentialManager, client);

        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(datafeedConfigProvider).findDatafeedIdsForJobIds(any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[1];
            listener.onResponse(Boolean.TRUE);
            return null;
        }).when(jobConfigProvider).validateDatafeedJob(any(), any());

        doAnswer(invocation -> {
            ActionListener<Tuple<DatafeedConfig, DocWriteResponse>> listener = (ActionListener<
                Tuple<DatafeedConfig, DocWriteResponse>>) invocation.getArguments()[2];
            DatafeedConfig cfg = invocation.getArgument(0);
            listener.onResponse(Tuple.tuple(cfg, mock(DocWriteResponse.class)));
            return null;
        }).when(datafeedConfigProvider).putDatafeedConfig(any(), any(), any());

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(builder);
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());

        SecurityContext securityContext = mockSecurityContextWithUser("df-user");
        SecondaryAuthentication secondaryAuth = mock(SecondaryAuthentication.class);
        when(secondaryAuth.wrap(any(Runnable.class))).thenAnswer(invocation -> {
            Runnable inner = invocation.getArgument(0);
            return (Runnable) inner::run;
        });
        when(securityContext.getSecondaryAuthentication()).thenReturn(secondaryAuth);

        AtomicReference<PutDatafeedAction.Response> response = new AtomicReference<>();
        manager.putDatafeed(
            request,
            mockClusterStateForUpdate(),
            securityContext,
            threadPool,
            ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e))
        );

        assertThat(response.get().getResponse().getCloudInternalCredential(), equalTo(persisted));
        Mockito.verify(secondaryAuth, Mockito.atLeast(2)).wrap(any(Runnable.class));
        verify(credentialManager).wrapClient(same(client), eq(extractedCredential));
    }

    @SuppressWarnings("unchecked")
    public void testPutDatafeed_MintsWhenCpsEnabledAndCloudCallerWithoutRouting() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = cpsWithSecurityEnabledSettings();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        CloudCredential extractedCredential = new CloudCredential(new SecureString("caller-token".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(extractedCredential);

        PersistedCloudCredential persisted = new PersistedCloudCredential("minted-key-id", new SecureString("secret".toCharArray()));
        mockGrantSucceeds(apiKeyService, persisted);

        stubClientForSecurityPutPath(client, threadPool);
        mockSearchProbeSucceeds(credentialManager, client);

        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(datafeedConfigProvider).findDatafeedIdsForJobIds(any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[1];
            listener.onResponse(Boolean.TRUE);
            return null;
        }).when(jobConfigProvider).validateDatafeedJob(any(), any());

        doAnswer(invocation -> {
            ActionListener<Tuple<DatafeedConfig, DocWriteResponse>> listener = (ActionListener<
                Tuple<DatafeedConfig, DocWriteResponse>>) invocation.getArguments()[2];
            DatafeedConfig cfg = invocation.getArgument(0);
            listener.onResponse(Tuple.tuple(cfg, mock(DocWriteResponse.class)));
            return null;
        }).when(datafeedConfigProvider).putDatafeedConfig(any(), any(), any());

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());

        SecurityContext securityContext = mockSecurityContextWithUser("df-user");

        AtomicReference<PutDatafeedAction.Response> response = new AtomicReference<>();
        manager.putDatafeed(
            request,
            mockClusterStateForUpdate(),
            securityContext,
            threadPool,
            ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e))
        );

        assertThat(response.get().getResponse().getCloudInternalCredential(), equalTo(persisted));
        assertThat(response.get().getResponse().getProjectRouting(), equalTo(null));
    }

    /**
     * If downstream work fails after a cloud API key was granted, the minted key is revoked and the failure is propagated.
     */
    @SuppressWarnings("unchecked")
    public void testPutDatafeed_PropagatesFailureWhenDownstreamFailsAfterGrant() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = cpsWithSecurityEnabledSettings();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        CloudCredential extractedCredential = new CloudCredential(new SecureString("test-token".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(extractedCredential);

        PersistedCloudCredential persisted = new PersistedCloudCredential("new-key-id", new SecureString("secret".toCharArray()));
        mockGrantSucceeds(apiKeyService, persisted);
        mockRevokeSucceeds(apiKeyService);

        stubClientForSecurityPutPath(client, threadPool);
        mockSearchProbeSucceeds(credentialManager, client);

        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onFailure(new RuntimeException("simulated downstream failure"));
            return null;
        }).when(datafeedConfigProvider).findDatafeedIdsForJobIds(any(), any());

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(builder);
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());

        SecurityContext securityContext = mockSecurityContextWithUser("df-user");

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.putDatafeed(
            request,
            mockClusterStateWithNoTasks(),
            securityContext,
            threadPool,
            ActionListener.wrap(r -> fail("Expected failure"), failure::set)
        );

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("simulated downstream failure"));
    }

    /**
     * CPS update path uses {@link DatafeedConfigProvider#updateDatefeedConfig} with a persisted credential; if that update fails,
     * the error is propagated to the listener.
     */
    @SuppressWarnings("unchecked")
    public void testUpdateDatafeed_PropagatesFailureWhenUpdateConfigFails() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        CloudCredential extractedCredential = new CloudCredential(new SecureString("test-token".toCharArray()));
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(extractedCredential);

        PersistedCloudCredential persisted = new PersistedCloudCredential("update-key-id", new SecureString("secret".toCharArray()));
        mockGrantSucceeds(apiKeyService, persisted);
        mockRevokeSucceeds(apiKeyService);
        mockSearchProbeSucceeds(credentialManager, client);

        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(existingBuilder);
        stubGetDatafeedConfig(datafeedConfigProvider, existingBuilder.build());

        doAnswer(invocation -> {
            ActionListener<Tuple<DatafeedConfig, PersistedCloudCredential>> listener = (ActionListener<
                Tuple<DatafeedConfig, PersistedCloudCredential>>) invocation.getArguments()[5];
            listener.onFailure(new RuntimeException("simulated update failure"));
            return null;
        }).when(datafeedConfigProvider)
            .updateDatefeedConfig(anyString(), any(DatafeedUpdate.class), any(Map.class), any(Change.class), any(), any());

        DatafeedUpdate.Builder updateBuilder = new DatafeedUpdate.Builder("test-datafeed");
        updateBuilder.setIndices(List.of("new-logs-*"));
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(updateBuilder.build());

        ClusterState clusterState = mockClusterStateForUpdate();

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.updateDatafeed(request, clusterState, null, threadPool, ActionListener.wrap(r -> fail("Expected failure"), failure::set));

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("simulated update failure"));
    }

    @SuppressWarnings("unchecked")
    public void testPutDatafeedWithCpsCredential_PropagatesFailureAndLogsErrorWhenGrantFails() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = cpsWithSecurityEnabledSettings();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(new CloudCredential(new SecureString("t".toCharArray())));
        IOException grantFailure = new IOException("UIAM unreachable");
        mockGrantFails(apiKeyService, grantFailure);

        stubClientForSecurityPutPath(client, threadPool);
        mockSearchProbeSucceeds(credentialManager, client);

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(builder);
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());

        SecurityContext securityContext = mockSecurityContextWithUser("df-user");

        AtomicReference<Exception> failure = new AtomicReference<>();
        MockLog.assertThatLogger(
            () -> manager.putDatafeed(
                request,
                mockClusterStateWithNoTasks(),
                securityContext,
                threadPool,
                ActionListener.wrap(r -> fail("Expected failure"), failure::set)
            ),
            CredentialTransitions.class,
            new MockLog.PatternSeenEventExpectation(
                "grant failure logged at ERROR",
                CredentialTransitions.class.getCanonicalName(),
                Level.ERROR,
                ".*\\[test-datafeed\\].*Failed to mint internal cloud API key for CPS datafeed.*"
            )
        );

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get(), equalTo(grantFailure));
        verify(datafeedConfigProvider, never()).putDatafeedConfig(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDatafeedWithCpsCredential_PropagatesFailureAndLogsErrorWhenGrantFails() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(new CloudCredential(new SecureString("t".toCharArray())));
        IOException grantFailure = new IOException("UIAM unreachable");
        mockGrantFails(apiKeyService, grantFailure);
        mockSearchProbeSucceeds(credentialManager, client);

        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(existingBuilder);
        stubGetDatafeedConfig(datafeedConfigProvider, existingBuilder.build());
        stubUpdateDatefeedConfigInvokesMintHook(datafeedConfigProvider, existingBuilder.build(), null);

        DatafeedUpdate.Builder updateBuilder = new DatafeedUpdate.Builder("test-datafeed");
        updateBuilder.setIndices(List.of("new-logs-*"));
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(updateBuilder.build());

        AtomicReference<Exception> failure = new AtomicReference<>();
        MockLog.assertThatLogger(
            () -> manager.updateDatafeed(
                request,
                mockClusterStateForUpdate(),
                null,
                threadPool,
                ActionListener.wrap(r -> fail("Expected failure"), failure::set)
            ),
            CredentialTransitions.class,
            new MockLog.PatternSeenEventExpectation(
                "grant failure logged at ERROR on update",
                CredentialTransitions.class.getCanonicalName(),
                Level.ERROR,
                ".*\\[test-datafeed\\].*Failed to mint internal cloud API key for CPS datafeed.*"
            )
        );

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get(), equalTo(grantFailure));
    }

    @SuppressWarnings("unchecked")
    public void testPutDatafeed_DoesNotMintWhenCrossProjectClusterSettingDisabled() {
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", false).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);

        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(datafeedConfigProvider).findDatafeedIdsForJobIds(any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[1];
            listener.onResponse(Boolean.TRUE);
            return null;
        }).when(jobConfigProvider).validateDatafeedJob(any(), any());

        doAnswer(invocation -> {
            ActionListener<Tuple<DatafeedConfig, DocWriteResponse>> listener = (ActionListener<
                Tuple<DatafeedConfig, DocWriteResponse>>) invocation.getArguments()[2];
            DatafeedConfig cfg = invocation.getArgument(0);
            listener.onResponse(Tuple.tuple(cfg, mock(DocWriteResponse.class)));
            return null;
        }).when(datafeedConfigProvider).putDatafeedConfig(any(), any(), any());

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("remote:logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());

        AtomicReference<PutDatafeedAction.Response> response = new AtomicReference<>();
        manager.putDatafeed(
            request,
            mockClusterStateWithNoTasks(),
            null,
            threadPool,
            ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e))
        );

        assertThat(response.get(), notNullValue());
        assertThat(response.get().getResponse().getCloudInternalCredential(), equalTo(null));
        verify(apiKeyService, never()).grantCloudAuthentication(any(), anyString(), any());
        verify(auditor, never()).info(anyString(), anyString());
    }

    @SuppressWarnings("unchecked")
    public void testPutDatafeed_DoesNotMintWhenMlCrossProjectFeatureFlagDisabled() {
        assumeFalse(
            "Run with -Des.ml_cross_project_feature_flag_enabled=false to assert the feature-flag gate",
            CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled()
        );

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);

        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(datafeedConfigProvider).findDatafeedIdsForJobIds(any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[1];
            listener.onResponse(Boolean.TRUE);
            return null;
        }).when(jobConfigProvider).validateDatafeedJob(any(), any());

        doAnswer(invocation -> {
            ActionListener<Tuple<DatafeedConfig, DocWriteResponse>> listener = (ActionListener<
                Tuple<DatafeedConfig, DocWriteResponse>>) invocation.getArguments()[2];
            DatafeedConfig cfg = invocation.getArgument(0);
            listener.onResponse(Tuple.tuple(cfg, mock(DocWriteResponse.class)));
            return null;
        }).when(datafeedConfigProvider).putDatafeedConfig(any(), any(), any());

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("remote:logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());

        AtomicReference<PutDatafeedAction.Response> response = new AtomicReference<>();
        manager.putDatafeed(
            request,
            mockClusterStateWithNoTasks(),
            null,
            threadPool,
            ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e))
        );

        assertThat(response.get(), notNullValue());
        assertThat(response.get().getResponse().getCloudInternalCredential(), equalTo(null));
        verify(apiKeyService, never()).grantCloudAuthentication(any(), anyString(), any());
        verify(auditor, never()).info(anyString(), anyString());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDatafeedNoopShouldNotRekey() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            settings,
            client,
            mlExtension,
            mockAuditor()
        );

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);

        PersistedCloudCredential existingCred = new PersistedCloudCredential("existing-key-id", new SecureString("e".toCharArray()));
        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(existingBuilder);
        existingBuilder.setCloudInternalCredential(existingCred);
        DatafeedConfig existingConfig = existingBuilder.build();
        stubGetDatafeedConfig(datafeedConfigProvider, existingConfig);

        DatafeedConfig.Builder updatedBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        updatedBuilder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(updatedBuilder);
        updatedBuilder.setCloudInternalCredential(existingCred);
        DatafeedConfig updatedConfig = updatedBuilder.build();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<DatafeedConfig> listener = invocation.getArgument(4);
            listener.onResponse(updatedConfig);
            return null;
        }).when(datafeedConfigProvider).updateDatefeedConfig(anyString(), any(), any(), any(), any());

        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(new DatafeedUpdate.Builder("test-datafeed").build());

        AtomicReference<PutDatafeedAction.Response> response = new AtomicReference<>();
        manager.updateDatafeed(
            request,
            mockClusterStateForUpdate(),
            null,
            threadPool,
            ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e))
        );

        assertThat(response.get(), notNullValue());
        assertThat(response.get().getResponse().getCloudInternalCredential(), equalTo(existingCred));
        verify(apiKeyService, never()).grantCloudAuthentication(any(), anyString(), any());
        verify(datafeedConfigProvider, never()).updateDatefeedConfig(
            anyString(),
            any(DatafeedUpdate.class),
            any(Map.class),
            any(Change.class),
            any(),
            any()
        );
    }

    @SuppressWarnings("unchecked")
    public void testUpdateWithoutCloudCredentialClearsExistingCloudInternalCredential() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(false);

        PersistedCloudCredential existingCred = new PersistedCloudCredential("old-key-id", new SecureString("e".toCharArray()));
        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(existingBuilder);
        existingBuilder.setCloudInternalCredential(existingCred);
        stubGetDatafeedConfig(datafeedConfigProvider, existingBuilder.build());

        DatafeedConfig clearedConfig = new DatafeedConfig.Builder("test-datafeed", "test-job").setIndices(List.of("logs-*"))
            .setProjectRouting("_alias:_origin")
            .build();
        mockRevokeSucceeds(apiKeyService);
        doAnswer(invocation -> {
            assertThat(invocation.getArgument(3), equalTo(Change.CLEAR));
            ActionListener<Tuple<DatafeedConfig, PersistedCloudCredential>> listener = invocation.getArgument(5);
            listener.onResponse(Tuple.tuple(clearedConfig, existingCred));
            return null;
        }).when(datafeedConfigProvider).updateDatefeedConfig(anyString(), any(), any(), any(Change.class), any(), any());

        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(new DatafeedUpdate.Builder("test-datafeed").build());

        AtomicReference<PutDatafeedAction.Response> response = new AtomicReference<>();
        manager.updateDatafeed(
            request,
            mockClusterStateForUpdate(),
            null,
            threadPool,
            ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e))
        );

        assertThat(response.get().getResponse().getCloudInternalCredential(), equalTo(null));
        verify(apiKeyService, never()).grantCloudAuthentication(any(), anyString(), any());
        verify(apiKeyService).revokeCloudAuthentication(same(existingCred), any());
        verify(auditor).info(eq("test-job"), eq(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_CLEARED)));
        verify(auditor).info(eq("test-job"), eq(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOKED)));
    }

    @SuppressWarnings("unchecked")
    public void testUpdateWithoutCloudCredentialDoesNotStaticallyRejectProjectRouting() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            settings,
            client,
            mlExtension,
            mockAuditor()
        );

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(false);

        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("logs-*"));
        stubGetDatafeedConfig(datafeedConfigProvider, existingBuilder.build());

        DatafeedConfig updatedConfig = new DatafeedConfig.Builder("test-datafeed", "test-job").setIndices(List.of("logs-*"))
            .setProjectRouting("_origin")
            .build();

        doAnswer(invocation -> {
            ActionListener<DatafeedConfig> listener = invocation.getArgument(4);
            listener.onResponse(updatedConfig);
            return null;
        }).when(datafeedConfigProvider).updateDatefeedConfig(anyString(), any(), any(), any(), any());

        DatafeedUpdate.Builder updateBuilder = new DatafeedUpdate.Builder("test-datafeed");
        updateBuilder.setProjectRouting("_origin");
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(updateBuilder.build());

        AtomicReference<PutDatafeedAction.Response> response = new AtomicReference<>();
        manager.updateDatafeed(
            request,
            mockClusterStateForUpdate(),
            null,
            threadPool,
            ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e))
        );

        assertThat(response.get().getResponse().getProjectRouting(), equalTo("_origin"));
        verify(apiKeyService, never()).grantCloudAuthentication(any(), anyString(), any());
    }

    @SuppressWarnings("unchecked")
    public void testRevokeFailureEmitsRevocationFailedAudit() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(false);

        PersistedCloudCredential existingCred = new PersistedCloudCredential("old-key-id", new SecureString("e".toCharArray()));
        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(existingBuilder);
        existingBuilder.setCloudInternalCredential(existingCred);
        stubGetDatafeedConfig(datafeedConfigProvider, existingBuilder.build());

        DatafeedConfig clearedConfig = new DatafeedConfig.Builder("test-datafeed", "test-job").setIndices(List.of("logs-*"))
            .setProjectRouting("_alias:_origin")
            .build();
        mockRevokeFails(apiKeyService, new RuntimeException("revoke failed"));
        doAnswer(invocation -> {
            ActionListener<Tuple<DatafeedConfig, PersistedCloudCredential>> listener = invocation.getArgument(5);
            listener.onResponse(Tuple.tuple(clearedConfig, existingCred));
            return null;
        }).when(datafeedConfigProvider).updateDatefeedConfig(anyString(), any(), any(), eq(Change.CLEAR), any(), any());

        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(new DatafeedUpdate.Builder("test-datafeed").build());

        AtomicReference<PutDatafeedAction.Response> response = new AtomicReference<>();
        manager.updateDatafeed(
            request,
            mockClusterStateForUpdate(),
            null,
            threadPool,
            ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e))
        );

        assertThat(response.get().getResponse().getCloudInternalCredential(), equalTo(null));
        verify(auditor).info(eq("test-job"), eq(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOCATION_FAILED, "old-key-id")));
    }

    @SuppressWarnings("unchecked")
    public void testProbeFailureAbortsUpdateWithoutMint() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            settings,
            client,
            mlExtension,
            mockAuditor()
        );

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(new CloudCredential(new SecureString("t".toCharArray())));

        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(existingBuilder);
        stubGetDatafeedConfig(datafeedConfigProvider, existingBuilder.build());

        RuntimeException probeFailure = new RuntimeException("search probe failed");
        mockSearchProbeFails(credentialManager, client, probeFailure);
        stubUpdateDatefeedConfigInvokesMintHook(datafeedConfigProvider, existingBuilder.build(), null);

        DatafeedUpdate.Builder updateBuilder = new DatafeedUpdate.Builder("test-datafeed");
        updateBuilder.setIndices(List.of("new-logs-*"));
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(updateBuilder.build());

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.updateDatafeed(
            request,
            mockClusterStateForUpdate(),
            null,
            threadPool,
            ActionListener.wrap(r -> fail("Expected failure"), failure::set)
        );

        assertThat(failure.get(), equalTo(probeFailure));
        verify(apiKeyService, never()).grantCloudAuthentication(any(), anyString(), any());
    }

    @SuppressWarnings("unchecked")
    public void testProbeReturnsOkWithSecurityClusterFailureAbortsUpdateWithoutMint() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            settings,
            client,
            mlExtension,
            mockAuditor()
        );

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(new CloudCredential(new SecureString("t".toCharArray())));

        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(existingBuilder);
        stubGetDatafeedConfig(datafeedConfigProvider, existingBuilder.build());

        mockSearchProbeOkWithSkippedClusterSecurityFailure(credentialManager, client);
        stubUpdateDatefeedConfigInvokesMintHook(datafeedConfigProvider, existingBuilder.build(), null);

        DatafeedUpdate.Builder updateBuilder = new DatafeedUpdate.Builder("test-datafeed");
        updateBuilder.setIndices(List.of("new-logs-*"));
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(updateBuilder.build());

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.updateDatafeed(
            request,
            mockClusterStateForUpdate(),
            null,
            threadPool,
            ActionListener.wrap(r -> fail("Expected failure"), failure::set)
        );

        assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.FORBIDDEN));
        verify(apiKeyService, never()).grantCloudAuthentication(any(), anyString(), any());
    }

    @SuppressWarnings("unchecked")
    public void testProbeUsesMergedConfigNotStoredConfig() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            settings,
            client,
            mlExtension,
            mockAuditor()
        );

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(new CloudCredential(new SecureString("t".toCharArray())));

        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("stored-*"));
        withCpsSearchSurface(existingBuilder);
        stubGetDatafeedConfig(datafeedConfigProvider, existingBuilder.build());

        mockSearchProbeSucceeds(credentialManager, client);
        mockGrantFails(apiKeyService, new IOException("stop after probe"));
        stubUpdateDatefeedConfigInvokesMintHook(datafeedConfigProvider, existingBuilder.build(), null);

        DatafeedUpdate.Builder updateBuilder = new DatafeedUpdate.Builder("test-datafeed");
        updateBuilder.setIndices(List.of("merged-*"));
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(updateBuilder.build());

        manager.updateDatafeed(
            request,
            mockClusterStateForUpdate(),
            null,
            threadPool,
            ActionListener.wrap(r -> fail("Expected failure after probe"), e -> {
                assertThat(e, notNullValue());
            })
        );

        verify(client).execute(
            same(TransportSearchAction.TYPE),
            argThat((SearchRequest searchRequest) -> searchRequest.indices()[0].equals("merged-*")),
            any()
        );
    }

    @SuppressWarnings("unchecked")
    public void testProbePassesThenPersistFailsTriggersRevoke() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            settings,
            client,
            mlExtension,
            mockAuditor()
        );

        when(credentialManager.hasCloudManagedCredential(any())).thenReturn(true);
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(new CloudCredential(new SecureString("t".toCharArray())));

        DatafeedConfig.Builder existingBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        existingBuilder.setIndices(List.of("logs-*"));
        withCpsSearchSurface(existingBuilder);
        stubGetDatafeedConfig(datafeedConfigProvider, existingBuilder.build());

        mockSearchProbeSucceeds(credentialManager, client);
        PersistedCloudCredential minted = new PersistedCloudCredential("minted-key", new SecureString("s".toCharArray()));
        mockGrantSucceeds(apiKeyService, minted);
        mockRevokeSucceeds(apiKeyService);
        stubUpdateDatefeedConfigInvokesMintHook(datafeedConfigProvider, existingBuilder.build(), new RuntimeException("persist failed"));

        DatafeedUpdate.Builder updateBuilder = new DatafeedUpdate.Builder("test-datafeed");
        updateBuilder.setIndices(List.of("new-logs-*"));
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(updateBuilder.build());

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.updateDatafeed(
            request,
            mockClusterStateForUpdate(),
            null,
            threadPool,
            ActionListener.wrap(r -> fail("Expected failure"), failure::set)
        );

        assertThat(failure.get().getMessage(), containsString("persist failed"));
        verify(apiKeyService).revokeCloudAuthentication(same(minted), any());
    }

    public void testPutDatafeedWithRequestCloudCredentialShouldNotInjectIntoThreadContext() {
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, mock(InternalCloudApiKeyService.class));
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = invocation.getArgument(1);
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(datafeedConfigProvider).findDatafeedIdsForJobIds(any(), any());

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            mock(JobConfigProvider.class),
            settings,
            mock(Client.class),
            mlExtension,
            mockAuditor()
        );

        CloudCredential requestCredential = new CloudCredential(new SecureString("from-request".toCharArray()));
        when(credentialManager.hasCloudManagedCredential(threadContext)).thenReturn(false);

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        request.setCloudCredential(requestCredential);

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.putDatafeed(request, null, null, threadPool, ActionListener.wrap(r -> fail("unexpected success"), failure::set));

        verify(credentialManager, never()).injectCloudManagedCredential(any(), any());
    }

    public void testPutDatafeedWithRequestCloudCredentialShouldNotDoubleInjectWhenTransientPresent() {
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, mock(InternalCloudApiKeyService.class));
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = invocation.getArgument(1);
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(datafeedConfigProvider).findDatafeedIdsForJobIds(any(), any());

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            mock(JobConfigProvider.class),
            settings,
            mock(Client.class),
            mlExtension,
            mockAuditor()
        );

        CloudCredential requestCredential = new CloudCredential(new SecureString("from-request".toCharArray()));
        when(credentialManager.hasCloudManagedCredential(threadContext)).thenReturn(true);

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());
        request.setCloudCredential(requestCredential);

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.putDatafeed(request, null, null, threadPool, ActionListener.wrap(r -> fail("unexpected success"), failure::set));

        verify(credentialManager, never()).injectCloudManagedCredential(any(), any());
    }

    public void testCurrentCallerCredentialWithSecondaryAuthShouldExtractFromSecondaryContext() {
        assumeTrue("feature under test must be enabled", DatafeedConfig.DATAFEED_CROSS_PROJECT.isEnabled());

        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", true).build();
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, mock(InternalCloudApiKeyService.class));
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        CloudCredential secondaryCredential = new CloudCredential(new SecureString("caller-uiam-token".toCharArray()));
        when(credentialManager.hasCloudManagedCredential(any())).thenAnswer(invocation -> {
            ThreadContext ctx = invocation.getArgument(0);
            return ctx.getTransient("test_cloud_token") != null;
        });
        when(credentialManager.extractCloudManagedCredential(any())).thenAnswer(invocation -> {
            ThreadContext ctx = invocation.getArgument(0);
            if (ctx.getTransient("test_cloud_token") != null) {
                return secondaryCredential;
            }
            return null;
        });

        SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.getThreadContext()).thenReturn(threadContext);
        SecondaryAuthentication secondaryAuth = mock(SecondaryAuthentication.class);
        when(securityContext.getSecondaryAuthentication()).thenReturn(secondaryAuth);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            return (Runnable) () -> {
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    threadContext.putTransient("test_cloud_token", "present");
                    runnable.run();
                }
            };
        }).when(secondaryAuth).wrap(any());

        DatafeedManager manager = newDatafeedManager(
            mock(DatafeedConfigProvider.class),
            mock(JobConfigProvider.class),
            settings,
            mock(Client.class),
            mlExtension,
            mockAuditor()
        );

        assertThat(manager.currentCallerCredential(threadPool, securityContext), equalTo(secondaryCredential));
        verify(secondaryAuth).wrap(any());
    }

    public void testCurrentCallerCredentialWithSecondaryAuthShouldIgnorePrimaryContextToken() {
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", true).build();
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, mock(InternalCloudApiKeyService.class));
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("test_cloud_token", "primary-only");

        CloudCredential primaryCredential = new CloudCredential(new SecureString("kibana-service-token".toCharArray()));
        when(credentialManager.hasCloudManagedCredential(any())).thenAnswer(invocation -> {
            ThreadContext ctx = invocation.getArgument(0);
            return ctx.getTransient("test_cloud_token") != null;
        });
        when(credentialManager.extractCloudManagedCredential(any())).thenReturn(primaryCredential);

        SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.getThreadContext()).thenReturn(threadContext);
        SecondaryAuthentication secondaryAuth = mock(SecondaryAuthentication.class);
        when(securityContext.getSecondaryAuthentication()).thenReturn(secondaryAuth);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            return (Runnable) () -> {
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    runnable.run();
                }
            };
        }).when(secondaryAuth).wrap(any());

        DatafeedManager manager = newDatafeedManager(
            mock(DatafeedConfigProvider.class),
            mock(JobConfigProvider.class),
            settings,
            mock(Client.class),
            mlExtension,
            mockAuditor()
        );

        assertThat(manager.currentCallerCredential(threadPool, securityContext), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDatafeedFirstTimeMigrationShouldDefaultProjectRoutingToOrigin() {
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        AnomalyDetectionAuditor auditor = mockAuditor();
        DatafeedManager manager = newDatafeedManager(datafeedConfigProvider, jobConfigProvider, settings, client, mlExtension, auditor);

        DatafeedConfig legacyConfig = new DatafeedConfig.Builder("df-1", "job-1").setIndices(List.of("logs-*")).build();
        AtomicReference<DatafeedUpdate> capturedUpdate = new AtomicReference<>();
        stubUpdateMigrationPath(
            datafeedConfigProvider,
            jobConfigProvider,
            credentialManager,
            apiKeyService,
            client,
            threadPool,
            legacyConfig,
            capturedUpdate
        );

        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(new DatafeedUpdate.Builder("df-1").build());
        manager.updateDatafeed(request, mockClusterStateForUpdate(), null, threadPool, ActionTestUtils.assertNoFailureListener(r -> {}));

        assertThat(capturedUpdate.get(), notNullValue());
        assertThat(capturedUpdate.get().getProjectRouting(), equalTo(ProjectRoutingResolver.LOCAL_ONLY));
        verify(auditor).info(eq("job-1"), eq(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_MIGRATION_PROJECT_ROUTING_DEFAULTED)));
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDatafeedFirstTimeMigrationShouldNotOverrideExistingProjectRouting() {
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            settings,
            client,
            mlExtension,
            mockAuditor()
        );

        DatafeedConfig legacyConfig = new DatafeedConfig.Builder("df-2", "job-2").setIndices(List.of("logs-*"))
            .setProjectRouting("_alias:linked_project")
            .build();
        AtomicReference<DatafeedUpdate> capturedUpdate = new AtomicReference<>();
        stubUpdateMigrationPath(
            datafeedConfigProvider,
            jobConfigProvider,
            credentialManager,
            apiKeyService,
            client,
            threadPool,
            legacyConfig,
            capturedUpdate
        );

        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(new DatafeedUpdate.Builder("df-2").build());
        manager.updateDatafeed(request, mockClusterStateForUpdate(), null, threadPool, ActionTestUtils.assertNoFailureListener(r -> {}));

        assertThat(capturedUpdate.get(), notNullValue());
        assertThat(capturedUpdate.get().getProjectRouting(), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDatafeedFirstTimeMigrationShouldHonourExplicitProjectRoutingInUpdate() {
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            settings,
            client,
            mlExtension,
            mockAuditor()
        );

        DatafeedConfig legacyConfig = new DatafeedConfig.Builder("df-3", "job-3").setIndices(List.of("logs-*")).build();
        AtomicReference<DatafeedUpdate> capturedUpdate = new AtomicReference<>();
        stubUpdateMigrationPath(
            datafeedConfigProvider,
            jobConfigProvider,
            credentialManager,
            apiKeyService,
            client,
            threadPool,
            legacyConfig,
            capturedUpdate
        );

        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(
            new DatafeedUpdate.Builder("df-3").setProjectRouting("_alias:explicit").build()
        );
        manager.updateDatafeed(request, mockClusterStateForUpdate(), null, threadPool, ActionTestUtils.assertNoFailureListener(r -> {}));

        assertThat(capturedUpdate.get(), notNullValue());
        assertThat(capturedUpdate.get().getProjectRouting(), equalTo("_alias:explicit"));
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDatafeedAlreadyMigratedShouldNotDefaultRoutingOnRekey() {
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        CloudCredentialManager credentialManager = mock(CloudCredentialManager.class);
        InternalCloudApiKeyService apiKeyService = mock(InternalCloudApiKeyService.class);
        MachineLearningExtension mlExtension = mockMlExtension(credentialManager, apiKeyService);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        DatafeedManager manager = newDatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            settings,
            client,
            mlExtension,
            mockAuditor()
        );

        PersistedCloudCredential existingCred = new PersistedCloudCredential("old-key-id", new SecureString("e".toCharArray()));
        DatafeedConfig migratedConfig = new DatafeedConfig.Builder("df-4", "job-4").setIndices(List.of("logs-*"))
            .setCloudInternalCredential(existingCred)
            .build();
        AtomicReference<DatafeedUpdate> capturedUpdate = new AtomicReference<>();
        stubUpdateMigrationPath(
            datafeedConfigProvider,
            jobConfigProvider,
            credentialManager,
            apiKeyService,
            client,
            threadPool,
            migratedConfig,
            capturedUpdate
        );
        mockRevokeSucceeds(apiKeyService);

        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(
            new DatafeedUpdate.Builder("df-4").setIndices(List.of("new-logs-*")).build()
        );
        manager.updateDatafeed(request, mockClusterStateForUpdate(), null, threadPool, ActionTestUtils.assertNoFailureListener(r -> {}));

        assertThat(capturedUpdate.get(), notNullValue());
        assertThat(capturedUpdate.get().getProjectRouting(), nullValue());
    }

    /**
     * Creates a mock ClusterState suitable for both task checks and ElasticsearchMappings.addDocMappingIfMissing.
     */
    private static ClusterState mockClusterStateForUpdate() {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getProject()).thenReturn(projectMetadata);
        when(projectMetadata.custom(any())).thenReturn(null);
        when(projectMetadata.getIndicesLookup()).thenReturn(Collections.emptySortedMap());
        return clusterState;
    }
}
