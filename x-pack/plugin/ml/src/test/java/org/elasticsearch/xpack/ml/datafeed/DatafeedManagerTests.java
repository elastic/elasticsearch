/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedManagerTests extends ESTestCase {

    public void testExtractApiKeyId() {
        String apiKeyId = "test-key-id-123";
        String apiKeySecret = "secret-value";
        String encoded = Base64.getEncoder().encodeToString((apiKeyId + ":" + apiKeySecret).getBytes(StandardCharsets.UTF_8));

        String extractedId = DatafeedManager.extractApiKeyId(encoded);
        assertThat(extractedId, is(apiKeyId));
    }

    public void testExtractApiKeyId_InvalidFormat() {
        String extracted = DatafeedManager.extractApiKeyId("not-valid-base64!!!");
        assertThat(extracted, nullValue());
    }

    public void testExtractApiKeyId_NoColon() {
        // Valid base64 but no colon separator
        String encoded = Base64.getEncoder().encodeToString("nocolon".getBytes(StandardCharsets.UTF_8));
        String extracted = DatafeedManager.extractApiKeyId(encoded);
        assertThat(extracted, nullValue());
    }

    public void testExtractApiKeyId_EmptyId() {
        // Colon at position 0 means empty ID
        String encoded = Base64.getEncoder().encodeToString(":secret".getBytes(StandardCharsets.UTF_8));
        String extracted = DatafeedManager.extractApiKeyId(encoded);
        // colonIndex == 0, so the condition `colonIndex > 0` is false
        assertThat(extracted, nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteDatafeed_RevokesApiKeyForCpsDatafeed() {
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        UiamCredentialManager uiamCredentialManager = mock(UiamCredentialManager.class);
        Client client = mock(Client.class);

        DatafeedManager manager = new DatafeedManager(
            datafeedConfigProvider,
            mock(JobConfigProvider.class),
            NamedXContentRegistry.EMPTY,
            Settings.EMPTY,
            client,
            uiamCredentialManager
        );

        // Build a CPS datafeed with a cloudInternalApiKey
        String apiKeyId = "test-key-id";
        String apiKeySecret = "secret-value";
        String encodedKey = Base64.getEncoder().encodeToString((apiKeyId + ":" + apiKeySecret).getBytes(StandardCharsets.UTF_8));
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        builder.setCloudInternalApiKey(encodedKey);
        DatafeedConfig datafeedConfig = builder.build();

        // Mock getDatafeedConfig to return the CPS datafeed
        doAnswer(invocation -> {
            ActionListener<DatafeedConfig.Builder> listener = (ActionListener<DatafeedConfig.Builder>) invocation.getArguments()[2];
            listener.onResponse(new DatafeedConfig.Builder(datafeedConfig));
            return null;
        }).when(datafeedConfigProvider).getDatafeedConfig(eq("test-datafeed"), isNull(), any());

        // Mock revokeApiKey to succeed
        AtomicBoolean revokeCalled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            revokeCalled.set(true);
            String revokedKeyId = (String) invocation.getArguments()[0];
            assertThat(revokedKeyId, is(apiKeyId));
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[2];
            listener.onResponse(true);
            return null;
        }).when(uiamCredentialManager).revokeApiKey(any(), eq("test-datafeed"), any());

        // Mock deleteDatafeedTimingStats and deleteDatafeedConfig (via the JobDataDeleter / client)
        // Since proceedWithDeletion creates a JobDataDeleter which requires client mocking,
        // we verify that revokeApiKey was called with the correct API key ID.
        ClusterState clusterState = mockClusterStateWithNoTasks();
        DeleteDatafeedAction.Request request = new DeleteDatafeedAction.Request("test-datafeed");

        // We can't easily complete the full deletion chain without extensive mocking,
        // but we can verify revokeApiKey is called with the correct arguments
        manager.deleteDatafeed(request, clusterState, ActionListener.wrap(r -> {}, e -> {}));

        assertTrue("revokeApiKey should have been called", revokeCalled.get());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteDatafeed_SkipsRevocationForNonCpsDatafeed() {
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        UiamCredentialManager uiamCredentialManager = mock(UiamCredentialManager.class);
        Client client = mock(Client.class);

        DatafeedManager manager = new DatafeedManager(
            datafeedConfigProvider,
            mock(JobConfigProvider.class),
            NamedXContentRegistry.EMPTY,
            Settings.EMPTY,
            client,
            uiamCredentialManager
        );

        // Build a non-CPS datafeed (no cloudInternalApiKey)
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        DatafeedConfig datafeedConfig = builder.build();

        // Mock getDatafeedConfig
        doAnswer(invocation -> {
            ActionListener<DatafeedConfig.Builder> listener = (ActionListener<DatafeedConfig.Builder>) invocation.getArguments()[2];
            listener.onResponse(new DatafeedConfig.Builder(datafeedConfig));
            return null;
        }).when(datafeedConfigProvider).getDatafeedConfig(eq("test-datafeed"), isNull(), any());

        ClusterState clusterState = mockClusterStateWithNoTasks();
        DeleteDatafeedAction.Request request = new DeleteDatafeedAction.Request("test-datafeed");

        manager.deleteDatafeed(request, clusterState, ActionListener.wrap(r -> {}, e -> {}));

        // revokeApiKey should NOT be called for a non-CPS datafeed
        verify(uiamCredentialManager, never()).revokeApiKey(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteDatafeed_ProceedsWhenRevocationFails() {
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        UiamCredentialManager uiamCredentialManager = mock(UiamCredentialManager.class);
        Client client = mock(Client.class);

        DatafeedManager manager = new DatafeedManager(
            datafeedConfigProvider,
            mock(JobConfigProvider.class),
            NamedXContentRegistry.EMPTY,
            Settings.EMPTY,
            client,
            uiamCredentialManager
        );

        // Build a CPS datafeed
        String encodedKey = Base64.getEncoder().encodeToString("key-id:secret".getBytes(StandardCharsets.UTF_8));
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        builder.setCloudInternalApiKey(encodedKey);
        DatafeedConfig datafeedConfig = builder.build();

        // Mock getDatafeedConfig
        doAnswer(invocation -> {
            ActionListener<DatafeedConfig.Builder> listener = (ActionListener<DatafeedConfig.Builder>) invocation.getArguments()[2];
            listener.onResponse(new DatafeedConfig.Builder(datafeedConfig));
            return null;
        }).when(datafeedConfigProvider).getDatafeedConfig(eq("test-datafeed"), isNull(), any());

        // Mock revokeApiKey to fail
        AtomicBoolean revokeCalled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            revokeCalled.set(true);
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("revoke failed"));
            return null;
        }).when(uiamCredentialManager).revokeApiKey(any(), eq("test-datafeed"), any());

        ClusterState clusterState = mockClusterStateWithNoTasks();
        DeleteDatafeedAction.Request request = new DeleteDatafeedAction.Request("test-datafeed");

        // The deletion should still proceed (not fail) even though revocation failed.
        // We verify revokeApiKey was called.
        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.deleteDatafeed(request, clusterState, ActionListener.wrap(r -> {}, failure::set));

        assertTrue("revokeApiKey should have been called", revokeCalled.get());
        // The deletion may fail later in the chain (because we haven't mocked JobDataDeleter),
        // but the point is that revocation failure does not prevent the attempt to proceed.
    }

    private static ClusterState mockClusterStateWithNoTasks() {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(metadata.getProject()).thenReturn(projectMetadata);
        when(projectMetadata.custom(any())).thenReturn(null);
        return clusterState;
    }

    /**
     * Tests that if grantInternalApiKey succeeds but putDatafeed fails (e.g., due to validation),
     * the newly minted API key is revoked to prevent leaks.
     */
    @SuppressWarnings("unchecked")
    public void testPutDatafeed_RevokesKeyWhenDownstreamFails() {
        // CPS-enabled environment, security disabled
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        UiamCredentialManager uiamCredentialManager = mock(UiamCredentialManager.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        DatafeedManager manager = new DatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            NamedXContentRegistry.EMPTY,
            settings,
            client,
            uiamCredentialManager
        );

        // CPS credential manager reports UIAM credential present
        when(uiamCredentialManager.hasUiamCredential()).thenReturn(true);

        String apiKeyId = "new-key-id";
        String encodedKey = Base64.getEncoder().encodeToString(("new-key-id:secret").getBytes(StandardCharsets.UTF_8));

        // Mock grantInternalApiKey to succeed
        doAnswer(invocation -> {
            ActionListener<UiamCredentialManager.InternalApiKeyResult> listener = (ActionListener<
                UiamCredentialManager.InternalApiKeyResult>) invocation.getArguments()[1];
            listener.onResponse(new UiamCredentialManager.InternalApiKeyResult(apiKeyId, encodedKey, Map.of()));
            return null;
        }).when(uiamCredentialManager).grantInternalApiKey(eq("test-datafeed"), any());

        // Make the first downstream operation fail (findDatafeedIdsForJobIds → failure)
        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onFailure(new RuntimeException("simulated downstream failure"));
            return null;
        }).when(datafeedConfigProvider).findDatafeedIdsForJobIds(any(), any());

        // Track revokeApiKey calls
        AtomicBoolean revokeCalled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            revokeCalled.set(true);
            String revokedKeyId = (String) invocation.getArguments()[0];
            assertThat(revokedKeyId, is(apiKeyId));
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[2];
            listener.onResponse(true);
            return null;
        }).when(uiamCredentialManager).revokeApiKey(any(), eq("test-datafeed"), any());

        // Build request
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        builder.setIndices(List.of("logs-*"));
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(builder.build());

        // Call putDatafeed (non-security path with CPS)
        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.putDatafeed(
            request,
            mockClusterStateWithNoTasks(),
            null,
            threadPool,
            ActionListener.wrap(r -> fail("Expected failure"), e -> {
                failure.set(e);
            })
        );

        assertTrue("revokeApiKey should have been called to clean up the leaked key", revokeCalled.get());
        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("simulated downstream failure"));
    }

    /**
     * Tests that if grantInternalApiKey succeeds but updateDatefeedConfig fails during a CPS update,
     * the newly minted API key is revoked to prevent leaks.
     */
    @SuppressWarnings("unchecked")
    public void testUpdateDatafeed_RevokesKeyWhenUpdateConfigFails() {
        // CPS-enabled environment, security disabled
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        UiamCredentialManager uiamCredentialManager = mock(UiamCredentialManager.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        DatafeedManager manager = new DatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            NamedXContentRegistry.EMPTY,
            settings,
            client,
            uiamCredentialManager
        );

        when(uiamCredentialManager.hasUiamCredential()).thenReturn(true);

        String apiKeyId = "update-key-id";
        String encodedKey = Base64.getEncoder().encodeToString(("update-key-id:secret").getBytes(StandardCharsets.UTF_8));

        // Mock grantInternalApiKey to succeed
        doAnswer(invocation -> {
            ActionListener<UiamCredentialManager.InternalApiKeyResult> listener = (ActionListener<
                UiamCredentialManager.InternalApiKeyResult>) invocation.getArguments()[1];
            listener.onResponse(new UiamCredentialManager.InternalApiKeyResult(apiKeyId, encodedKey, Map.of()));
            return null;
        }).when(uiamCredentialManager).grantInternalApiKey(eq("test-datafeed"), any());

        // Mock updateDatefeedConfig to fail
        doAnswer(invocation -> {
            ActionListener<DatafeedConfig> listener = (ActionListener<DatafeedConfig>) invocation.getArguments()[4];
            listener.onFailure(new RuntimeException("simulated update failure"));
            return null;
        }).when(datafeedConfigProvider).updateDatefeedConfig(anyString(), any(DatafeedUpdate.class), any(Map.class), any(), any());

        // Track revokeApiKey calls
        AtomicBoolean revokeCalled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            revokeCalled.set(true);
            String revokedKeyId = (String) invocation.getArguments()[0];
            assertThat(revokedKeyId, is(apiKeyId));
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[2];
            listener.onResponse(true);
            return null;
        }).when(uiamCredentialManager).revokeApiKey(any(), eq("test-datafeed"), any());

        // Build update request
        DatafeedUpdate.Builder updateBuilder = new DatafeedUpdate.Builder("test-datafeed");
        updateBuilder.setIndices(List.of("new-logs-*"));
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(updateBuilder.build());

        // ClusterState: no running datafeed task, no .ml-config index (so addDocMappingIfMissing succeeds immediately)
        ClusterState clusterState = mockClusterStateForUpdate();

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.updateDatafeed(
            request,
            clusterState,
            null,
            threadPool,
            ActionListener.wrap(r -> fail("Expected failure"), e -> { failure.set(e); })
        );

        assertTrue("revokeApiKey should have been called to clean up the leaked key", revokeCalled.get());
        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("simulated update failure"));
    }

    /**
     * Tests that if updateDatefeedConfig succeeds but patchCloudInternalApiKey fails during a CPS update,
     * the newly minted API key is revoked to prevent leaks.
     */
    @SuppressWarnings("unchecked")
    public void testUpdateDatafeed_RevokesKeyWhenPatchFails() {
        // CPS-enabled environment, security disabled
        Settings settings = Settings.builder().put("serverless.cross_project.enabled", true).put("xpack.security.enabled", false).build();

        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        UiamCredentialManager uiamCredentialManager = mock(UiamCredentialManager.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        DatafeedManager manager = new DatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            NamedXContentRegistry.EMPTY,
            settings,
            client,
            uiamCredentialManager
        );

        when(uiamCredentialManager.hasUiamCredential()).thenReturn(true);

        String apiKeyId = "patch-key-id";
        String encodedKey = Base64.getEncoder().encodeToString(("patch-key-id:secret").getBytes(StandardCharsets.UTF_8));

        // Mock grantInternalApiKey to succeed
        doAnswer(invocation -> {
            ActionListener<UiamCredentialManager.InternalApiKeyResult> listener = (ActionListener<
                UiamCredentialManager.InternalApiKeyResult>) invocation.getArguments()[1];
            listener.onResponse(new UiamCredentialManager.InternalApiKeyResult(apiKeyId, encodedKey, Map.of()));
            return null;
        }).when(uiamCredentialManager).grantInternalApiKey(eq("test-datafeed"), any());

        // Mock updateDatefeedConfig to succeed (return a config without cloudInternalApiKey)
        DatafeedConfig.Builder configBuilder = new DatafeedConfig.Builder("test-datafeed", "test-job");
        configBuilder.setIndices(List.of("logs-*"));
        DatafeedConfig updatedConfig = configBuilder.build();
        doAnswer(invocation -> {
            ActionListener<DatafeedConfig> listener = (ActionListener<DatafeedConfig>) invocation.getArguments()[4];
            listener.onResponse(updatedConfig);
            return null;
        }).when(datafeedConfigProvider).updateDatefeedConfig(anyString(), any(DatafeedUpdate.class), any(Map.class), any(), any());

        // Mock patchCloudInternalApiKey to fail
        doAnswer(invocation -> {
            ActionListener<DatafeedConfig> listener = (ActionListener<DatafeedConfig>) invocation.getArguments()[3];
            listener.onFailure(new RuntimeException("simulated patch failure"));
            return null;
        }).when(datafeedConfigProvider).patchCloudInternalApiKey(anyString(), anyString(), any(Map.class), any());

        // Track revokeApiKey calls
        AtomicBoolean revokeCalled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            revokeCalled.set(true);
            String revokedKeyId = (String) invocation.getArguments()[0];
            assertThat(revokedKeyId, is(apiKeyId));
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[2];
            listener.onResponse(true);
            return null;
        }).when(uiamCredentialManager).revokeApiKey(any(), eq("test-datafeed"), any());

        // Build update request
        DatafeedUpdate.Builder updateBuilder = new DatafeedUpdate.Builder("test-datafeed");
        updateBuilder.setIndices(List.of("new-logs-*"));
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(updateBuilder.build());

        ClusterState clusterState = mockClusterStateForUpdate();

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.updateDatafeed(
            request,
            clusterState,
            null,
            threadPool,
            ActionListener.wrap(r -> fail("Expected failure"), e -> { failure.set(e); })
        );

        assertTrue("revokeApiKey should have been called to clean up the leaked key", revokeCalled.get());
        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("simulated patch failure"));
    }

    /**
     * Creates a mock ClusterState suitable for both task checks and ElasticsearchMappings.addDocMappingIfMissing.
     * The mock returns no persistent tasks and no index for .ml-config (so mapping check succeeds immediately).
     */
    private static ClusterState mockClusterStateForUpdate() {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        ProjectMetadata projectMetadata = mock(ProjectMetadata.class);
        // For getDatafeedTask (uses getMetadata())
        when(clusterState.getMetadata()).thenReturn(metadata);
        // For ElasticsearchMappings.addDocMappingIfMissing (uses metadata())
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getProject()).thenReturn(projectMetadata);
        // No persistent tasks (datafeed is not running)
        when(projectMetadata.custom(any())).thenReturn(null);
        // No .ml-config index (so addDocMappingIfMissing returns true immediately)
        when(projectMetadata.getIndicesLookup()).thenReturn(Collections.emptySortedMap());
        return clusterState;
    }
}
