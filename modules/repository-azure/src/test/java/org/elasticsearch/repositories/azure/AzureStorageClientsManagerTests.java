/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import joptsimple.internal.Strings;

import com.azure.core.http.ProxyOptions;
import com.azure.storage.common.policy.RequestRetryOptions;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.repositories.azure.AzureStorageService.AzureStorageClientsManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.repositories.azure.AzureStorageServiceTests.encodeKey;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class AzureStorageClientsManagerTests extends ESTestCase {

    private Map<ProjectId, AtomicInteger> idGenerators;
    private List<String> clientNames;
    private Map<String, AzureStorageSettings> clusterClientsSettings;
    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private TestAzureClientProvider azureClientProvider;
    private AzureStorageService azureStorageService;
    private AzureStorageClientsManager azureClientsManager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        idGenerators = ConcurrentCollections.newConcurrentMap();
        clientNames = IntStream.range(0, between(2, 5)).mapToObj(i -> randomIdentifier() + "_" + i).toList();

        final Settings.Builder builder = Settings.builder();
        final var mockSecureSettings = new MockSecureSettings();
        clientNames.forEach(clientName -> {
            mockSecureSettings.setString(
                ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                clientName + "_cluster_account"
            );
            mockSecureSettings.setString(
                KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                encodeKey(clientName + "_cluster_key")
            );
            if (randomBoolean()) {
                builder.put("azure.client." + clientName + ".max_retries", between(1, 10));
            }
            if (randomBoolean()) {
                builder.put("azure.client." + clientName + ".timeout", between(1, 99) + "s");
            }
        });

        final Settings settings = builder.setSecureSettings(mockSecureSettings).build();
        clusterClientsSettings = AzureStorageSettings.load(settings);
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);

        azureClientProvider = new TestAzureClientProvider(between(1, 10));
        azureStorageService = new AzureStorageService(
            settings,
            azureClientProvider,
            clusterService,
            TestProjectResolvers.allProjects() // with multiple projects support
        );
        azureClientsManager = azureStorageService.getClientsManager();
        assertThat(azureClientsManager.getAllClientSettings(randomFrom(ProjectId.DEFAULT, null)), equalTo(clusterClientsSettings));
        assertNotNull(azureClientsManager.getPerProjectStorageSettings());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        threadPool.close();
    }

    public void testDoesNotCreateClientWhenSecretsAreNotConfigured() {
        assertThat(azureClientsManager.getPerProjectStorageSettings(), anEmptyMap());
        final ProjectId projectId = randomUniqueProjectId();

        // No project secrets at all
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state()).putProjectMetadata(ProjectMetadata.builder(projectId)).build()
        );
        assertThat(azureClientsManager.getPerProjectStorageSettings(), anEmptyMap());

        // Project secrets but no azure credentials
        final var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile(
            Strings.join(randomList(1, 5, ESTestCase::randomIdentifier), "."),
            randomByteArrayOfLength(between(8, 20))
        );
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state())
                .putProjectMetadata(
                    ProjectMetadata.builder(projectId)
                        .putCustom(ProjectSecrets.TYPE, new ProjectSecrets(new SecureClusterStateSettings(mockSecureSettings)))
                )
                .build()
        );
        assertThat(azureClientsManager.getPerProjectStorageSettings(), anEmptyMap());
    }

    public void testClientsLifeCycleForSingleProject() throws Exception {
        final ProjectId projectId = randomUniqueProjectId();
        final String clientName = randomFrom(clientNames);
        final String anotherClientName = randomValueOtherThan(clientName, () -> randomFrom(clientNames));

        // Configure project secrets for one client
        assertClientNotFound(projectId, clientName);
        updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName));
        {
            assertProjectClientSettings(projectId, clientName);

            // Retrieve client for the 1st time
            final var initialClient = getClientFromManager(projectId, clientName);
            assertClientCredentials(projectId, clientName, initialClient);

            // Client not configured cannot be accessed,
            assertClientNotFound(projectId, anotherClientName);

            // Update client secrets to enable another client
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName, anotherClientName));
            assertProjectClientSettings(projectId, clientName, anotherClientName);
            final var anotherClient = getClientFromManager(projectId, anotherClientName);
            assertClientCredentials(projectId, anotherClientName, anotherClient);
        }

        // Remove project secrets or the entire project
        if (randomBoolean()) {
            updateProjectInClusterState(projectId, Map.of());
        } else {
            removeProjectFromClusterState(projectId);
        }
        assertClientNotFound(projectId, clientName);

        assertThat(azureClientsManager.getPerProjectStorageSettings(), not(hasKey(projectId)));
    }

    public void testClientsWithNoCredentialsAreFilteredOut() throws IOException {
        final ProjectId projectId = randomUniqueProjectId();
        updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientNames.toArray(String[]::new)));
        for (var clientName : clientNames) {
            assertNotNull(getClientFromManager(projectId, clientName));
        }

        final List<String> clientsWithIncorrectSecretsConfig = randomNonEmptySubsetOf(clientNames);

        final Map<String, String> projectClientSecrets = new HashMap<>();
        for (var clientName : clientNames) {
            if (clientsWithIncorrectSecretsConfig.contains(clientName)) {
                projectClientSecrets.put("azure.client." + clientName + ".some_non_existing_setting", randomAlphaOfLength(12));
            } else {
                projectClientSecrets.put(
                    ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                    projectClientAccount(projectId, clientName)
                );
                projectClientSecrets.put(
                    KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                    projectClientKey(projectId, clientName)
                );
            }
        }
        updateProjectInClusterState(projectId, projectClientSecrets);
        for (var clientName : clientNames) {
            if (clientsWithIncorrectSecretsConfig.contains(clientName)) {
                assertClientNotFound(projectId, clientName);
            } else {
                assertNotNull(getClientFromManager(projectId, clientName));
            }
        }
    }

    public void testClientsForMultipleProjects() throws InterruptedException {
        final List<ProjectId> projectIds = randomList(2, 8, ESTestCase::randomUniqueProjectId);

        final List<Thread> threads = projectIds.stream().map(projectId -> new Thread(() -> {
            final int iterations = between(1, 3);
            for (var i = 0; i < iterations; i++) {
                final List<String> clientNames = randomNonEmptySubsetOf(this.clientNames);
                updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientNames.toArray(String[]::new)));

                assertProjectClientSettings(projectId, clientNames.toArray(String[]::new));
                for (var clientName : shuffledList(clientNames)) {
                    try {
                        final var azureBlobServiceClient = getClientFromManager(projectId, clientName);
                        assertClientCredentials(projectId, clientName, azureBlobServiceClient);
                    } catch (IOException e) {
                        fail(e);
                    }
                }

                if (randomBoolean()) {
                    updateProjectInClusterState(projectId, Map.of());
                } else {
                    removeProjectFromClusterState(projectId);
                }
                assertThat(azureClientsManager.getPerProjectStorageSettings(), not(hasKey(projectId)));
                clientNames.forEach(clientName -> assertClientNotFound(projectId, clientName));
            }
        })).toList();

        threads.forEach(Thread::start);
        for (var thread : threads) {
            assertTrue(thread.join(Duration.ofSeconds(10)));
        }
    }

    public void testClusterAndProjectClients() throws IOException {
        final ProjectId projectId = randomUniqueProjectId();
        final String clientName = randomFrom(clientNames);
        final boolean configureProjectClientsFirst = randomBoolean();
        if (configureProjectClientsFirst) {
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName));
        }

        final var clusterClient = getClientFromService(projectIdForClusterClient(), clientName);
        if (configureProjectClientsFirst == false) {
            assertThat(azureClientsManager.getPerProjectStorageSettings(), anEmptyMap());
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName));
        }
        final var projectClient = getClientFromService(projectId, clientName);
        assertThat(projectClient, not(sameInstance(clusterClient)));
    }

    public void testProjectClientsDisabled() throws IOException {
        final var clusterService = spy(this.clusterService);
        final var serviceWithNoProjectSupport = new AzureStorageService(
            clusterService.getSettings(),
            azureClientProvider,
            clusterService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
        verify(clusterService, never()).addHighPriorityApplier(any());
        assertNull(serviceWithNoProjectSupport.getClientsManager().getPerProjectStorageSettings());

        // Cluster client still works
        final String clientName = randomFrom(clientNames);
        assertNotNull(getClientFromService(projectIdForClusterClient(), clientName));
    }

    private TestAzureBlobServiceClient getClientFromManager(ProjectId projectId, String clientName) throws IOException {
        return asInstanceOf(
            TestAzureBlobServiceClient.class,
            azureClientsManager.client(projectId, clientName, LocationMode.PRIMARY_ONLY, randomFrom(OperationPurpose.values()), null)
        );
    }

    private TestAzureBlobServiceClient getClientFromService(ProjectId projectId, String clientName) throws IOException {
        return asInstanceOf(
            TestAzureBlobServiceClient.class,
            azureStorageService.client(projectId, clientName, LocationMode.PRIMARY_ONLY, randomFrom(OperationPurpose.values()))
        );
    }

    private ProjectId projectIdForClusterClient() {
        return randomBoolean() ? ProjectId.DEFAULT : null;
    }

    private void assertProjectClientSettings(ProjectId projectId, String... clientNames) {
        final var allClientSettingsMap = azureClientsManager.getPerProjectStorageSettings().get(projectId);
        assertNotNull(allClientSettingsMap);
        // The default client is always present when there is any other client configured
        assertThat(
            allClientSettingsMap.keySet(),
            equalTo(Stream.concat(Stream.of("default"), Stream.of(clientNames)).collect(Collectors.toSet()))
        );

        for (var clientName : clientNames) {
            final var projectClientSettings = allClientSettingsMap.get(clientName);
            final var clusterClientSettings = clusterClientsSettings.get(clientName);
            assertNotNull(clusterClientSettings);

            // Picks up the correct project scoped credentials
            final var projectClientCredentials = projectClientSettings.getConnectString();
            assertThat(projectClientCredentials, not(equalTo(clusterClientSettings.getConnectString())));
            assertThat(projectClientCredentials, containsString(";AccountName=" + projectClientAccount(projectId, clientName)));
            assertThat(projectClientCredentials, containsString(";AccountKey=" + projectClientKey(projectId, clientName)));
            // Inherit setting override from the cluster client of the same name
            assertThat(projectClientSettings.getMaxRetries(), equalTo(clusterClientSettings.getMaxRetries()));
            assertThat(projectClientSettings.getTimeout(), equalTo(clusterClientSettings.getTimeout()));
        }
    }

    private void assertClientCredentials(ProjectId projectId, String clientName, TestAzureBlobServiceClient azureBlobServiceClient) {
        final String connectString = azureBlobServiceClient.settings.getConnectString();
        assertThat(connectString, containsString(";AccountName=" + projectClientAccount(projectId, clientName)));
        assertThat(connectString, containsString(";AccountKey=" + projectClientKey(projectId, clientName)));
    }

    private void assertClientNotFound(ProjectId projectId, String clientName) {
        final SettingsException e = expectThrows(SettingsException.class, () -> getClientFromManager(projectId, clientName));
        assertThat(
            e.getMessage(),
            anyOf(
                containsString("Unable to find client with name [" + clientName + "]"),
                containsString("Unable to find any client for project [" + projectId + "]")
            )
        );
    }

    private void updateProjectInClusterState(ProjectId projectId, Map<String, String> projectClientSecrets) {
        final var mockSecureSettings = new MockSecureSettings();
        projectClientSecrets.forEach((k, v) -> mockSecureSettings.setFile(k, v.getBytes(StandardCharsets.UTF_8)));
        // Sometimes add an unrelated project secret
        if (randomBoolean() && randomBoolean()) {
            mockSecureSettings.setFile(
                Strings.join(randomList(1, 5, ESTestCase::randomIdentifier), "."),
                randomByteArrayOfLength(between(18, 20))
            );
        }
        final var secureClusterStateSettings = new SecureClusterStateSettings(mockSecureSettings);

        synchronized (this) {
            final ClusterState initialState = clusterService.state();
            final ProjectMetadata.Builder projectBuilder = initialState.metadata().hasProject(projectId)
                ? ProjectMetadata.builder(initialState.metadata().getProject(projectId))
                : ProjectMetadata.builder(projectId);

            if (secureClusterStateSettings.getSettingNames().isEmpty() == false
                || projectBuilder.getCustom(ProjectSecrets.TYPE) != null
                || randomBoolean()) {
                projectBuilder.putCustom(ProjectSecrets.TYPE, new ProjectSecrets(secureClusterStateSettings));
            }

            final ClusterState stateWithProject = ClusterState.builder(initialState).putProjectMetadata(projectBuilder).build();
            ClusterServiceUtils.setState(clusterService, stateWithProject);
        }
    }

    private void removeProjectFromClusterState(ProjectId projectId) {
        synchronized (this) {
            final ClusterState initialState = clusterService.state();
            final ClusterState stateWithoutProject = ClusterState.builder(initialState)
                .metadata(Metadata.builder(initialState.metadata()).removeProject(projectId))
                .routingTable(GlobalRoutingTable.builder(initialState.globalRoutingTable()).removeProject(projectId).build())
                .build();
            ClusterServiceUtils.setState(clusterService, stateWithoutProject);
        }
    }

    private Map<String, String> newProjectClientsSecrets(ProjectId projectId, String... clientNames) {
        idGenerators.computeIfAbsent(projectId, ignored -> new AtomicInteger(0)).incrementAndGet();
        final Map<String, String> m = new HashMap<>();
        Arrays.stream(clientNames).forEach(clientName -> {
            m.put(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), projectClientAccount(projectId, clientName));
            m.put(KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), projectClientKey(projectId, clientName));
        });
        return Map.copyOf(m);
    }

    private String projectClientAccount(ProjectId projectId, String clientName) {
        return projectId + "_" + clientName + "_account_" + idGenerators.get(projectId).get();
    }

    private String projectClientKey(ProjectId projectId, String clientName) {
        return encodeKey(projectId + "_" + clientName + "_key_" + idGenerators.get(projectId).get());
    }

    private String repoNameForClient(String clientName) {
        return "repo_for_" + clientName;
    }

    static class TestAzureClientProvider extends AzureClientProvider {

        TestAzureClientProvider(int multipartUploadMaxConcurrency) {
            super(null, null, null, null, null, multipartUploadMaxConcurrency);
        }

        @Override
        TestAzureBlobServiceClient createClient(
            AzureStorageSettings settings,
            LocationMode locationMode,
            RequestRetryOptions retryOptions,
            ProxyOptions proxyOptions,
            RequestMetricsHandler requestMetricsHandler,
            OperationPurpose purpose
        ) {
            return new TestAzureBlobServiceClient(settings);
        }
    }

    static class TestAzureBlobServiceClient extends AzureBlobServiceClient {
        final AzureStorageSettings settings;

        TestAzureBlobServiceClient(AzureStorageSettings settings) {
            super(null, null, settings.getMaxRetries(), null);
            this.settings = settings;
        }
    }
}
