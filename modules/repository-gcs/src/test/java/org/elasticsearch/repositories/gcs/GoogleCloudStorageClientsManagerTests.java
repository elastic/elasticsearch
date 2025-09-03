/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import fixture.gcs.TestUtils;
import joptsimple.internal.Strings;

import com.google.auth.oauth2.ServiceAccountCredentials;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.repositories.gcs.GoogleCloudStorageService.GoogleCloudStorageClientsManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class GoogleCloudStorageClientsManagerTests extends ESTestCase {

    private Map<ProjectId, AtomicInteger> privateKeyIdGenerators;
    private GcsRepositoryStatsCollector statsCollector;
    private List<String> clientNames;
    private Map<String, GoogleCloudStorageClientSettings> clusterClientsSettings;
    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private GoogleCloudStorageService googleCloudStorageService;
    private GoogleCloudStorageClientsManager gcsClientsManager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        privateKeyIdGenerators = ConcurrentCollections.newConcurrentMap();
        statsCollector = new GcsRepositoryStatsCollector();
        clientNames = Stream.concat(Stream.of("default"), IntStream.range(0, between(1, 4)).mapToObj(i -> randomIdentifier() + "_" + i))
            .toList();

        final Settings.Builder builder = Settings.builder();
        final var mockSecureSettings = new MockSecureSettings();
        clientNames.forEach(clientName -> {
            mockSecureSettings.setFile(
                CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                TestUtils.createServiceAccount(random())
            );
            if (randomBoolean()) {
                builder.put("gcs.client." + clientName + ".token_uri", "https://" + randomAlphaOfLength(12) + ".com");
            }
            if (randomBoolean()) {
                builder.put("gcs.client." + clientName + ".read_timeout", between(1, 99) + "s");
            }
            if (randomBoolean()) {
                builder.put("gcs.client." + clientName + ".connect_timeout", between(10, 60) + "s");
            }
        });

        final Settings settings = builder.setSecureSettings(mockSecureSettings).build();
        clusterClientsSettings = GoogleCloudStorageClientSettings.load(settings);
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
        googleCloudStorageService = new GoogleCloudStorageService(
            clusterService,
            TestProjectResolvers.allProjects() // with multiple projects support
        );
        googleCloudStorageService.refreshAndClearCache(clusterClientsSettings);
        gcsClientsManager = googleCloudStorageService.getClientsManager();
        assertThat(gcsClientsManager.getClusterClientsHolder().allClientSettings(), equalTo(clusterClientsSettings));
        assertNotNull(gcsClientsManager.getPerProjectClientsHolders());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        threadPool.close();
    }

    public void testDoesNotCreateClientWhenSecretsAreNotConfigured() {
        assertThat(gcsClientsManager.getPerProjectClientsHolders(), anEmptyMap());
        final ProjectId projectId = randomUniqueProjectId();

        // No project secrets at all
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state()).putProjectMetadata(ProjectMetadata.builder(projectId)).build()
        );
        assertThat(gcsClientsManager.getPerProjectClientsHolders(), anEmptyMap());

        // Project secrets but no gcs credentials
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
        assertThat(gcsClientsManager.getPerProjectClientsHolders(), anEmptyMap());
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
            // Client is cached when retrieved again
            assertThat(initialClient, sameInstance(getClientFromManager(projectId, clientName)));

            // Client not configured cannot be accessed,
            assertClientNotFound(projectId, anotherClientName);

            // Update client secrets should release and recreate the client
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName, anotherClientName));
            assertProjectClientSettings(projectId, clientName, anotherClientName);
            final var clientUpdated = getClientFromManager(projectId, clientName);
            assertThat(clientUpdated, not(sameInstance(initialClient)));

            // A different client for a different client name
            final var anotherClient = getClientFromManager(projectId, anotherClientName);
            assertClientCredentials(projectId, anotherClientName, anotherClient);
            assertThat(anotherClient, not(sameInstance(clientUpdated)));
        }

        // Remove project secrets or the entire project
        if (randomBoolean()) {
            updateProjectInClusterState(projectId, Map.of());
        } else {
            removeProjectFromClusterState(projectId);
        }
        assertClientNotFound(projectId, clientName);

        assertThat(gcsClientsManager.getPerProjectClientsHolders(), not(hasKey(projectId)));
    }

    public void testClientsWithNoCredentialsAreFilteredOut() throws IOException {
        final ProjectId projectId = randomUniqueProjectId();
        updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientNames.toArray(String[]::new)));
        for (var clientName : clientNames) {
            assertNotNull(getClientFromManager(projectId, clientName));
        }

        final List<String> clientsWithIncorrectSecretsConfig = randomNonEmptySubsetOf(clientNames);

        updateProjectInClusterState(projectId, clientNames.stream().collect(Collectors.toUnmodifiableMap(clientName -> {
            if (clientsWithIncorrectSecretsConfig.contains(clientName)) {
                return "gcs.client." + clientName + ".some_non_existing_setting";
            } else {
                return CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).getKey();
            }
        }, clientName -> TestUtils.createServiceAccount(random(), projectClientPrivateKeyId(projectId, clientName)))));

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
                        final var meteredStorage = getClientFromManager(projectId, clientName);
                        assertClientCredentials(projectId, clientName, meteredStorage);
                    } catch (IOException e) {
                        fail(e);
                    }
                }

                if (randomBoolean()) {
                    updateProjectInClusterState(projectId, Map.of());
                } else {
                    removeProjectFromClusterState(projectId);
                }
                assertThat(gcsClientsManager.getPerProjectClientsHolders(), not(hasKey(projectId)));
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
            assertThat(gcsClientsManager.getPerProjectClientsHolders(), anEmptyMap());
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName));
        }
        final var projectClient = getClientFromService(projectId, clientName);
        assertThat(projectClient, not(sameInstance(clusterClient)));

        // Release the cluster client
        googleCloudStorageService.closeRepositoryClients(projectIdForClusterClient(), repoNameForClient(clientName));
        assertFalse(
            googleCloudStorageService.getClientsManager()
                .getClusterClientsHolder()
                .hasCachedClientForRepository(repoNameForClient(clientName))
        );

        // Release the project client
        googleCloudStorageService.closeRepositoryClients(projectId, repoNameForClient(clientName));
        assertFalse(
            googleCloudStorageService.getClientsManager()
                .getPerProjectClientsHolders()
                .get(projectId)
                .hasCachedClientForRepository(repoNameForClient(clientName))
        );
    }

    public void testProjectClientsDisabled() throws IOException {
        final var clusterService = spy(this.clusterService);
        final var serviceWithNoProjectSupport = new GoogleCloudStorageService(clusterService, TestProjectResolvers.DEFAULT_PROJECT_ONLY);
        serviceWithNoProjectSupport.refreshAndClearCache(GoogleCloudStorageClientSettings.load(clusterService.getSettings()));
        verify(clusterService, never()).addHighPriorityApplier(any());
        assertNull(serviceWithNoProjectSupport.getClientsManager().getPerProjectClientsHolders());

        // Cluster client still works
        final String clientName = randomFrom(clientNames);
        assertNotNull(getClientFromService(projectIdForClusterClient(), clientName));
    }

    private MeteredStorage getClientFromManager(ProjectId projectId, String clientName) throws IOException {
        return gcsClientsManager.client(projectId, clientName, repoNameForClient(clientName), statsCollector);
    }

    private MeteredStorage getClientFromService(ProjectId projectId, String clientName) throws IOException {
        return googleCloudStorageService.client(projectId, clientName, repoNameForClient(clientName), statsCollector);
    }

    private ProjectId projectIdForClusterClient() {
        return randomBoolean() ? ProjectId.DEFAULT : null;
    }

    private void assertProjectClientSettings(ProjectId projectId, String... clientNames) {
        final var clientsHolder = gcsClientsManager.getPerProjectClientsHolders().get(projectId);
        assertNotNull(clientsHolder);
        final Map<String, GoogleCloudStorageClientSettings> allClientSettingsMap = clientsHolder.allClientSettings();
        assertThat(allClientSettingsMap.keySet(), containsInAnyOrder(clientNames));

        for (var clientName : clientNames) {
            final var projectClientSettings = allClientSettingsMap.get(clientName);
            final var clusterClientSettings = clusterClientsSettings.get(clientName);
            assertNotNull(clusterClientSettings);

            // Picks up the correct project scoped credentials
            final var projectClientCredentials = projectClientSettings.getCredential();
            assertThat(projectClientCredentials, not(equalTo(clusterClientSettings.getCredential())));
            assertThat(projectClientCredentials.getPrivateKeyId(), equalTo(projectClientPrivateKeyId(projectId, clientName)));
            // Inherit setting override from the cluster client of the same name
            assertThat(projectClientSettings.getTokenUri(), equalTo(clusterClientSettings.getTokenUri()));
            assertThat(projectClientSettings.getReadTimeout(), equalTo(clusterClientSettings.getReadTimeout()));
            assertThat(projectClientSettings.getConnectTimeout(), equalTo(clusterClientSettings.getConnectTimeout()));
        }
    }

    private void assertClientCredentials(ProjectId projectId, String clientName, MeteredStorage meteredStorage) {
        final var credentials = asInstanceOf(ServiceAccountCredentials.class, meteredStorage.getOptions().getCredentials());
        assertThat(credentials.getPrivateKeyId(), equalTo(projectClientPrivateKeyId(projectId, clientName)));
    }

    private void assertClientNotFound(ProjectId projectId, String clientName) {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> getClientFromManager(projectId, clientName));
        assertThat(
            e.getMessage(),
            anyOf(
                containsString("Unknown client name [" + clientName + "]"),
                containsString("No GCS client is configured for project [" + projectId + "]")
            )
        );
    }

    private void updateProjectInClusterState(ProjectId projectId, Map<String, byte[]> projectClientSecrets) {
        final var mockSecureSettings = new MockSecureSettings();
        projectClientSecrets.forEach(mockSecureSettings::setFile);
        // Sometimes add an unrelated project secret
        if (randomBoolean() && randomBoolean()) {
            mockSecureSettings.setFile(
                Strings.join(randomList(1, 5, ESTestCase::randomIdentifier), "."),
                randomByteArrayOfLength(between(8, 20))
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

    private Map<String, byte[]> newProjectClientsSecrets(ProjectId projectId, String... clientNames) {
        privateKeyIdGenerators.computeIfAbsent(projectId, ignored -> new AtomicInteger(0)).incrementAndGet();
        final Map<String, byte[]> m = new HashMap<>();
        Arrays.stream(clientNames).forEach(clientName -> {
            m.put(
                CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                TestUtils.createServiceAccount(random(), projectClientPrivateKeyId(projectId, clientName))
            );
        });
        return Map.copyOf(m);
    }

    private String projectClientPrivateKeyId(ProjectId projectId, String clientName) {
        return projectId + "_" + clientName + "_private_key_id_" + privateKeyIdGenerators.get(projectId).get();
    }

    private String repoNameForClient(String clientName) {
        return "repo_for_" + clientName;
    }
}
