/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import joptsimple.internal.Strings;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.regions.Region;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class S3ClientsManagerTests extends ESTestCase {

    private Map<ProjectId, AtomicInteger> s3SecretsIdGenerators;
    private List<String> clientNames;
    private Map<String, S3ClientSettings> clusterClientsSettings;
    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private S3Service s3Service;
    private S3ClientsManager s3ClientsManager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        s3SecretsIdGenerators = ConcurrentCollections.newConcurrentMap();
        clientNames = IntStream.range(0, between(2, 5)).mapToObj(i -> randomIdentifier() + "_" + i).toList();

        final Settings.Builder builder = Settings.builder();
        final var mockSecureSettings = new MockSecureSettings();
        clientNames.forEach(clientName -> {
            mockSecureSettings.setString("s3.client." + clientName + ".access_key", clientName + "_cluster_access_key");
            mockSecureSettings.setString("s3.client." + clientName + ".secret_key", clientName + "_cluster_secret_key");
            if (randomBoolean()) {
                builder.put("s3.client." + clientName + ".max_retries", between(1, 10));
            }
            if (randomBoolean()) {
                builder.put("s3.client." + clientName + ".read_timeout", between(1, 99) + "s");
            }
            if (randomBoolean()) {
                builder.put("s3.client." + clientName + ".max_connections", between(1, 100));
            }
        });

        final Settings settings = builder.setSecureSettings(mockSecureSettings).build();
        clusterClientsSettings = S3ClientSettings.load(settings);
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
        s3Service = new S3Service(
            mock(Environment.class),
            clusterService,
            TestProjectResolvers.allProjects(), // with multiple projects support
            mock(ResourceWatcherService.class),
            () -> Region.of("es-test-region")
        );
        s3Service.refreshAndClearCache(S3ClientSettings.load(settings));
        s3ClientsManager = s3Service.getS3ClientsManager();
        assertThat(s3ClientsManager.getClusterClientsHolder().allClientSettings(), equalTo(clusterClientsSettings));
        assertNotNull(s3ClientsManager.getPerProjectClientsHolders());
        s3Service.start();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        s3Service.close();
        clusterService.close();
        threadPool.close();
        assertTrue(s3ClientsManager.isManagerClosed());
        s3ClientsManager.getPerProjectClientsHolders().forEach((projectId, clientsHolder) -> assertTrue(clientsHolder.isClosed()));
        assertTrue(s3ClientsManager.getClusterClientsHolder().isClosed());
    }

    public void testDoesNotCreateClientWhenSecretsAreNotConfigured() {
        assertThat(s3ClientsManager.getPerProjectClientsHolders(), anEmptyMap());
        final ProjectId projectId = randomUniqueProjectId();

        // No project secrets at all
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state()).putProjectMetadata(ProjectMetadata.builder(projectId)).build()
        );
        assertThat(s3ClientsManager.getPerProjectClientsHolders(), anEmptyMap());

        // Project secrets but no s3 credentials
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
        assertThat(s3ClientsManager.getPerProjectClientsHolders(), anEmptyMap());
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
            final AmazonS3Reference initialClient = s3ClientsManager.client(projectId, createRepositoryMetadata(clientName));
            assertClientCredentials(projectId, clientName, initialClient);
            // Client is cached when retrieved again
            assertThat(initialClient, sameInstance(s3ClientsManager.client(projectId, createRepositoryMetadata(clientName))));

            // Client not configured cannot be accessed
            assertClientNotFound(projectId, anotherClientName);

            // Client should be released and recreated again on access
            s3ClientsManager.releaseCachedClients(projectId);
            final AmazonS3Reference clientUpdated = s3ClientsManager.client(projectId, createRepositoryMetadata(clientName));
            assertThat(clientUpdated, not(sameInstance(initialClient)));
            clientUpdated.decRef();

            // Release the initial client and all references should be cleared
            initialClient.decRef();
            initialClient.decRef();
            assertFalse(initialClient.hasReferences());

            // Update client secrets should release and recreate the client
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName, anotherClientName));
            assertProjectClientSettings(projectId, clientName, anotherClientName);
            final AmazonS3Reference clientUpdateAgain = s3ClientsManager.client(projectId, createRepositoryMetadata(clientName));
            assertThat(clientUpdateAgain, not(sameInstance(clientUpdated)));
            clientUpdateAgain.decRef();

            // A different client for a different client name
            final AmazonS3Reference antherClient = s3ClientsManager.client(projectId, createRepositoryMetadata(anotherClientName));
            assertClientCredentials(projectId, anotherClientName, antherClient);
            assertThat(antherClient, not(sameInstance(clientUpdateAgain)));
            antherClient.decRef();
        }

        final var clientsHolder = s3ClientsManager.getPerProjectClientsHolders().get(projectId);

        // Remove project secrets or the entire project
        if (randomBoolean()) {
            updateProjectInClusterState(projectId, Map.of());
        } else {
            removeProjectFromClusterState(projectId);
        }
        assertClientNotFound(projectId, clientName);

        assertBusy(() -> assertTrue(clientsHolder.isClosed()));
        final var e = expectThrows(
            IllegalStateException.class,
            () -> clientsHolder.client(createRepositoryMetadata(randomFrom(clientName, anotherClientName)))
        );
        assertThat(e.getMessage(), containsString("Project [" + projectId + "] clients holder is closed"));
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
                    try (var clientRef = s3ClientsManager.client(projectId, createRepositoryMetadata(clientName))) {
                        assertClientCredentials(projectId, clientName, clientRef);
                    }
                }

                if (randomBoolean()) {
                    final Map<String, AmazonS3Reference> previousClientRefs = clientNames.stream()
                        .map(clientName -> Map.entry(clientName, s3ClientsManager.client(projectId, createRepositoryMetadata(clientName))))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    s3ClientsManager.releaseCachedClients(projectId);
                    previousClientRefs.forEach((clientName, previousClientRef) -> {
                        final AmazonS3Reference currentClientRef = s3ClientsManager.client(projectId, createRepositoryMetadata(clientName));
                        assertThat(currentClientRef, not(sameInstance(previousClientRef)));
                        assertClientCredentials(projectId, clientName, currentClientRef);
                        currentClientRef.decRef();
                        previousClientRef.decRef();
                    });
                } else if (randomBoolean()) {
                    if (randomBoolean()) {
                        updateProjectInClusterState(projectId, Map.of());
                    } else {
                        removeProjectFromClusterState(projectId);
                    }
                    assertThat(s3ClientsManager.getPerProjectClientsHolders(), not(hasKey(projectId)));
                    clientNames.forEach(clientName -> assertClientNotFound(projectId, clientName));
                }
            }
        })).toList();

        threads.forEach(Thread::start);
        for (var thread : threads) {
            assertTrue(thread.join(Duration.ofSeconds(10)));
        }
    }

    public void testClusterAndProjectClients() {
        final ProjectId projectId = randomUniqueProjectId();
        final String clientName = randomFrom(clientNames);
        final boolean configureProjectClientsFirst = randomBoolean();
        if (configureProjectClientsFirst) {
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName));
        }

        final var repositoryMetadata = new RepositoryMetadata(
            randomIdentifier(),
            "s3",
            Settings.builder().put("client", clientName).build()
        );

        final AmazonS3Reference clusterClient = s3Service.client(projectIdForClusterClient(), repositoryMetadata);
        if (configureProjectClientsFirst == false) {
            assertThat(s3ClientsManager.getPerProjectClientsHolders(), anEmptyMap());
        }
        clusterClient.decRef();

        if (configureProjectClientsFirst == false) {
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName));
        }
        final AmazonS3Reference projectClient = s3Service.client(projectId, repositoryMetadata);
        assertThat(projectClient, not(sameInstance(clusterClient)));
        projectClient.decRef();

        // Release the cluster client
        s3Service.onBlobStoreClose(projectIdForClusterClient());
        assertFalse(clusterClient.hasReferences());
        assertTrue(projectClient.hasReferences());

        // Release the project client
        s3Service.onBlobStoreClose(projectId);
        assertFalse(projectClient.hasReferences());
    }

    public void testClientsHolderAfterManagerClosed() {
        final ProjectId projectId = randomUniqueProjectId();
        final String clientName = randomFrom(clientNames);

        s3ClientsManager.close();
        // New holder can be added after the manager is closed, but no actual client can be created
        updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName));
        try (var clientsHolder = s3ClientsManager.getPerProjectClientsHolders().get(projectId)) {
            assertNotNull(clientsHolder);
            assertFalse(clientsHolder.isClosed());

            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> s3ClientsManager.client(projectId, createRepositoryMetadata(clientName))
            );
            assertThat(e.getMessage(), containsString("s3 clients manager is closed"));
        }
    }

    public void testProjectClientsDisabled() {
        final var clusterService = spy(this.clusterService);
        final S3Service s3ServiceWithNoProjectSupport = new S3Service(
            mock(Environment.class),
            clusterService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(ResourceWatcherService.class),
            () -> Region.of("es-test-region")
        );
        s3ServiceWithNoProjectSupport.refreshAndClearCache(S3ClientSettings.load(clusterService.getSettings()));
        s3ServiceWithNoProjectSupport.start();
        verify(clusterService, never()).addHighPriorityApplier(any());
        assertNull(s3ServiceWithNoProjectSupport.getS3ClientsManager().getPerProjectClientsHolders());

        // Cluster client still works
        final String clientName = randomFrom(clientNames);
        final var repositoryMetadata = new RepositoryMetadata(
            randomIdentifier(),
            "s3",
            Settings.builder().put("client", clientName).build()
        );
        final AmazonS3Reference clientRef = s3ServiceWithNoProjectSupport.client(projectIdForClusterClient(), repositoryMetadata);
        clientRef.decRef();
        s3ServiceWithNoProjectSupport.close();
        assertFalse(clientRef.hasReferences());
    }

    private ProjectId projectIdForClusterClient() {
        return randomBoolean() ? ProjectId.DEFAULT : null;
    }

    private void assertProjectClientSettings(ProjectId projectId, String... clientNames) {
        final var clientsHolder = s3ClientsManager.getPerProjectClientsHolders().get(projectId);
        assertNotNull(clientsHolder);
        final Map<String, S3ClientSettings> s3ClientSettingsMap = clientsHolder.allClientSettings();
        assertThat(s3ClientSettingsMap.keySet(), containsInAnyOrder(clientNames));

        for (var clientName : clientNames) {
            final S3ClientSettings projectClientSettings = s3ClientSettingsMap.get(clientName);
            final S3ClientSettings clusterClientSettings = clusterClientsSettings.get(clientName);
            assertNotNull(clusterClientSettings);

            // Picks up the correct project scoped credentials
            assertThat(
                projectClientSettings.credentials,
                equalTo(
                    AwsBasicCredentials.create(projectClientAccessKey(projectId, clientName), projectClientSecretKey(projectId, clientName))
                )
            );
            assertThat(projectClientSettings.credentials, not(equalTo(clusterClientSettings.credentials)));
            // Inherit setting override from the cluster client of the same name
            assertThat(projectClientSettings.maxRetries, equalTo(clusterClientSettings.maxRetries));
            assertThat(projectClientSettings.maxConnections, equalTo(clusterClientSettings.maxConnections));
            assertThat(projectClientSettings.readTimeoutMillis, equalTo(clusterClientSettings.readTimeoutMillis));
        }
    }

    private void assertClientCredentials(ProjectId projectId, String clientName, AmazonS3Reference clientRef) {
        try {
            final AwsCredentialsIdentity awsCredentialsIdentity = clientRef.client()
                .serviceClientConfiguration()
                .credentialsProvider()
                .resolveIdentity()
                .get();
            assertThat(awsCredentialsIdentity.accessKeyId(), equalTo(projectClientAccessKey(projectId, clientName)));
            assertThat(awsCredentialsIdentity.secretAccessKey(), equalTo(projectClientSecretKey(projectId, clientName)));
        } catch (InterruptedException | ExecutionException e) {
            fail(e, "unexpected exception");
        }
    }

    private void assertClientNotFound(ProjectId projectId, String clientName) {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> s3ClientsManager.client(projectId, createRepositoryMetadata(clientName))
        );
        assertThat(
            e.getMessage(),
            anyOf(
                containsString("no s3 client is configured for project [" + projectId + "]"),
                containsString("s3 client [" + clientName + "] does not exist for project [" + projectId + "]")
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

    private RepositoryMetadata createRepositoryMetadata(String clientName) {
        return new RepositoryMetadata("repo", S3Repository.TYPE, Settings.builder().put("client", clientName).build());
    }

    private Map<String, String> newProjectClientsSecrets(ProjectId projectId, String... clientNames) {
        s3SecretsIdGenerators.computeIfAbsent(projectId, ignored -> new AtomicInteger(0)).incrementAndGet();
        final Map<String, String> m = new HashMap<>();
        Arrays.stream(clientNames).forEach(clientName -> {
            m.put("s3.client." + clientName + ".access_key", projectClientAccessKey(projectId, clientName));
            m.put("s3.client." + clientName + ".secret_key", projectClientSecretKey(projectId, clientName));
        });
        return Map.copyOf(m);
    }

    private String projectClientAccessKey(ProjectId projectId, String clientName) {
        return projectId + "_" + clientName + "_access_key_" + s3SecretsIdGenerators.get(projectId).get();
    }

    private String projectClientSecretKey(ProjectId projectId, String clientName) {
        return projectId + "_" + clientName + "_secret_key_" + s3SecretsIdGenerators.get(projectId).get();
    }
}
