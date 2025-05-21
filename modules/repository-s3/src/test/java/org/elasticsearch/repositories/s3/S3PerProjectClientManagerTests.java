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
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import static org.mockito.Mockito.mock;

public class S3PerProjectClientManagerTests extends ESTestCase {

    private Map<ProjectId, AtomicInteger> s3SecretsIdGenerators;
    private List<String> clientNames;
    private Map<String, S3ClientSettings> clusterClientsSettings;
    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private S3Service s3Service;
    private S3PerProjectClientManager s3PerProjectClientManager;
    private final AtomicReference<CountDownLatch> clientRefsCloseLatchRef = new AtomicReference<>();
    private final AtomicBoolean closeInternalInvoked = new AtomicBoolean(false);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        s3SecretsIdGenerators = ConcurrentCollections.newConcurrentMap();
        clientRefsCloseLatchRef.set(null);
        closeInternalInvoked.set(false);
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
            TestProjectResolvers.allProjects(),
            mock(ResourceWatcherService.class),
            () -> Region.of("es-test-region")
        ) {
            @Override
            protected AmazonS3Reference buildClientReference(S3ClientSettings clientSettings) {
                final var original = super.buildClientReference(clientSettings);
                final var closeLatch = clientRefsCloseLatchRef.get();
                if (closeLatch == null) {
                    return original;
                }

                original.decRef();
                final AmazonS3Reference proxy = new AmazonS3Reference(original.client(), DummySdkHttpClient.INSTANCE) {
                    @Override
                    protected void closeInternal() {
                        closeInternalInvoked.set(true);
                        safeAwait(closeLatch);
                        original.close();
                    }
                };
                proxy.mustIncRef();
                return proxy;
            }
        };
        s3Service.refreshAndClearCache(S3ClientSettings.load(settings));
        s3PerProjectClientManager = s3Service.getS3PerProjectClientManager();
        assertNotNull(s3PerProjectClientManager);
        s3Service.start();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        s3Service.close();
        clusterService.close();
        threadPool.close();
        final var clientsCloseListener = s3PerProjectClientManager.getClientsCloseListener();
        assertTrue(clientsCloseListener == null || clientsCloseListener.isDone());
        s3PerProjectClientManager.getProjectClientsHolders().forEach((projectId, clientsHolder) -> assertTrue(clientsHolder.isClosed()));
    }

    public void testDoesNotCreateClientWhenSecretsAreNotConfigured() {
        assertThat(s3PerProjectClientManager.getProjectClientsHolders(), anEmptyMap());
        final ProjectId projectId = randomUniqueProjectId();

        // No project secrets at all
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state()).putProjectMetadata(ProjectMetadata.builder(projectId)).build()
        );
        assertThat(s3PerProjectClientManager.getProjectClientsHolders(), anEmptyMap());

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
        assertThat(s3PerProjectClientManager.getProjectClientsHolders(), anEmptyMap());
    }

    public void testClientsLifeCycleForSingleProject() {
        final ProjectId projectId = randomUniqueProjectId();
        final String clientName = randomFrom(clientNames);
        final String anotherClientName = randomValueOtherThan(clientName, () -> randomFrom(clientNames));

        // Configure project secrets for one client
        assertClientNotFound(projectId, clientName);
        updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName));
        {
            assertProjectClientSettings(projectId, clientName);

            // Retrieve client for the 1st time
            final AmazonS3Reference initialClient = s3PerProjectClientManager.client(projectId, clientName);
            assertClientCredentials(projectId, clientName, initialClient);
            // Client is cached when retrieved again
            assertThat(initialClient, sameInstance(s3PerProjectClientManager.client(projectId, clientName)));

            // Client not configured cannot be accessed
            assertClientNotFound(projectId, anotherClientName);

            // Client should be released and recreated again on access
            s3PerProjectClientManager.releaseProjectClients(projectId);
            final AmazonS3Reference clientUpdated = s3PerProjectClientManager.client(projectId, clientName);
            assertThat(clientUpdated, not(sameInstance(initialClient)));
            clientUpdated.decRef();

            // Release the initial client and all references should be cleared
            initialClient.decRef();
            initialClient.decRef();
            assertFalse(initialClient.hasReferences());

            // Update client secrets should release and recreate the client
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName, anotherClientName));
            assertProjectClientSettings(projectId, clientName, anotherClientName);
            final AmazonS3Reference clientUpdateAgain = s3PerProjectClientManager.client(projectId, clientName);
            assertThat(clientUpdateAgain, not(sameInstance(clientUpdated)));
            clientUpdateAgain.decRef();

            // A different client for a different client name
            final AmazonS3Reference antherClient = s3PerProjectClientManager.client(projectId, anotherClientName);
            assertClientCredentials(projectId, anotherClientName, antherClient);
            assertThat(antherClient, not(sameInstance(clientUpdateAgain)));
            antherClient.decRef();
        }

        // Remove project secrets
        if (randomBoolean()) {
            updateProjectInClusterState(projectId, Map.of());
        } else {
            removeProjectFromClusterState(projectId);
        }
        assertClientNotFound(projectId, clientName);
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
                    try (var clientRef = s3PerProjectClientManager.client(projectId, clientName)) {
                        assertClientCredentials(projectId, clientName, clientRef);
                    }
                }

                if (randomBoolean()) {
                    final Map<String, AmazonS3Reference> previousClientRefs = clientNames.stream()
                        .map(clientName -> Map.entry(clientName, s3PerProjectClientManager.client(projectId, clientName)))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    s3PerProjectClientManager.releaseProjectClients(projectId);
                    previousClientRefs.forEach((clientName, previousClientRef) -> {
                        final AmazonS3Reference currentClientRef = s3PerProjectClientManager.client(projectId, clientName);
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
                    assertThat(s3PerProjectClientManager.getProjectClientsHolders(), not(hasKey(projectId)));
                    clientNames.forEach(clientName -> assertClientNotFound(projectId, clientName));
                }
            }
        })).toList();

        threads.forEach(Thread::start);
        for (var thread : threads) {
            assertTrue(thread.join(Duration.ofSeconds(10)));
        }
    }

    public void testWaitForAsyncClientClose() throws Exception {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        clientRefsCloseLatchRef.set(closeLatch);

        final List<ProjectId> projectIds = randomList(1, 3, ESTestCase::randomUniqueProjectId);
        final int iterations = between(3, 8);

        final List<AmazonS3Reference> clientRefs = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            for (var projectId : projectIds) {
                final List<String> subsetOfClientNames = randomNonEmptySubsetOf(clientNames);
                updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, subsetOfClientNames.toArray(String[]::new)));
                subsetOfClientNames.forEach(clientName -> {
                    final var newClient = s3PerProjectClientManager.client(projectId, clientName);
                    clientRefs.add(newClient);
                    newClient.decRef();
                });
                if (randomBoolean() && randomBoolean()) {
                    removeProjectFromClusterState(projectId);
                }
            }
        }

        final Thread thread = new Thread(() -> s3Service.close());
        thread.start();

        assertBusy(() -> assertTrue(closeInternalInvoked.get()));
        Thread.sleep(between(0, 100));
        assertFalse(s3PerProjectClientManager.getClientsCloseListener().isDone());

        closeLatch.countDown();
        assertTrue(thread.join(Duration.ofSeconds(10)));
        assertTrue(s3PerProjectClientManager.getClientsCloseListener().isDone());
        clientRefs.forEach(clientRef -> assertFalse(clientRef.hasReferences()));
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

        final AmazonS3Reference clusterClient = s3Service.client(null, repositoryMetadata);
        if (configureProjectClientsFirst == false) {
            assertThat(s3PerProjectClientManager.getProjectClientsHolders(), anEmptyMap());
        }
        clusterClient.decRef();

        if (configureProjectClientsFirst == false) {
            updateProjectInClusterState(projectId, newProjectClientsSecrets(projectId, clientName));
        }
        final AmazonS3Reference projectClient = s3Service.client(projectId, repositoryMetadata);
        assertThat(projectClient, not(sameInstance(clusterClient)));
        projectClient.decRef();

        s3Service.onBlobStoreClose(null);
        assertFalse(clusterClient.hasReferences());
        assertTrue(projectClient.hasReferences());

        s3Service.onBlobStoreClose(projectId);
        assertFalse(projectClient.hasReferences());
    }

    public void testProjectClientsDisabled() {
        final S3Service s3ServiceWithNoProjectSupport = new S3Service(
            mock(Environment.class),
            clusterService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(ResourceWatcherService.class),
            () -> Region.of("es-test-region")
        ) {
        };
        s3ServiceWithNoProjectSupport.refreshAndClearCache(S3ClientSettings.load(clusterService.getSettings()));
        s3ServiceWithNoProjectSupport.start();
        assertNull(s3ServiceWithNoProjectSupport.getS3PerProjectClientManager());

        final String clientName = randomFrom(clientNames);
        final var repositoryMetadata = new RepositoryMetadata(
            randomIdentifier(),
            "s3",
            Settings.builder().put("client", clientName).build()
        );
        final AmazonS3Reference clientRef = s3ServiceWithNoProjectSupport.client(ProjectId.DEFAULT, repositoryMetadata);
        clientRef.decRef();
        s3ServiceWithNoProjectSupport.close();
        assertFalse(clientRef.hasReferences());
    }

    private void assertProjectClientSettings(ProjectId projectId, String... clientNames) {
        final var clientsHolder = s3PerProjectClientManager.getProjectClientsHolders().get(projectId);
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
            () -> s3PerProjectClientManager.client(projectId, clientName)
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

    private static class DummySdkHttpClient implements SdkHttpClient {

        static final SdkHttpClient INSTANCE = new DummySdkHttpClient();

        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            return null;
        }

        @Override
        public void close() {}
    }
}
