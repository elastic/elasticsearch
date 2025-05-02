/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class S3PerProjectClientManagerTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private S3Service s3Service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("s3.client.default.access_key", "cluster_access_key");
        mockSecureSettings.setString("s3.client.default.secret_key", "cluster_secret_key");
        final Settings settings = Settings.builder()
            .put("s3.client.default.max_retries", 9)
            .put("s3.client.backup.read_timeout", "99s")
            .setSecureSettings(mockSecureSettings)
            .build();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
        s3Service = new S3Service(
            mock(Environment.class),
            clusterService,
            TestProjectResolvers.allProjects(),
            mock(ResourceWatcherService.class),
            () -> Region.of("es-test-region")
        );
        s3Service.start();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        s3Service.close();
        clusterService.close();
        threadPool.close();
        final var s3PerProjectClientManager = s3Service.getS3PerProjectClientManager();
        final var clientsCloseListener = s3PerProjectClientManager.getClientsCloseListener();
        assertTrue(clientsCloseListener == null || clientsCloseListener.isDone());
        s3PerProjectClientManager.getProjectClientsHolders().forEach((projectId, clientsHolder) -> assertTrue(clientsHolder.isClosed()));
    }

    public void testBasic() {
        final var s3PerProjectClientManager = s3Service.getS3PerProjectClientManager();
        assertNotNull(s3PerProjectClientManager);
        assertThat(s3PerProjectClientManager.getProjectClientsHolders(), anEmptyMap());

        final ProjectId projectId = randomUniqueProjectId();
        final var repositoryMetadata = createRepositoryMetadata("default");
        {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> s3PerProjectClientManager.client(projectId, repositoryMetadata)
            );
            assertThat(e.getMessage(), containsString("project [" + projectId + "] does not exist"));
        }

        final var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile("s3.client.default.access_key", (projectId + "_access_key").getBytes(StandardCharsets.UTF_8));
        mockSecureSettings.setFile("s3.client.default.secret_key", (projectId + "_secret_key").getBytes(StandardCharsets.UTF_8));
        final ClusterState stateWithProject = ClusterState.builder(clusterService.state())
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .putCustom(ProjectSecrets.TYPE, new ProjectSecrets(new SecureClusterStateSettings(mockSecureSettings)))
            )
            .build();
        ClusterServiceUtils.setState(clusterService, stateWithProject);
        {
            final var clientsHolder = s3PerProjectClientManager.getProjectClientsHolders().get(projectId);
            assertNotNull(clientsHolder);
            final Map<String, S3ClientSettings> s3ClientSettingsMap = clientsHolder.clientSettings();
            assertThat(s3ClientSettingsMap.keySet(), equalTo(Set.of("default")));
            final S3ClientSettings clientSettings = s3ClientSettingsMap.get("default");
            // Picks up the correct project scoped credentials
            assertThat(
                clientSettings.credentials,
                equalTo(AwsBasicCredentials.create(projectId + "_access_key", projectId + "_secret_key"))
            );
            // Inherit setting override from the cluster client of the same name
            assertThat(clientSettings.maxRetries, equalTo(9));
            // Does not inherit setting override from a cluster client of a different name
            assertThat((long) clientSettings.readTimeoutMillis, equalTo(S3ClientSettings.Defaults.READ_TIMEOUT.millis()));

            // Retrieve client for the 1st time
            final AmazonS3Reference initialClient = s3PerProjectClientManager.client(projectId, repositoryMetadata);
            // Client is cached when retrieved again
            assertThat(initialClient, sameInstance(s3PerProjectClientManager.client(projectId, repositoryMetadata)));

            // Client should be released and recreated again on access
            s3PerProjectClientManager.releaseProjectClients(projectId);
            final AmazonS3Reference clientAgain = s3PerProjectClientManager.client(projectId, repositoryMetadata);
            assertThat(clientAgain, not(sameInstance(initialClient)));
            clientAgain.decRef();

            // Release the initial client and all references should be cleared
            initialClient.decRef();
            initialClient.decRef();
            assertFalse(initialClient.hasReferences());

            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> s3PerProjectClientManager.client(projectId, createRepositoryMetadata("backup"))
            );
            assertThat(e.getMessage(), containsString("client [backup] does not exist"));
        }

        {
            final ProjectId anotherProjectId = randomUniqueProjectId();
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> s3PerProjectClientManager.client(anotherProjectId, repositoryMetadata)
            );
            assertThat(e.getMessage(), containsString("project [" + anotherProjectId + "] does not exist"));
        }
    }

    private static RepositoryMetadata createRepositoryMetadata(String clientName) {
        return new RepositoryMetadata(randomIdentifier(), "s3", Settings.builder().put("client", clientName).build());
    }

}
