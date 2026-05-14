/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import com.google.cloud.storage.Storage;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.gcs.GcsRepositoryStatsCollector;
import org.elasticsearch.repositories.gcs.GoogleCloudStoragePlugin;
import org.elasticsearch.repositories.gcs.GoogleCloudStorageService;
import org.elasticsearch.repositories.gcs.MeteredStorage;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public class GoogleObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestGoogleCloudStoragePlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Map.of("/", new GoogleCloudStorageHttpHandler("bucket"), "/token", new FakeOAuth2HttpHandler());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        final Settings.Builder settings = super.nodeSettings();
        settings.put("gcs.client.test.endpoint", httpServerUrl());
        settings.put("gcs.client.test.token_uri", httpServerUrl() + "/token");
        settings.put("gcs.client.backup.endpoint", httpServerUrl());
        settings.put("gcs.client.backup.token_uri", httpServerUrl() + "/token");
        settings.put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.GCS);
        settings.put(ObjectStoreService.BUCKET_SETTING.getKey(), "bucket");
        settings.put(ObjectStoreService.CLIENT_SETTING.getKey(), "test");

        final byte[] serviceAccount = TestUtils.createServiceAccount(random(), UUID.randomUUID().toString(), "admin@cluster.com");
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile("gcs.client.test.credentials_file", serviceAccount);
        settings.setSecureSettings(mockSecureSettings);

        return settings;
    }

    @Override
    protected String repositoryType() {
        return "gcs";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put("bucket", "bucket")
            .put("base_path", "backup")
            .put("client", "test")
            .build();
    }

    void assertMetricStats(Set<String> expectedMetrics, Map<String, BlobStoreActionStats> actualMetrics) {
        for (var expectedMetric : expectedMetrics) {
            var actualStats = actualMetrics.get(expectedMetric);
            assertNotNull(expectedMetric + " not found in " + actualMetrics, actualStats);
            assertTrue(expectedMetric + " value should be not zero", actualStats.operations() > 0);
        }
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats actualStats, boolean withRandomCrud, OperationPurpose randomPurpose) {
        var expectedMetrics = new HashSet<>(
            List.of(
                "ClusterState_GetObject",
                "ClusterState_InsertObject",
                "ClusterState_ListObjects",
                "Indices_ListObjects",
                "Translog_ListObjects"
            )
        );
        if (withRandomCrud) {
            for (var op : List.of("GetObject", "ListObjects", "InsertObject")) {
                expectedMetrics.add(randomPurpose.getKey() + "_" + op);
            }
        }
        assertMetricStats(expectedMetrics, actualStats.actionStats);
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats actualStats) {
        var expectedMetrics = new HashSet<>(
            List.of(
                "SnapshotMetadata_GetObject",
                "SnapshotMetadata_ListObjects",
                "SnapshotMetadata_InsertObject",
                "SnapshotData_ListObjects"
            )
        );
        assertMetricStats(expectedMetrics, actualStats.actionStats);
    }

    public void testRecoverySucceedAgainstTransientCSPError() throws Exception {
        final var settings = Settings.builder()
            .put("gcs.client.test.max_retries", 0) // disable sdk client retries
            .put("gcs.client.test.tenacious_retries.enabled", true)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startIndexNode(settings);
        ensureStableCluster(2);

        // Create the index with shard on node1 then start the 3rd node
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node0).build());
        ensureGreen(indexName);
        indexDocs(indexName, between(10, 100));
        flush(indexName);

        final var node2 = startIndexNode(settings);
        ensureStableCluster(3);

        // Inject errors on the 3rd node. Stop the node for ungraceful relocation since graceful relocation gets the list of blobs
        // from the source node and avoids listing operations which are what we want to test against.
        final var numberOfErrors = new AtomicInteger(5);
        findPlugin(node2, TestGoogleCloudStoragePlugin.class).shouldInjectErrorRef.set(() -> numberOfErrors.decrementAndGet() >= 0);

        logger.info("--> stopping " + node1);
        internalCluster().stopNode(node1);
        ensureGreen(indexName);
    }

    public static class TestGoogleCloudStoragePlugin extends GoogleCloudStoragePlugin {
        final AtomicReference<BooleanSupplier> shouldInjectErrorRef = new AtomicReference<>();

        public TestGoogleCloudStoragePlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected GoogleCloudStorageService createStorageService(ClusterService clusterService, ProjectResolver projectResolver) {
            return new TestGoogleCloudStorageService(clusterService, projectResolver, shouldInjectErrorRef);
        }
    }

    public static class TestGoogleCloudStorageService extends GoogleCloudStorageService {
        private final AtomicReference<BooleanSupplier> shouldInjectErrorRef;

        public TestGoogleCloudStorageService(
            ClusterService clusterService,
            ProjectResolver projectResolver,
            AtomicReference<BooleanSupplier> shouldInjectErrorRef
        ) {
            super(clusterService, projectResolver);
            this.shouldInjectErrorRef = shouldInjectErrorRef;
        }

        @Override
        public MeteredStorage client(
            ProjectId projectId,
            String clientName,
            String repositoryName,
            GcsRepositoryStatsCollector statsCollector
        ) throws IOException {
            final MeteredStorage original = super.client(projectId, clientName, repositoryName, statsCollector);

            final var shouldInjectError = shouldInjectErrorRef.get();
            if (shouldInjectError != null && shouldInjectError.getAsBoolean()) {
                final MeteredStorage spied = Mockito.spy(original);
                doAnswer(invocation -> {
                    final OperationPurpose purpose = invocation.getArgument(0);
                    if (purpose == OperationPurpose.INDICES) {
                        throw new UnknownHostException("simulated");
                    }
                    return invocation.callRealMethod();
                }).when(spied).meteredList(any(OperationPurpose.class), any(String.class), any(Storage.BlobListOption[].class));
                return spied;
            } else {
                return original;
            }
        }
    }
}
