/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * This class tests whether global and index metadata are only loaded from the repository when needed.
*/
public class MetadataLoadingDuringSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        /// This test uses a snapshot/restore plugin implementation that
        // counts the number of times metadata are loaded
        return Collections.singletonList(CountingMockRepositoryPlugin.class);
    }

    public void testWhenMetadataAreLoaded() throws Exception {
        createIndex("docs");
        indexRandom(
            true,
            prepareIndex("docs").setId("1").setSource("rank", 1),
            prepareIndex("docs").setId("2").setSource("rank", 2),
            prepareIndex("docs").setId("3").setSource("rank", 3),
            prepareIndex("others").setSource("rank", 4),
            prepareIndex("others").setSource("rank", 5)
        );

        createRepository("repository", CountingMockRepositoryPlugin.TYPE);

        // Creating a snapshot does not load any metadata
        SnapshotInfo snapshotInfo = createFullSnapshot("repository", "snap");
        assertThat(snapshotInfo.failedShards(), equalTo(0));
        assertThat(snapshotInfo.status(), equalTo(RestStatus.OK));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 0);
        assertIndexMetadataLoads("snap", "others", 0);

        // Getting a snapshot does not load any metadata
        GetSnapshotsResponse getSnapshotsResponse = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "repository")
            .addSnapshots("snap")
            .setVerbose(randomBoolean())
            .get();
        assertThat(getSnapshotsResponse.getSnapshots(), hasSize(1));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 0);
        assertIndexMetadataLoads("snap", "others", 0);

        // Getting the status of a snapshot loads indices metadata but not global metadata
        SnapshotsStatusResponse snapshotStatusResponse = clusterAdmin().prepareSnapshotStatus(TEST_REQUEST_TIMEOUT, "repository")
            .setSnapshots("snap")
            .get();
        assertThat(snapshotStatusResponse.getSnapshots(), hasSize(1));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 1);
        assertIndexMetadataLoads("snap", "others", 1);

        assertAcked(indicesAdmin().prepareDelete("docs", "others"));

        // Restoring a snapshot loads indices metadata but not the global state
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repository", "snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 2);
        assertIndexMetadataLoads("snap", "others", 2);

        assertAcked(indicesAdmin().prepareDelete("docs"));

        // Restoring a snapshot with selective indices loads only required index metadata
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repository", "snap")
            .setIndices("docs")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 3);
        assertIndexMetadataLoads("snap", "others", 2);

        assertAcked(indicesAdmin().prepareDelete("docs", "others"));

        // Restoring a snapshot including the global state loads it with the index metadata
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repository", "snap")
            .setIndices("docs", "oth*")
            .setRestoreGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertGlobalMetadataLoads("snap", 1);
        assertIndexMetadataLoads("snap", "docs", 4);
        assertIndexMetadataLoads("snap", "others", 3);

        // Deleting a snapshot does not load the global metadata state but loads each index metadata
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "repository", "snap").get());
        assertGlobalMetadataLoads("snap", 1);
        assertIndexMetadataLoads("snap", "docs", 4);
        assertIndexMetadataLoads("snap", "others", 3);
    }

    private void assertGlobalMetadataLoads(final String snapshot, final int times) {
        AtomicInteger count = getCountingMockRepository().globalMetadata.get(snapshot);
        if (times == 0) {
            assertThat("Global metadata for " + snapshot + " must not have been loaded", count, nullValue());
        } else {
            assertThat("Global metadata for " + snapshot + " must have been loaded " + times + " times", count.get(), equalTo(times));
        }
    }

    private void assertIndexMetadataLoads(final String snapshot, final String index, final int times) {
        final String key = key(snapshot, index);
        AtomicInteger count = getCountingMockRepository().indicesMetadata.get(key);
        if (times == 0) {
            assertThat("Index metadata for " + key + " must not have been loaded", count, nullValue());
        } else {
            assertThat("Index metadata for " + key + " must have been loaded " + times + " times", count.get(), equalTo(times));
        }
    }

    private CountingMockRepository getCountingMockRepository() {
        String master = internalCluster().getMasterName();
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, master);
        Repository repository = repositoriesService.repository("repository");
        assertThat(repository, instanceOf(CountingMockRepository.class));
        return (CountingMockRepository) repository;
    }

    /** Compute a map key for the given snapshot and index names **/
    private static String key(final String snapshot, final String index) {
        return snapshot + ":" + index;
    }

    /** A mocked repository that counts the number of times global/index metadata are accessed **/
    public static class CountingMockRepository extends MockRepository {

        final Map<String, AtomicInteger> globalMetadata = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> indicesMetadata = new ConcurrentHashMap<>();

        public CountingMockRepository(
            final RepositoryMetadata metadata,
            final Environment environment,
            final NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        @Override
        public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
            globalMetadata.computeIfAbsent(snapshotId.getName(), (s) -> new AtomicInteger(0)).incrementAndGet();
            return super.getSnapshotGlobalMetadata(snapshotId);
        }

        @Override
        public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId indexId)
            throws IOException {
            indicesMetadata.computeIfAbsent(key(snapshotId.getName(), indexId.getName()), (s) -> new AtomicInteger(0)).incrementAndGet();
            return super.getSnapshotIndexMetaData(AbstractSnapshotIntegTestCase.getRepositoryData(this), snapshotId, indexId);
        }
    }

    /** A plugin that uses CountingMockRepository as implementation of the Repository **/
    public static class CountingMockRepositoryPlugin extends MockRepository.Plugin {

        public static final String TYPE = "countingmock";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Collections.singletonMap(
                TYPE,
                (projectId, metadata) -> new CountingMockRepository(
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings
                )
            );
        }
    }
}
