/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;

public class SearchableSnapshotsRollingUpgradeIT extends AbstractUpgradeTestCase {

    public enum Storage {

        FULL_COPY("full_copy"),
        SHARED_CACHE("shared_cache");

        private final String storageName;

        public String storageName() {
            return storageName;
        }

        Storage(final String storageName) {
            this.storageName = storageName;
        }
    }

    public void testMountFullCopyAndRecoversCorrectly() throws Exception {
        final Storage storage = Storage.FULL_COPY;
        assumeVersion(Version.V_7_10_0, storage);

        executeMountAndRecoversCorrectlyTestCase(storage, 6789L);
    }

    public void testMountPartialCopyAndRecoversCorrectly() throws Exception {
        final Storage storage = Storage.SHARED_CACHE;
        assumeVersion(Version.V_7_12_0, Storage.SHARED_CACHE);

        if (CLUSTER_TYPE.equals(ClusterType.UPGRADED)) {
            assertBusy(() -> {
                Map<String, Object> settings = getIndexSettingsAsMap("mounted_index_shared_cache");
                assertThat(
                    settings,
                    hasEntry(ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.getKey(), ShardLimitValidator.FROZEN_GROUP)
                );
            });
        }

        executeMountAndRecoversCorrectlyTestCase(storage, 5678L);
    }

    /**
     * Test that a snapshot mounted as a searchable snapshot index in the previous version recovers correctly during rolling upgrade
     */
    private void executeMountAndRecoversCorrectlyTestCase(Storage storage, long numberOfDocs) throws Exception {
        final String suffix = storage.storageName().toLowerCase(Locale.ROOT);
        final String repository = "repository_" + suffix;
        final String snapshot = "snapshot_" + suffix;
        final String index = "mounted_index_" + suffix;

        if (CLUSTER_TYPE.equals(ClusterType.OLD)) {
            registerRepository(repository, FsRepository.TYPE, true, repositorySettings(repository));

            final String originalIndex = "logs_" + suffix;
            createIndex(
                originalIndex,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            );
            indexDocs(originalIndex, numberOfDocs);
            createSnapshot(repository, snapshot, originalIndex);
            deleteIndex(originalIndex);

            logger.info(
                "mounting snapshot [repository={}, snapshot={}, index={}] as index [{}] with storage [{}] on version [{}]",
                repository,
                snapshot,
                originalIndex,
                index,
                storage,
                UPGRADE_FROM_VERSION
            );
            mountSnapshot(repository, snapshot, originalIndex, index, storage, Settings.EMPTY);
        }

        ensureGreen(index);
        assertHitCount(index, equalTo(numberOfDocs));

        if (CLUSTER_TYPE.equals(ClusterType.UPGRADED)) {
            deleteIndex(index);
            deleteSnapshot(repository, snapshot);
            deleteRepository(repository);
        }
    }

    public void testBlobStoreCacheWithFullCopyInMixedVersions() throws Exception {
        final Storage storage = Storage.FULL_COPY;
        assumeVersion(Version.V_7_10_0, storage);

        executeBlobCacheCreationTestCase(storage, 9876L);
    }

    public void testBlobStoreCacheWithPartialCopyInMixedVersions() throws Exception {
        final Storage storage = Storage.SHARED_CACHE;
        assumeVersion(Version.V_7_12_0, Storage.SHARED_CACHE);

        executeBlobCacheCreationTestCase(storage, 8765L);
    }

    /**
     * Test the behavior of the blob store cache in mixed versions cluster. The idea is to mount a new snapshot as an index on a node with
     * version X so that this node generates cached blobs documents in the blob cache system index, and then mount the snapshot again on
     * a different node with version Y so that this other node is likely to use the previously generated cached blobs documents.
     */
    private void executeBlobCacheCreationTestCase(Storage storage, long numberOfDocs) throws Exception {
        final String suffix = "blob_cache_" + storage.storageName().toLowerCase(Locale.ROOT);
        final String repository = "repository_" + suffix;

        final int numberOfSnapshots = 2;
        final String[] snapshots = new String[numberOfSnapshots];
        final String[] indices = new String[numberOfSnapshots];
        for (int i = 0; i < numberOfSnapshots; i++) {
            snapshots[i] = "snapshot_" + i;
            indices[i] = "index_" + i;
        }

        if (CLUSTER_TYPE.equals(ClusterType.OLD)) {
            registerRepository(repository, FsRepository.TYPE, true, repositorySettings(repository));

            // snapshots must be created from indices on the lowest version, otherwise we won't be able
            // to mount them again in the mixed version cluster (and we'll have IndexFormatTooNewException)
            for (int i = 0; i < numberOfSnapshots; i++) {
                createIndex(
                    indices[i],
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                );
                indexDocs(indices[i], numberOfDocs * (i + 1L));

                createSnapshot(repository, snapshots[i], indices[i]);
                deleteIndex(indices[i]);
            }

        } else if (CLUSTER_TYPE.equals(ClusterType.MIXED)) {
            final int numberOfNodes = 3;
            waitForNodes(numberOfNodes);

            final Map<String, Version> nodesIdsAndVersions = nodesVersions();
            assertThat("Cluster should have 3 nodes", nodesIdsAndVersions.size(), equalTo(numberOfNodes));

            final Version minVersion = nodesIdsAndVersions.values().stream().min(Version::compareTo).get();
            final Version maxVersion = nodesIdsAndVersions.values().stream().max(Version::compareTo).get();

            final String nodeIdWithMinVersion = randomFrom(
                nodesIdsAndVersions.entrySet()
                    .stream()
                    .filter(node -> minVersion.equals(node.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet())
            );

            final String nodeIdWithMaxVersion = randomValueOtherThan(
                nodeIdWithMinVersion,
                () -> randomFrom(
                    nodesIdsAndVersions.entrySet()
                        .stream()
                        .filter(node -> maxVersion.equals(node.getValue()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet())
                )
            );

            // The snapshot is mounted on the node with the min. version in order to force the node to populate the blob store cache index.
            // Then the snapshot is mounted again on a different node with a higher version in order to verify that the docs in the cache
            // index can be used.

            String index = "first_mount_" + indices[0];
            logger.info(
                "mounting snapshot as index [{}] with storage [{}] on node [{}] with min. version [{}]",
                index,
                storage,
                nodeIdWithMinVersion,
                minVersion
            );
            mountSnapshot(
                repository,
                snapshots[0],
                indices[0],
                index,
                storage,
                Settings.builder()
                    // we want a specific node version to create docs in the blob cache index
                    .put("index.routing.allocation.include._id", nodeIdWithMinVersion)
                    // prevent interferences with blob cache when full_copy is used
                    .put("index.store.snapshot.cache.prewarm.enabled", false)
                    .build()
            );
            ensureGreen(index);
            assertHitCount(index, equalTo(numberOfDocs));
            deleteIndex(index);

            index = "second_mount_" + indices[0];
            logger.info(
                "mounting the same snapshot of index [{}] with storage [{}], this time on node [{}] with higher version [{}]",
                index,
                storage,
                nodeIdWithMaxVersion,
                maxVersion
            );
            mountSnapshot(
                repository,
                snapshots[0],
                indices[0],
                index,
                storage,
                Settings.builder()
                    // we want a specific node version to use the cached blobs created by the nodeIdWithMinVersion
                    .put("index.routing.allocation.include._id", nodeIdWithMaxVersion)
                    .put("index.routing.allocation.exclude._id", nodeIdWithMinVersion)
                    // prevent interferences with blob cache when full_copy is used
                    .put("index.store.snapshot.cache.prewarm.enabled", false)
                    .build()
            );
            ensureGreen(index);
            assertHitCount(index, equalTo(numberOfDocs));
            deleteIndex(index);

            // Now the same thing but this time the docs in blob cache index are created from the upgraded version and mounted in a second
            // time on the node with the minimum version.

            index = "first_mount_" + indices[1];
            logger.info(
                "mounting snapshot as index [{}] with storage [{}] on node [{}] with max. version [{}]",
                index,
                storage,
                nodeIdWithMaxVersion,
                maxVersion
            );
            mountSnapshot(
                repository,
                snapshots[1],
                indices[1],
                index,
                storage,
                Settings.builder()
                    // we want a specific node version to create docs in the blob cache index
                    .put("index.routing.allocation.include._id", nodeIdWithMaxVersion)
                    // prevent interferences with blob cache when full_copy is used
                    .put("index.store.snapshot.cache.prewarm.enabled", false)
                    .build()
            );
            ensureGreen(index);
            assertHitCount(index, equalTo(numberOfDocs * 2L));
            deleteIndex(index);

            index = "second_mount_" + indices[1];
            logger.info(
                "mounting the same snapshot of index [{}] with storage [{}], this time on node [{}] with lower version [{}]",
                index,
                storage,
                nodeIdWithMinVersion,
                minVersion
            );
            mountSnapshot(
                repository,
                snapshots[1],
                indices[1],
                index,
                storage,
                Settings.builder()
                    // we want a specific node version to use the cached blobs created by the nodeIdWithMinVersion
                    .put("index.routing.allocation.include._id", nodeIdWithMinVersion)
                    .put("index.routing.allocation.exclude._id", nodeIdWithMaxVersion)
                    // prevent interferences with blob cache when full_copy is used
                    .put("index.store.snapshot.cache.prewarm.enabled", false)
                    .build()
            );
            ensureGreen(index);
            assertHitCount(index, equalTo(numberOfDocs * 2L));
            deleteIndex(index);

            if (UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_13_0)) {
                final Request request = new Request(
                    "GET",
                    "/.snapshot-blob-cache/_settings/index.routing.allocation.include._tier_preference"
                );
                request.setOptions(
                    expectWarnings(
                        "this request accesses system indices: [.snapshot-blob-cache], but in a future major "
                            + "version, direct access to system indices will be prevented by default"
                    )
                );
                request.addParameter("flat_settings", "true");

                final Map<String, ?> snapshotBlobCacheSettings = entityAsMap(adminClient().performRequest(request));
                assertThat(snapshotBlobCacheSettings, notNullValue());
                final String tierPreference = (String) extractValue(
                    ".snapshot-blob-cache.settings.index.routing.allocation.include._tier_preference",
                    snapshotBlobCacheSettings
                );
                assertThat(tierPreference, equalTo("data_content,data_hot"));
            }

        } else if (CLUSTER_TYPE.equals(ClusterType.UPGRADED)) {
            for (String snapshot : snapshots) {
                deleteSnapshot(repository, snapshot);
            }
            deleteRepository(repository);
        }
    }

    private static void assumeVersion(Version minSupportedVersion, Storage storageType) {
        assumeTrue(
            "Searchable snapshots with storage type [" + storageType + "] is supported since version [" + minSupportedVersion + ']',
            UPGRADE_FROM_VERSION.onOrAfter(minSupportedVersion)
        );
    }

    private static void indexDocs(String indexName, long numberOfDocs) throws IOException {
        final StringBuilder builder = new StringBuilder();
        for (long i = 0L; i < numberOfDocs; i++) {
            builder.append("{\"create\":{\"_index\":\"").append(indexName).append("\"}}\n");
            builder.append("{\"value\":").append(i).append("}\n");
        }
        final Request bulk = new Request(HttpPost.METHOD_NAME, "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(builder.toString());
        final Response response = client().performRequest(bulk);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        assertFalse((Boolean) XContentMapValues.extractValue("errors", responseAsMap(response)));
    }

    private static void createSnapshot(String repositoryName, String snapshotName, String indexName) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "/_snapshot/" + repositoryName + '/' + snapshotName);
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity("{ \"indices\" : \"" + indexName + "\", \"include_global_state\": false}");
        final Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private static void waitForNodes(int numberOfNodes) throws IOException {
        final Request request = new Request(HttpGet.METHOD_NAME, "/_cluster/health");
        request.addParameter("wait_for_nodes", String.valueOf(numberOfNodes));
        final Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Version> nodesVersions() throws IOException {
        final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, "_nodes/_all"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        final Map<String, Object> nodes = (Map<String, Object>) extractValue(responseAsMap(response), "nodes");
        assertNotNull("Nodes info is null", nodes);
        final Map<String, Version> nodesVersions = Maps.newMapWithExpectedSize(nodes.size());
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            nodesVersions.put(node.getKey(), Version.fromString((String) extractValue((Map<?, ?>) node.getValue(), "version")));
        }
        return nodesVersions;
    }

    private static void deleteSnapshot(String repositoryName, String snapshotName) throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "/_snapshot/" + repositoryName + '/' + snapshotName);
        final Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private static void mountSnapshot(
        String repositoryName,
        String snapshotName,
        String indexName,
        String renamedIndex,
        Storage storage,
        Settings indexSettings
    ) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, "/_snapshot/" + repositoryName + '/' + snapshotName + "/_mount");
        if (UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_12_0)) {
            request.addParameter("storage", storage.storageName());
        } else {
            assertThat("Parameter 'storage' was introduced in 7.12.0 with " + Storage.SHARED_CACHE, storage, equalTo(Storage.FULL_COPY));
        }
        request.setJsonEntity("""
            {
              "index": "%s",
              "renamed_index": "%s",
              "index_settings": %s
            }""".formatted(indexName, renamedIndex, Strings.toString(indexSettings)));
        final Response response = client().performRequest(request);
        assertThat(
            "Failed to mount snapshot [" + snapshotName + "] from repository [" + repositoryName + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    private static void assertHitCount(String indexName, Matcher<Long> countMatcher) throws IOException {
        final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, "/" + indexName + "/_count"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        final Map<String, Object> responseAsMap = responseAsMap(response);
        final Number responseCount = (Number) extractValue("count", responseAsMap);
        assertThat(responseAsMap + "", responseCount, notNullValue());
        assertThat(((Number) extractValue("count", responseAsMap)).longValue(), countMatcher);
        assertThat(((Number) extractValue("_shards.failed", responseAsMap)).intValue(), equalTo(0));
    }

    private static Settings repositorySettings(String repository) {
        final String pathRepo = System.getProperty("tests.path.searchable.snapshots.repo");
        assertThat("Searchable snapshots repository path is null", pathRepo, notNullValue());
        return Settings.builder().put("location", pathRepo + '/' + repository).build();
    }
}
