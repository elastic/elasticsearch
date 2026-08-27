/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;

public class SearchableSnapshotsRollingUpgradeIT extends AbstractXpackRollingUpgradeTestCase {

    public SearchableSnapshotsRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public enum Storage {

        FULL_COPY("full_copy"),
        SHARED_CACHE("shared_cache");

        private final String storageName;

        Storage(String storageName) {
            this.storageName = storageName;
        }

        public String storageName() {
            return storageName;
        }
    }

    public void testMountFullCopyAndRecoversCorrectly() throws Exception {
        executeMountAndRecoversCorrectlyTestCase(Storage.FULL_COPY, 6789L);
    }

    public void testMountPartialCopyAndRecoversCorrectly() throws Exception {
        executeMountAndRecoversCorrectlyTestCase(Storage.SHARED_CACHE, 5678L);
    }

    /**
     * Test that a snapshot mounted as a searchable snapshot index in the previous version recovers correctly during rolling upgrade
     */
    private void executeMountAndRecoversCorrectlyTestCase(Storage storage, long numberOfDocs) throws Exception {
        final String suffix = storage.storageName().toLowerCase(Locale.ROOT);
        final String repository = "repository_" + suffix;
        final String snapshot = "snapshot_" + suffix;
        final String originalIndex = "logs_" + suffix;
        final String index = "mounted_index_" + suffix;

        if (isOldCluster()) {
            registerRepository(repository, FsRepository.TYPE, true, repositorySettings(repository));

            createIndex(originalIndex, indexSettings(randomIntBetween(1, 3), 0).build());
            indexDocs(originalIndex, numberOfDocs);
            createSnapshotOfIndex(repository, snapshot, originalIndex);
            deleteIndex(originalIndex);

            logger.info(
                "mounting snapshot [repository={}, snapshot={}, index={}] as index [{}] with storage [{}] on version [{}]",
                repository,
                snapshot,
                originalIndex,
                index,
                storage,
                getOldClusterVersion()
            );
            mountSnapshot(repository, snapshot, originalIndex, index, storage, Settings.EMPTY);
        }

        if (isUpgradedCluster() && storage == Storage.SHARED_CACHE) {
            // After a full upgrade, partial-copy (shared_cache) mounts are moved to the frozen tier,
            // which is reflected via index.shard_limit.group=frozen.
            assertBusy(() -> {
                Map<String, Object> settings = getIndexSettingsAsMap(index);
                assertThat(settings, hasEntry("index.shard_limit.group", "frozen"));
            });
        }

        ensureGreen(index);
        assertHitCount(index, equalTo(numberOfDocs));

        if (isUpgradedCluster()) {
            deleteIndex(index);
            deleteSnapshot(repository, snapshot, false);
            deleteRepository(repository);
        }
    }

    public void testBlobStoreCacheWithFullCopyInMixedVersions() throws Exception {
        executeBlobCacheCreationTestCase(Storage.FULL_COPY, 9876L);
    }

    public void testBlobStoreCacheWithPartialCopyInMixedVersions() throws Exception {
        executeBlobCacheCreationTestCase(Storage.SHARED_CACHE, 8765L);
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

        if (isOldCluster()) {
            registerRepository(repository, FsRepository.TYPE, true, repositorySettings(repository));

            // snapshots must be created from indices on the lowest version, otherwise we won't be able
            // to mount them again in the mixed version cluster (and we'll have IndexFormatTooNewException)
            for (int i = 0; i < numberOfSnapshots; i++) {
                createIndex(indices[i], indexSettings(randomIntBetween(1, 3), 0).build());
                indexDocs(indices[i], numberOfDocs * (i + 1L));
                createSnapshotOfIndex(repository, snapshots[i], indices[i]);
                deleteIndex(indices[i]);
            }
        }

        if (isMixedCluster()) {
            final List<NodeInfo> nodeInfos = getNodeInfos();

            final List<String> oldVersionNodeIds = nodeInfos.stream()
                .filter(n -> isOldClusterVersion(n.version()))
                .map(NodeInfo::nodeId)
                .toList();

            final List<String> upgradedVersionNodeIds = nodeInfos.stream()
                .filter(n -> isOldClusterVersion(n.version()) == false)
                .map(NodeInfo::nodeId)
                .toList();

            final String oldVersionNodeId = randomFrom(oldVersionNodeIds);

            // We may not have upgraded nodes, if we are running these test on the same version (original == current)
            final List<String> effectiveUpgradedNodeIds = upgradedVersionNodeIds.isEmpty() ? oldVersionNodeIds : upgradedVersionNodeIds;
            final String upgradedVersionNodeId = randomValueOtherThan(oldVersionNodeId, () -> randomFrom(effectiveUpgradedNodeIds));

            // The snapshot is mounted on the node with the min. version in order to force the node to populate the blob store cache index.
            // Then the snapshot is mounted again on a different node with a higher version in order to verify that the docs in the cache
            // index can be used.

            String index = "first_mount_" + indices[0];
            logger.info("mounting snapshot as [{}] with storage [{}] on old-version node [{}]", index, storage, oldVersionNodeId);
            mountSnapshot(
                repository,
                snapshots[0],
                indices[0],
                index,
                storage,
                Settings.builder()
                    // we want a specific node version to create docs in the blob cache index
                    .put("index.routing.allocation.include._id", oldVersionNodeId)
                    // prevent interferences with blob cache when full_copy is used
                    .put("index.store.snapshot.cache.prewarm.enabled", false)
                    .build()
            );
            ensureGreen(index);
            assertHitCount(index, equalTo(numberOfDocs));
            deleteIndex(index);

            index = "second_mount_" + indices[0];
            logger.info("mounting same snapshot as [{}] with storage [{}] on new-version node [{}]", index, storage, upgradedVersionNodeId);
            mountSnapshot(
                repository,
                snapshots[0],
                indices[0],
                index,
                storage,
                Settings.builder()
                    // we want a specific node version to use the cached blobs created by the nodeIdWithMinVersion
                    .put("index.routing.allocation.include._id", upgradedVersionNodeId)
                    .put("index.routing.allocation.exclude._id", oldVersionNodeId)
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
            logger.info("mounting snapshot as [{}] with storage [{}] on new-version node [{}]", index, storage, upgradedVersionNodeId);
            mountSnapshot(
                repository,
                snapshots[1],
                indices[1],
                index,
                storage,
                Settings.builder()
                    // we want a specific node version to create docs in the blob cache index
                    .put("index.routing.allocation.include._id", upgradedVersionNodeId)
                    // prevent interferences with blob cache when full_copy is used
                    .put("index.store.snapshot.cache.prewarm.enabled", false)
                    .build()
            );
            ensureGreen(index);
            assertHitCount(index, equalTo(numberOfDocs * 2L));
            deleteIndex(index);

            index = "second_mount_" + indices[1];
            logger.info("mounting same snapshot as [{}] with storage [{}] on old-version node [{}]", index, storage, oldVersionNodeId);
            mountSnapshot(
                repository,
                snapshots[1],
                indices[1],
                index,
                storage,
                Settings.builder()
                    // we want a specific node version to use the cached blobs created by the nodeIdWithMinVersion
                    .put("index.routing.allocation.include._id", oldVersionNodeId)
                    .put("index.routing.allocation.exclude._id", upgradedVersionNodeId)
                    // prevent interferences with blob cache when full_copy is used
                    .put("index.store.snapshot.cache.prewarm.enabled", false)
                    .build()
            );
            ensureGreen(index);
            assertHitCount(index, equalTo(numberOfDocs * 2L));
            deleteIndex(index);

            final Request request = new Request("GET", "/.snapshot-blob-cache/_settings/index.routing.allocation.include._tier_preference");
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

        } else if (isUpgradedCluster()) {
            for (String snapshot : snapshots) {
                deleteSnapshot(repository, snapshot, false);
            }
            deleteRepository(repository);
        }
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
        assertOK(response);
        assertFalse((Boolean) XContentMapValues.extractValue("errors", responseAsMap(response)));
    }

    private static void createSnapshotOfIndex(String repository, String snapshot, String indexName) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "/_snapshot/" + repository + '/' + snapshot);
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity("{\"indices\":\"" + indexName + "\",\"include_global_state\":false}");
        assertOK(client().performRequest(request));
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
        request.addParameter("storage", storage.storageName());
        request.setJsonEntity(Strings.format("""
            {
              "index": "%s",
              "renamed_index": "%s",
              "index_settings": %s
            }""", indexName, renamedIndex, Strings.toString(indexSettings)));
        assertOK(client().performRequest(request));
    }

    private static void assertHitCount(String indexName, Matcher<Long> countMatcher) throws IOException {
        final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, "/" + indexName + "/_count"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        final Map<String, Object> responseAsMap = responseAsMap(response);
        final Number count = (Number) extractValue("count", responseAsMap);
        assertThat(responseAsMap + "", count, notNullValue());
        assertThat(count.longValue(), countMatcher);
        assertThat(((Number) extractValue("_shards.failed", responseAsMap)).intValue(), equalTo(0));
    }

    @SuppressWarnings("unchecked")
    private static List<NodeInfo> getNodeInfos() throws IOException {
        final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, "_nodes/_all"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        final Map<String, Object> nodes = (Map<String, Object>) extractValue(responseAsMap(response), "nodes");
        assertNotNull("Nodes info is null", nodes);
        return nodes.entrySet().stream().map(e -> {
            final Map<?, ?> info = (Map<?, ?>) e.getValue();
            return new NodeInfo(e.getKey(), (String) extractValue(info, "version"), (String) extractValue(info, "build_hash"));
        }).toList();
    }

    private static Settings repositorySettings(String repository) {
        return Settings.builder().put("location", "./" + repository).build();
    }

    private record NodeInfo(String nodeId, String version, String buildHash) {}
}
