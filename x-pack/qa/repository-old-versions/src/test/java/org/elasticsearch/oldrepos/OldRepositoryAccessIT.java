/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import org.apache.http.HttpHost;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.searchable_snapshots.MountSnapshotRequest;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class OldRepositoryAccessIT extends ESRestTestCase {
    @Override
    protected Map<String, List<Map<?, ?>>> wipeSnapshots() {
        return Collections.emptyMap();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @SuppressWarnings("removal")
    protected static RestHighLevelClient highLevelClient(RestClient client) {
        return new RestHighLevelClient(client, ignore -> {}, Collections.emptyList()) {
        };
    }

    @SuppressWarnings("removal")
    public void testOldRepoAccess() throws IOException {
        String repoLocation = System.getProperty("tests.repo.location");
        Version oldVersion = Version.fromString(System.getProperty("tests.es.version"));

        int oldEsPort = Integer.parseInt(System.getProperty("tests.es.port"));
        int numDocs = 5;
        final Set<String> expectedIds = new HashSet<>();
        try (
            RestHighLevelClient client = highLevelClient(adminClient());
            RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()
        ) {
            try {
                Request createIndex = new Request("PUT", "/test");
                int numberOfShards = randomIntBetween(1, 3);
                createIndex.setJsonEntity("""
                    {"settings":{"number_of_shards": %s}}
                    """.formatted(numberOfShards));
                oldEs.performRequest(createIndex);

                for (int i = 0; i < numDocs; i++) {
                    String id = "testdoc" + i;
                    expectedIds.add(id);
                    Request doc = new Request("PUT", "/test/doc/" + id);
                    doc.addParameter("refresh", "true");
                    doc.setJsonEntity(sourceForDoc(i));
                    oldEs.performRequest(doc);
                }

                // register repo on old ES and take snapshot
                Request createRepoRequest = new Request("PUT", "/_snapshot/testrepo");
                createRepoRequest.setJsonEntity("""
                    {"type":"fs","settings":{"location":"%s"}}
                    """.formatted(repoLocation));
                oldEs.performRequest(createRepoRequest);

                Request createSnapshotRequest = new Request("PUT", "/_snapshot/testrepo/snap1");
                createSnapshotRequest.addParameter("wait_for_completion", "true");
                createSnapshotRequest.setJsonEntity("{\"indices\":\"test\"}");
                oldEs.performRequest(createSnapshotRequest);

                // register repo on new ES
                Settings.Builder repoSettingsBuilder = Settings.builder().put("location", repoLocation);
                if (Build.CURRENT.isSnapshot()) {
                    repoSettingsBuilder.put("allow_bwc_indices", true);
                }
                ElasticsearchAssertions.assertAcked(
                    client.snapshot()
                        .createRepository(
                            new PutRepositoryRequest("testrepo").type("fs").settings(repoSettingsBuilder),
                            RequestOptions.DEFAULT
                        )
                );

                // list snapshots on new ES
                List<SnapshotInfo> snapshotInfos = client.snapshot()
                    .get(new GetSnapshotsRequest("testrepo").snapshots(new String[] { "_all" }), RequestOptions.DEFAULT)
                    .getSnapshots();
                assertThat(snapshotInfos, hasSize(1));
                SnapshotInfo snapshotInfo = snapshotInfos.get(0);
                assertEquals("snap1", snapshotInfo.snapshotId().getName());
                assertEquals("testrepo", snapshotInfo.repository());
                assertEquals(Arrays.asList("test"), snapshotInfo.indices());
                assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
                assertEquals(numberOfShards, snapshotInfo.successfulShards());
                assertEquals(numberOfShards, snapshotInfo.totalShards());
                assertEquals(0, snapshotInfo.failedShards());
                assertEquals(oldVersion, snapshotInfo.version());

                // list specific snapshot on new ES
                snapshotInfos = client.snapshot()
                    .get(new GetSnapshotsRequest("testrepo").snapshots(new String[] { "snap1" }), RequestOptions.DEFAULT)
                    .getSnapshots();
                assertThat(snapshotInfos, hasSize(1));
                snapshotInfo = snapshotInfos.get(0);
                assertEquals("snap1", snapshotInfo.snapshotId().getName());
                assertEquals("testrepo", snapshotInfo.repository());
                assertEquals(Arrays.asList("test"), snapshotInfo.indices());
                assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
                assertEquals(numberOfShards, snapshotInfo.successfulShards());
                assertEquals(numberOfShards, snapshotInfo.totalShards());
                assertEquals(0, snapshotInfo.failedShards());
                assertEquals(oldVersion, snapshotInfo.version());

                // list advanced snapshot info on new ES
                SnapshotsStatusResponse snapshotsStatusResponse = client.snapshot()
                    .status(new SnapshotsStatusRequest("testrepo").snapshots(new String[] { "snap1" }), RequestOptions.DEFAULT);
                assertThat(snapshotsStatusResponse.getSnapshots(), hasSize(1));
                SnapshotStatus snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
                assertEquals("snap1", snapshotStatus.getSnapshot().getSnapshotId().getName());
                assertEquals("testrepo", snapshotStatus.getSnapshot().getRepository());
                assertEquals(Sets.newHashSet("test"), snapshotStatus.getIndices().keySet());
                assertEquals(SnapshotsInProgress.State.SUCCESS, snapshotStatus.getState());
                assertEquals(numberOfShards, snapshotStatus.getShardsStats().getDoneShards());
                assertEquals(numberOfShards, snapshotStatus.getShardsStats().getTotalShards());
                assertEquals(0, snapshotStatus.getShardsStats().getFailedShards());
                assertThat(snapshotStatus.getStats().getTotalSize(), greaterThan(0L));
                assertThat(snapshotStatus.getStats().getTotalFileCount(), greaterThan(0));

                if (Build.CURRENT.isSnapshot()) {
                    // restore / mount and check whether searches work
                    restoreMountAndVerify(numDocs, expectedIds, client, numberOfShards);

                    // close indices
                    assertTrue(
                        client.indices().close(new CloseIndexRequest("restored_test"), RequestOptions.DEFAULT).isShardsAcknowledged()
                    );
                    assertTrue(
                        client.indices()
                            .close(new CloseIndexRequest("mounted_full_copy_test"), RequestOptions.DEFAULT)
                            .isShardsAcknowledged()
                    );
                    assertTrue(
                        client.indices()
                            .close(new CloseIndexRequest("mounted_shared_cache_test"), RequestOptions.DEFAULT)
                            .isShardsAcknowledged()
                    );

                    // restore / mount again
                    restoreMountAndVerify(numDocs, expectedIds, client, numberOfShards);
                }
            } finally {
                oldEs.performRequest(new Request("DELETE", "/test"));
            }
        }
    }

    private static String sourceForDoc(int i) {
        return "{\"test\":\"test" + i + "\",\"val\":" + i + "}";
    }

    @SuppressWarnings("removal")
    private void restoreMountAndVerify(int numDocs, Set<String> expectedIds, RestHighLevelClient client, int numberOfShards)
        throws IOException {
        // restore index
        RestoreSnapshotResponse restoreSnapshotResponse = client.snapshot()
            .restore(
                new RestoreSnapshotRequest("testrepo", "snap1").indices("test")
                    .renamePattern("(.+)")
                    .renameReplacement("restored_$1")
                    .waitForCompletion(true),
                RequestOptions.DEFAULT
            );
        assertNotNull(restoreSnapshotResponse.getRestoreInfo());
        assertEquals(numberOfShards, restoreSnapshotResponse.getRestoreInfo().totalShards());
        assertEquals(numberOfShards, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(
            ClusterHealthStatus.GREEN,
            client.cluster()
                .health(
                    new ClusterHealthRequest("restored_test").waitForGreenStatus().waitForNoRelocatingShards(true),
                    RequestOptions.DEFAULT
                )
                .getStatus()
        );

        // run a search against the index
        assertDocs("restored_test", numDocs, expectedIds, client);

        // mount as full copy searchable snapshot
        RestoreSnapshotResponse mountSnapshotResponse = client.searchableSnapshots()
            .mountSnapshot(
                new MountSnapshotRequest("testrepo", "snap1", "test").storage(MountSnapshotRequest.Storage.FULL_COPY)
                    .renamedIndex("mounted_full_copy_test")
                    .indexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build())
                    .waitForCompletion(true),
                RequestOptions.DEFAULT
            );
        assertNotNull(mountSnapshotResponse.getRestoreInfo());
        assertEquals(numberOfShards, mountSnapshotResponse.getRestoreInfo().totalShards());
        assertEquals(numberOfShards, mountSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(
            ClusterHealthStatus.GREEN,
            client.cluster()
                .health(
                    new ClusterHealthRequest("mounted_full_copy_test").waitForGreenStatus().waitForNoRelocatingShards(true),
                    RequestOptions.DEFAULT
                )
                .getStatus()
        );

        // run a search against the index
        assertDocs("mounted_full_copy_test", numDocs, expectedIds, client);

        // mount as shared cache searchable snapshot
        mountSnapshotResponse = client.searchableSnapshots()
            .mountSnapshot(
                new MountSnapshotRequest("testrepo", "snap1", "test").storage(MountSnapshotRequest.Storage.SHARED_CACHE)
                    .renamedIndex("mounted_shared_cache_test")
                    .waitForCompletion(true),
                RequestOptions.DEFAULT
            );
        assertNotNull(mountSnapshotResponse.getRestoreInfo());
        assertEquals(numberOfShards, mountSnapshotResponse.getRestoreInfo().totalShards());
        assertEquals(numberOfShards, mountSnapshotResponse.getRestoreInfo().successfulShards());

        // run a search against the index
        assertDocs("mounted_shared_cache_test", numDocs, expectedIds, client);
    }

    @SuppressWarnings("removal")
    private void assertDocs(String index, int numDocs, Set<String> expectedIds, RestHighLevelClient client) throws IOException {
        // run a search against the index
        SearchResponse searchResponse = client.search(new SearchRequest(index), RequestOptions.DEFAULT);
        logger.info(searchResponse);
        // check hit count
        assertEquals(numDocs, searchResponse.getHits().getTotalHits().value);
        // check that _index is properly set
        assertTrue(Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getIndex).allMatch(index::equals));
        // check that all _ids are there
        assertEquals(expectedIds, Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet()));
        // check that _source is present
        assertTrue(Arrays.stream(searchResponse.getHits().getHits()).allMatch(SearchHit::hasSource));
        // check that correct _source present for each document
        for (SearchHit h : searchResponse.getHits().getHits()) {
            assertEquals(sourceForDoc(Integer.parseInt(h.getId().substring("testdoc".length()))), h.getSourceAsString());
        }

        // run a search using runtime fields against the index
        searchResponse = client.search(
            new SearchRequest(index).source(
                SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchQuery("val", 2))
                    .runtimeMappings(Map.of("val", Map.of("type", "long")))
            ),
            RequestOptions.DEFAULT
        );
        logger.info(searchResponse);
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        assertEquals("testdoc2", searchResponse.getHits().getHits()[0].getId());
        assertEquals(sourceForDoc(2), searchResponse.getHits().getHits()[0].getSourceAsString());
    }
}
