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
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.client.searchable_snapshots.MountSnapshotRequest;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

    public void testOldRepoAccess() throws IOException {
        runTest(false);
    }

    public void testOldSourceOnlyRepoAccess() throws IOException {
        runTest(true);
    }

    @SuppressWarnings("removal")
    public void runTest(boolean sourceOnlyRepository) throws IOException {
        String repoLocation = System.getProperty("tests.repo.location");
        Version oldVersion = Version.fromString(System.getProperty("tests.es.version"));
        assumeTrue(
            "source only repositories only supported since ES 6.5.0",
            sourceOnlyRepository == false || oldVersion.onOrAfter(Version.fromString("6.5.0"))
        );

        int oldEsPort = Integer.parseInt(System.getProperty("tests.es.port"));
        int numDocs = 10;
        int extraDocs = 1;
        final Set<String> expectedIds = new HashSet<>();
        try (
            RestHighLevelClient client = highLevelClient(adminClient());
            RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()
        ) {
            try {
                Request createIndex = new Request("PUT", "/test");
                int numberOfShards = randomIntBetween(1, 3);

                XContentBuilder settingsBuilder = XContentFactory.jsonBuilder().startObject().startObject("settings");
                settingsBuilder.field("index.number_of_shards", numberOfShards);

                // 6.5.0 started using soft-deletes, but it was only enabled by default on 7.0
                if (oldVersion.onOrAfter(Version.fromString("6.5.0"))
                    && oldVersion.before(Version.fromString("7.0.0"))
                    && randomBoolean()) {
                    settingsBuilder.field("index.soft_deletes.enabled", true);
                }

                settingsBuilder.endObject().endObject();

                createIndex.setJsonEntity(Strings.toString(settingsBuilder));
                assertOK(oldEs.performRequest(createIndex));

                for (int i = 0; i < numDocs + extraDocs; i++) {
                    String id = "testdoc" + i;
                    expectedIds.add(id);
                    Request doc = new Request("PUT", "/test/doc/" + id);
                    doc.addParameter("refresh", "true");
                    doc.setJsonEntity(sourceForDoc(i));
                    assertOK(oldEs.performRequest(doc));
                }

                for (int i = 0; i < extraDocs; i++) {
                    String id = randomFrom(expectedIds);
                    expectedIds.remove(id);
                    Request doc = new Request("DELETE", "/test/doc/" + id);
                    doc.addParameter("refresh", "true");
                    oldEs.performRequest(doc);
                }

                // register repo on old ES and take snapshot
                Request createRepoRequest = new Request("PUT", "/_snapshot/testrepo");
                createRepoRequest.setJsonEntity(sourceOnlyRepository ? """
                    {"type":"source","settings":{"location":"%s","delegate_type":"fs"}}
                    """.formatted(repoLocation) : """
                    {"type":"fs","settings":{"location":"%s"}}
                    """.formatted(repoLocation));
                assertOK(oldEs.performRequest(createRepoRequest));

                Request createSnapshotRequest = new Request("PUT", "/_snapshot/testrepo/snap1");
                createSnapshotRequest.addParameter("wait_for_completion", "true");
                createSnapshotRequest.setJsonEntity("{\"indices\":\"test\"}");
                assertOK(oldEs.performRequest(createSnapshotRequest));

                // register repo on new ES
                Settings.Builder repoSettingsBuilder = Settings.builder().put("location", repoLocation);
                if (sourceOnlyRepository) {
                    repoSettingsBuilder.put("delegate_type", "fs");
                }
                if (Build.CURRENT.isSnapshot()) {
                    repoSettingsBuilder.put("allow_bwc_indices", true);
                }
                ElasticsearchAssertions.assertAcked(
                    client.snapshot()
                        .createRepository(
                            new PutRepositoryRequest("testrepo").type(sourceOnlyRepository ? "source" : "fs").settings(repoSettingsBuilder),
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
                    restoreMountAndVerify(numDocs, expectedIds, client, numberOfShards, sourceOnlyRepository);

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
                    restoreMountAndVerify(numDocs, expectedIds, client, numberOfShards, sourceOnlyRepository);
                }
            } finally {
                IOUtils.closeWhileHandlingException(
                    () -> oldEs.performRequest(new Request("DELETE", "/test")),
                    () -> oldEs.performRequest(new Request("DELETE", "/_snapshot/testrepo/snap1")),
                    () -> oldEs.performRequest(new Request("DELETE", "/_snapshot/testrepo"))
                );
                if (Build.CURRENT.isSnapshot()) {
                    IOUtils.closeWhileHandlingException(
                        () -> client().performRequest(new Request("DELETE", "/restored_test")),
                        () -> client().performRequest(new Request("DELETE", "/mounted_full_copy_test")),
                        () -> client().performRequest(new Request("DELETE", "/mounted_shared_cache_test"))
                    );
                }
                IOUtils.closeWhileHandlingException(() -> client().performRequest(new Request("DELETE", "/_snapshot/testrepo")));
            }
        }
    }

    private static String sourceForDoc(int i) {
        return "{\"test\":\"test" + i + "\",\"val\":" + i + "}";
    }

    @SuppressWarnings("removal")
    private void restoreMountAndVerify(
        int numDocs,
        Set<String> expectedIds,
        RestHighLevelClient client,
        int numberOfShards,
        boolean sourceOnlyRepository
    ) throws IOException {
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
        assertDocs("restored_test", numDocs, expectedIds, client, sourceOnlyRepository);

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
        assertDocs("mounted_full_copy_test", numDocs, expectedIds, client, sourceOnlyRepository);

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
        assertDocs("mounted_shared_cache_test", numDocs, expectedIds, client, sourceOnlyRepository);
    }

    @SuppressWarnings("removal")
    private void assertDocs(String index, int numDocs, Set<String> expectedIds, RestHighLevelClient client, boolean sourceOnlyRepository)
        throws IOException {
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
            assertEquals(sourceForDoc(getIdAsNumeric(h.getId())), h.getSourceAsString());
        }

        String id = randomFrom(expectedIds);
        int num = getIdAsNumeric(id);
        // run a search using runtime fields against the index
        searchResponse = client.search(
            new SearchRequest(index).source(
                SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchQuery("val", num))
                    .runtimeMappings(Map.of("val", Map.of("type", "long")))
            ),
            RequestOptions.DEFAULT
        );
        logger.info(searchResponse);
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        assertEquals(id, searchResponse.getHits().getHits()[0].getId());
        assertEquals(sourceForDoc(num), searchResponse.getHits().getHits()[0].getSourceAsString());

        if (sourceOnlyRepository == false) {
            // check that doc values can be accessed by (reverse) sorting on numeric val field
            // first add mapping for field (this will be done automatically in the future)
            XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
            mappingBuilder.startObject().startObject("properties").startObject("val");
            mappingBuilder.field("type", "long");
            mappingBuilder.endObject().endObject().endObject();
            assertTrue(
                client.indices().putMapping(new PutMappingRequest(index).source(mappingBuilder), RequestOptions.DEFAULT).isAcknowledged()
            );

            // search using reverse sort on val
            searchResponse = client.search(
                new SearchRequest(index).source(
                    SearchSourceBuilder.searchSource()
                        .query(QueryBuilders.matchAllQuery())
                        .sort(SortBuilders.fieldSort("val").order(SortOrder.DESC))
                ),
                RequestOptions.DEFAULT
            );
            logger.info(searchResponse);
            // check sort order
            assertEquals(
                expectedIds.stream().sorted(Comparator.comparingInt(this::getIdAsNumeric).reversed()).collect(Collectors.toList()),
                Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toList())
            );
        }
    }

    private int getIdAsNumeric(String id) {
        return Integer.parseInt(id.substring("testdoc".length()));
    }
}
