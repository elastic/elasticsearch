/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.ShardsAcknowledgedResponse;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class OldRepositoryAccessIT extends ESRestTestCase {

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
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
        boolean afterRestart = Booleans.parseBoolean(System.getProperty("tests.after_restart"));
        String repoLocation = System.getProperty("tests.repo.location");
        repoLocation = PathUtils.get(repoLocation).resolve("source_only_" + sourceOnlyRepository).toString();
        Version oldVersion = Version.fromString(System.getProperty("tests.es.version"));
        assumeTrue(
            "source only repositories only supported since ES 6.5.0",
            sourceOnlyRepository == false || oldVersion.onOrAfter(Version.fromString("6.5.0"))
        );

        assertThat("Index version should be added to archive tests", oldVersion, lessThan(Version.V_8_10_0));
        IndexVersion indexVersion = IndexVersion.fromId(oldVersion.id);

        int oldEsPort = Integer.parseInt(System.getProperty("tests.es.port"));
        String indexName;
        if (sourceOnlyRepository) {
            indexName = "source_only_test_index";
        } else {
            indexName = "test_index";
        }
        int numDocs = 10;
        int extraDocs = 1;
        final Set<String> expectedIds = new HashSet<>();
        try (
            RestHighLevelClient client = highLevelClient(adminClient());
            RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()
        ) {
            if (afterRestart == false) {
                beforeRestart(sourceOnlyRepository, repoLocation, oldVersion, indexVersion, numDocs, extraDocs, expectedIds, client, oldEs, indexName);
            } else {
                afterRestart(indexName);
            }
        }
    }

    private void afterRestart(String indexName) throws IOException {
        ensureGreen("restored_" + indexName);
        ensureGreen("mounted_full_copy_" + indexName);
        ensureGreen("mounted_shared_cache_" + indexName);
    }

    @SuppressWarnings("removal")
    private void beforeRestart(
        boolean sourceOnlyRepository,
        String repoLocation,
        Version oldVersion,
        IndexVersion indexVersion,
        int numDocs,
        int extraDocs,
        Set<String> expectedIds,
        RestHighLevelClient client,
        RestClient oldEs,
        String indexName
    ) throws IOException {
        String repoName = "repo_" + indexName;
        String snapshotName = "snap_" + indexName;
        Request createIndex = new Request("PUT", "/" + indexName);
        int numberOfShards = randomIntBetween(1, 3);

        XContentBuilder settingsBuilder = XContentFactory.jsonBuilder().startObject().startObject("settings");
        settingsBuilder.field("index.number_of_shards", numberOfShards);

        // 6.5.0 started using soft-deletes, but it was only enabled by default on 7.0
        if (oldVersion.onOrAfter(Version.fromString("6.5.0")) && oldVersion.before(Version.fromString("7.0.0")) && randomBoolean()) {
            settingsBuilder.field("index.soft_deletes.enabled", true);
        }

        settingsBuilder.endObject().endObject();

        createIndex.setJsonEntity(Strings.toString(settingsBuilder));
        assertOK(oldEs.performRequest(createIndex));

        for (int i = 0; i < numDocs + extraDocs; i++) {
            String id = "testdoc" + i;
            expectedIds.add(id);
            // use multiple types for ES versions < 6.0.0
            String type = getType(oldVersion, id);
            Request doc = new Request("PUT", "/" + indexName + "/" + type + "/" + id);
            doc.addParameter("refresh", "true");
            doc.setJsonEntity(sourceForDoc(i));
            assertOK(oldEs.performRequest(doc));
        }

        for (int i = 0; i < extraDocs; i++) {
            String id = randomFrom(expectedIds);
            expectedIds.remove(id);
            String type = getType(oldVersion, id);
            Request doc = new Request("DELETE", "/" + indexName + "/" + type + "/" + id);
            doc.addParameter("refresh", "true");
            oldEs.performRequest(doc);
        }

        // register repo on old ES and take snapshot
        Request createRepoRequest = new Request("PUT", "/_snapshot/" + repoName);
        createRepoRequest.setJsonEntity(sourceOnlyRepository ? Strings.format("""
            {"type":"source","settings":{"location":"%s","delegate_type":"fs"}}
            """, repoLocation) : Strings.format("""
            {"type":"fs","settings":{"location":"%s"}}
            """, repoLocation));
        assertOK(oldEs.performRequest(createRepoRequest));

        Request createSnapshotRequest = new Request("PUT", "/_snapshot/" + repoName + "/" + snapshotName);
        createSnapshotRequest.addParameter("wait_for_completion", "true");
        createSnapshotRequest.setJsonEntity("{\"indices\":\"" + indexName + "\"}");
        assertOK(oldEs.performRequest(createSnapshotRequest));

        // register repo on new ES
        Settings.Builder repoSettingsBuilder = Settings.builder().put("location", repoLocation);
        if (sourceOnlyRepository) {
            repoSettingsBuilder.put("delegate_type", "fs");
        }
        Request createRepo = new Request("PUT", "/_snapshot/" + repoName);
        createRepo.setJsonEntity(
            Strings.toString(new PutRepositoryRequest().type(sourceOnlyRepository ? "source" : "fs").settings(repoSettingsBuilder.build()))
        );
        assertAcknowledged(client().performRequest(createRepo));

        // list snapshots on new ES
        Request getSnaps = new Request("GET", "/_snapshot/" + repoName + "/_all");
        Response getResponse = client().performRequest(getSnaps);
        ObjectPath getResp = ObjectPath.createFromResponse(getResponse);
        assertThat(getResp.evaluate("total"), equalTo(1));
        assertThat(getResp.evaluate("snapshots.0.snapshot"), equalTo(snapshotName));
        assertThat(getResp.evaluate("snapshots.0.repository"), equalTo(repoName));
        assertThat(getResp.evaluate("snapshots.0.indices"), contains(indexName));
        assertThat(getResp.evaluate("snapshots.0.state"), equalTo(SnapshotState.SUCCESS.toString()));
        assertEquals(numberOfShards, (int) getResp.evaluate("snapshots.0.shards.successful"));
        assertEquals(numberOfShards, (int) getResp.evaluate("snapshots.0.shards.total"));
        assertEquals(0, (int) getResp.evaluate("snapshots.0.shards.failed"));
        assertEquals(indexVersion.toString(), getResp.evaluate("snapshots.0.version"));

        // list specific snapshot on new ES
        getSnaps = new Request("GET", "/_snapshot/" + repoName + "/" + snapshotName);
        getResponse = client().performRequest(getSnaps);
        getResp = ObjectPath.createFromResponse(getResponse);
        assertThat(getResp.evaluate("total"), equalTo(1));
        assertThat(getResp.evaluate("snapshots.0.snapshot"), equalTo(snapshotName));
        assertThat(getResp.evaluate("snapshots.0.repository"), equalTo(repoName));
        assertThat(getResp.evaluate("snapshots.0.indices"), contains(indexName));
        assertThat(getResp.evaluate("snapshots.0.state"), equalTo(SnapshotState.SUCCESS.toString()));
        assertEquals(numberOfShards, (int) getResp.evaluate("snapshots.0.shards.successful"));
        assertEquals(numberOfShards, (int) getResp.evaluate("snapshots.0.shards.total"));
        assertEquals(0, (int) getResp.evaluate("snapshots.0.shards.failed"));
        assertEquals(indexVersion.toString(), getResp.evaluate("snapshots.0.version"));

        // list advanced snapshot info on new ES
        getSnaps = new Request("GET", "/_snapshot/" + repoName + "/" + snapshotName + "/_status");
        getResponse = client().performRequest(getSnaps);
        getResp = ObjectPath.createFromResponse(getResponse);
        assertThat(((List<?>) getResp.evaluate("snapshots")).size(), equalTo(1));
        assertThat(getResp.evaluate("snapshots.0.snapshot"), equalTo(snapshotName));
        assertThat(getResp.evaluate("snapshots.0.repository"), equalTo(repoName));
        assertThat(((Map<?, ?>) getResp.evaluate("snapshots.0.indices")).keySet(), contains(indexName));
        assertThat(getResp.evaluate("snapshots.0.state"), equalTo(SnapshotState.SUCCESS.toString()));
        assertEquals(numberOfShards, (int) getResp.evaluate("snapshots.0.shards_stats.done"));
        assertEquals(numberOfShards, (int) getResp.evaluate("snapshots.0.shards_stats.total"));
        assertEquals(0, (int) getResp.evaluate("snapshots.0.shards_stats.failed"));
        assertThat(getResp.evaluate("snapshots.0.stats.total.size_in_bytes"), greaterThan(0));
        assertThat(getResp.evaluate("snapshots.0.stats.total.file_count"), greaterThan(0));

        // restore / mount and check whether searches work
        restoreMountAndVerify(
            numDocs,
            expectedIds,
            client,
            numberOfShards,
            sourceOnlyRepository,
            oldVersion,
            indexName,
            repoName,
            snapshotName
        );

        // close indices
        assertTrue(closeIndex(client(), "restored_" + indexName).isShardsAcknowledged());
        assertTrue(closeIndex(client(), "mounted_full_copy_" + indexName).isShardsAcknowledged());
        assertTrue(closeIndex(client(), "mounted_shared_cache_" + indexName).isShardsAcknowledged());

        // restore / mount again
        restoreMountAndVerify(
            numDocs,
            expectedIds,
            client,
            numberOfShards,
            sourceOnlyRepository,
            oldVersion,
            indexName,
            repoName,
            snapshotName
        );
    }

    private String getType(Version oldVersion, String id) {
        return "doc" + (oldVersion.before(Version.fromString("6.0.0")) ? Math.abs(Murmur3HashFunction.hash(id) % 2) : 0);
    }

    private static String sourceForDoc(int i) {
        return "{\"test\":\"test" + i + "\",\"val\":" + i + ",\"create_date\":\"2020-01-" + Strings.format("%02d", i + 1) + "\"}";
    }

    @SuppressWarnings("removal")
    private void restoreMountAndVerify(
        int numDocs,
        Set<String> expectedIds,
        RestHighLevelClient client,
        int numberOfShards,
        boolean sourceOnlyRepository,
        Version oldVersion,
        String indexName,
        String repoName,
        String snapshotName
    ) throws IOException {
        // restore index
        Request restoreRequest = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_restore");
        restoreRequest.setJsonEntity(
            Strings.toString(new RestoreSnapshotRequest().indices(indexName).renamePattern("(.+)").renameReplacement("restored_$1"))
        );
        restoreRequest.addParameter("wait_for_completion", "true");
        Response restoreResponse = client().performRequest(restoreRequest);
        ObjectPath restore = ObjectPath.createFromResponse(restoreResponse);
        assertEquals(numberOfShards, (int) restore.evaluate("snapshot.shards.total"));
        assertEquals(numberOfShards, (int) restore.evaluate("snapshot.shards.successful"));

        ensureGreen("restored_" + indexName);

        String restoredIndex = "restored_" + indexName;
        var response = responseAsMap(client().performRequest(new Request("GET", "/" + restoredIndex + "/_mapping")));
        Map<?, ?> mapping = ObjectPath.evaluate(response, restoredIndex + ".mappings");
        logger.info("mapping for {}: {}", restoredIndex, mapping);
        assertThat(mapping, hasKey("_meta"));
        assertThat(mapping.get("_meta"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        assertThat(meta, hasKey("legacy_mappings"));
        assertThat(meta.get("legacy_mappings"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> legacyMappings = (Map<String, Object>) meta.get("legacy_mappings");
        assertThat(legacyMappings.keySet(), not(empty()));
        for (Map.Entry<String, Object> entry : legacyMappings.entrySet()) {
            String type = entry.getKey();
            assertThat(type, startsWith("doc"));
            assertThat(entry.getValue(), instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> legacyMapping = (Map<String, Object>) entry.getValue();
            assertThat(legacyMapping, hasKey("properties"));
            assertThat(legacyMapping.get("properties"), instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> propertiesMapping = (Map<String, Object>) legacyMapping.get("properties");
            assertThat(propertiesMapping, hasKey("val"));
            assertThat(propertiesMapping.get("val"), instanceOf(Map.class));
            @SuppressWarnings("unchecked")
            Map<String, Object> valMapping = (Map<String, Object>) propertiesMapping.get("val");
            assertThat(valMapping, hasKey("type"));
            assertEquals("long", valMapping.get("type"));
        }

        // run a search against the index
        assertDocs("restored_" + indexName, numDocs, expectedIds, client, sourceOnlyRepository, oldVersion, numberOfShards);

        // mount as full copy searchable snapshot
        Request mountRequest = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_mount");
        mountRequest.setJsonEntity(
            "{\"index\": \""
                + indexName
                + "\",\"renamed_index\": \"mounted_full_copy_"
                + indexName
                + "\",\"index_settings\": {\"index.number_of_replicas\": 1}}"
        );
        mountRequest.addParameter("wait_for_completion", "true");
        ObjectPath mountResponse = ObjectPath.createFromResponse(client().performRequest(mountRequest));
        assertNotNull(mountResponse.evaluate("snapshot"));
        assertEquals(numberOfShards, (int) mountResponse.evaluate("snapshot.shards.total"));
        assertEquals(numberOfShards, (int) mountResponse.evaluate("snapshot.shards.successful"));

        ensureGreen("mounted_full_copy_" + indexName);

        // run a search against the index
        assertDocs("mounted_full_copy_" + indexName, numDocs, expectedIds, client, sourceOnlyRepository, oldVersion, numberOfShards);

        // mount as shared cache searchable snapshot
        mountRequest = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_mount");
        mountRequest.setJsonEntity("{\"index\": \"" + indexName + "\",\"renamed_index\": \"mounted_shared_cache_" + indexName + "\"}");
        mountRequest.addParameter("wait_for_completion", "true");
        mountRequest.addParameter("storage", "shared_cache");
        mountResponse = ObjectPath.createFromResponse(client().performRequest(mountRequest));
        assertNotNull(mountResponse.evaluate("snapshot"));
        assertEquals(numberOfShards, (int) mountResponse.evaluate("snapshot.shards.total"));
        assertEquals(numberOfShards, (int) mountResponse.evaluate("snapshot.shards.successful"));

        // run a search against the index
        assertDocs("mounted_shared_cache_" + indexName, numDocs, expectedIds, client, sourceOnlyRepository, oldVersion, numberOfShards);
    }

    @SuppressWarnings("removal")
    private void assertDocs(
        String index,
        int numDocs,
        Set<String> expectedIds,
        RestHighLevelClient client,
        boolean sourceOnlyRepository,
        Version oldVersion,
        int numberOfShards
    ) throws IOException {
        RequestOptions v7RequestOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Content-Type", "application/vnd.elasticsearch+json;compatible-with=7")
            .addHeader("Accept", "application/vnd.elasticsearch+json;compatible-with=7")
            .build();
        RequestOptions randomRequestOptions = randomBoolean() ? RequestOptions.DEFAULT : v7RequestOptions;

        // run a search against the index
        SearchResponse searchResponse = client.search(new SearchRequest(index), randomRequestOptions);
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
            randomRequestOptions
        );
        logger.info(searchResponse);
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        assertEquals(id, searchResponse.getHits().getHits()[0].getId());
        assertEquals(sourceForDoc(num), searchResponse.getHits().getHits()[0].getSourceAsString());

        if (sourceOnlyRepository == false) {
            // search using reverse sort on val
            searchResponse = client.search(
                new SearchRequest(index).source(
                    SearchSourceBuilder.searchSource()
                        .query(QueryBuilders.matchAllQuery())
                        .sort(SortBuilders.fieldSort("val").order(SortOrder.DESC))
                ),
                randomRequestOptions
            );
            logger.info(searchResponse);
            // check sort order
            assertEquals(
                expectedIds.stream().sorted(Comparator.comparingInt(this::getIdAsNumeric).reversed()).collect(Collectors.toList()),
                Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toList())
            );

            // look up postings
            searchResponse = client.search(
                new SearchRequest(index).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("test", "test" + num))),
                randomRequestOptions
            );
            logger.info(searchResponse);
            // check match
            ElasticsearchAssertions.assertSearchHits(searchResponse, id);

            if (oldVersion.before(Version.fromString("6.0.0"))) {
                // search on _type and check that results contain _type information
                String randomType = getType(oldVersion, randomFrom(expectedIds));
                long typeCount = expectedIds.stream().filter(idd -> getType(oldVersion, idd).equals(randomType)).count();
                searchResponse = client.search(
                    new SearchRequest(index).source(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("_type", randomType))),
                    randomRequestOptions
                );
                logger.info(searchResponse);
                assertEquals(typeCount, searchResponse.getHits().getTotalHits().value);
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    DocumentField typeField = hit.field("_type");
                    assertNotNull(typeField);
                    assertThat(typeField.getValue(), instanceOf(String.class));
                    assertEquals(randomType, typeField.getValue());
                }
            }

            assertThat(
                expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET", "/" + index + "/_doc/" + id)))
                    .getMessage(),
                containsString("get operations not allowed on a legacy index")
            );

            // check that shards are skipped based on non-matching date
            searchResponse = client.search(
                new SearchRequest(index).source(
                    SearchSourceBuilder.searchSource().query(QueryBuilders.rangeQuery("create_date").from("2020-02-01"))
                ),
                randomRequestOptions
            );
            logger.info(searchResponse);
            assertEquals(0, searchResponse.getHits().getTotalHits().value);
            assertEquals(numberOfShards, searchResponse.getSuccessfulShards());
            // When all shards are skipped, at least one of them is queried in order to provide a proper search response.
            assertEquals(numberOfShards - 1, searchResponse.getSkippedShards());
        }
    }

    private int getIdAsNumeric(String id) {
        return Integer.parseInt(id.substring("testdoc".length()));
    }

    static ShardsAcknowledgedResponse closeIndex(RestClient client, String index) throws IOException {
        Request request = new Request("POST", "/" + index + "/_close");
        Response response = client.performRequest(request);
        return ShardsAcknowledgedResponse.fromXContent(responseAsParser(response));
    }
}
