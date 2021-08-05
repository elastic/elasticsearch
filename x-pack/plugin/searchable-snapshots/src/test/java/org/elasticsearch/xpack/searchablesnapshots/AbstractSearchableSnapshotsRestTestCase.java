/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractSearchableSnapshotsRestTestCase extends ESRestTestCase {

    public static final String FROZEN_INDICES_WARNING = "Frozen indices are deprecated because they provide no benefit given "
        + "improvements in heap memory utilization. They will be removed in a future release.";

    private static final String WRITE_REPOSITORY_NAME = "repository";
    private static final String READ_REPOSITORY_NAME = "read-repository";
    private static final String SNAPSHOT_NAME = "searchable-snapshot";

    protected abstract String writeRepositoryType();

    protected abstract Settings writeRepositorySettings();

    protected boolean useReadRepository() {
        return false;
    }

    protected String readRepositoryType() {
        return writeRepositoryType();
    }

    protected Settings readRepositorySettings() {
        return writeRepositorySettings();
    }

    private void runSearchableSnapshotsTest(SearchableSnapshotsTestCaseBody testCaseBody) throws Exception {
        runSearchableSnapshotsTest(testCaseBody, false);
    }

    private void runSearchableSnapshotsTest(SearchableSnapshotsTestCaseBody testCaseBody, boolean sourceOnly) throws Exception {
        final String repositoryType = writeRepositoryType();
        Settings repositorySettings = writeRepositorySettings();
        if (sourceOnly) {
            repositorySettings = Settings.builder().put("delegate_type", repositoryType).put(repositorySettings).build();
        }

        logger.info("creating repository [{}] of type [{}]", WRITE_REPOSITORY_NAME, repositoryType);
        registerRepository(WRITE_REPOSITORY_NAME, sourceOnly ? "source" : repositoryType, true, repositorySettings);

        final String readRepository;
        if (useReadRepository()) {
            final String readRepositoryType = readRepositoryType();
            Settings readRepositorySettings = readRepositorySettings();
            if (sourceOnly) {
                readRepositorySettings = Settings.builder().put("delegate_type", readRepositoryType).put(readRepositorySettings).build();
            }

            logger.info("creating read repository [{}] of type [{}]", READ_REPOSITORY_NAME, readRepositoryType);
            registerRepository(READ_REPOSITORY_NAME, sourceOnly ? "source" : readRepositoryType, true, readRepositorySettings);
            readRepository = READ_REPOSITORY_NAME;
        } else {
            readRepository = WRITE_REPOSITORY_NAME;
        }

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);

        logger.info("creating index [{}]", indexName);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        final int numDocs = randomIntBetween(1, 500);
        logger.info("indexing [{}] documents", numDocs);

        final StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulkBody.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
            bulkBody.append("{\"field\":").append(i).append(",\"text\":\"Document number ").append(i).append("\"}\n");
        }

        final Request documents = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
        documents.addParameter("refresh", Boolean.TRUE.toString());
        documents.setJsonEntity(bulkBody.toString());
        assertOK(client().performRequest(documents));

        if (randomBoolean()) {
            final StringBuilder bulkUpdateBody = new StringBuilder();
            for (int i = 0; i < randomIntBetween(1, numDocs); i++) {
                bulkUpdateBody.append("{\"update\":{\"_id\":\"").append(i).append("\"}}\n");
                bulkUpdateBody.append("{\"doc\":{").append("\"text\":\"Updated document number ").append(i).append("\"}}\n");
            }

            final Request bulkUpdate = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
            bulkUpdate.addParameter("refresh", Boolean.TRUE.toString());
            bulkUpdate.setJsonEntity(bulkUpdateBody.toString());
            assertOK(client().performRequest(bulkUpdate));
        }

        logger.info("force merging index [{}]", indexName);
        forceMerge(indexName, randomBoolean(), randomBoolean());

        // Remove the snapshots, if a previous test failed to delete them. This is
        // useful for third party tests that runs the test against a real external service.
        deleteSnapshot(SNAPSHOT_NAME, true);

        logger.info("creating snapshot [{}]", SNAPSHOT_NAME);
        createSnapshot(WRITE_REPOSITORY_NAME, SNAPSHOT_NAME, true);

        logger.info("deleting index [{}]", indexName);
        deleteIndex(indexName);

        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        logger.info("restoring index [{}] from snapshot [{}] as [{}]", indexName, SNAPSHOT_NAME, restoredIndexName);
        mountSnapshot(indexName, restoredIndexName, readRepository);

        ensureGreen(restoredIndexName);

        final Number count = count(restoredIndexName);
        assertThat("Wrong index count for index " + restoredIndexName, count.intValue(), equalTo(numDocs));

        testCaseBody.runTest(restoredIndexName, numDocs);

        logger.info("deleting mounted index [{}]", indexName);
        deleteIndex(restoredIndexName);

        logger.info("deleting snapshot [{}]", SNAPSHOT_NAME);
        deleteSnapshot(SNAPSHOT_NAME, false);
    }

    public void testSearchResults() throws Exception {
        runSearchableSnapshotsTest((restoredIndexName, numDocs) -> {
            for (int i = 0; i < 10; i++) {
                assertSearchResults(restoredIndexName, numDocs, randomFrom(Boolean.TRUE, Boolean.FALSE, null));
            }
        });
    }

    public void testSearchResultsWhenFrozen() throws Exception {
        runSearchableSnapshotsTest((restoredIndexName, numDocs) -> {
            final Request freezeRequest = new Request(HttpPost.METHOD_NAME, restoredIndexName + "/_freeze");
            freezeRequest.setOptions(expectWarnings(FROZEN_INDICES_WARNING));
            assertOK(client().performRequest(freezeRequest));
            ensureGreen(restoredIndexName);
            assertSearchResults(restoredIndexName, numDocs, Boolean.FALSE);
            final Map<String, Object> frozenIndexSettings = indexSettings(restoredIndexName);
            assertThat(Boolean.valueOf(extractValue(frozenIndexSettings, "index.frozen")), equalTo(true));
            assertThat(Boolean.valueOf(extractValue(frozenIndexSettings, "index.search.throttled")), equalTo(true));
            assertThat(Boolean.valueOf(extractValue(frozenIndexSettings, "index.blocks.write")), equalTo(true));

            final Request unfreezeRequest = new Request(HttpPost.METHOD_NAME, restoredIndexName + "/_unfreeze");
            unfreezeRequest.setOptions(expectWarnings(FROZEN_INDICES_WARNING));
            assertOK(client().performRequest(unfreezeRequest));
            ensureGreen(restoredIndexName);
            assertSearchResults(restoredIndexName, numDocs, Boolean.FALSE);
            final Map<String, Object> unfrozenIndexSettings = indexSettings(restoredIndexName);
            assertThat(extractValue(unfrozenIndexSettings, "index.frozen"), nullValue());
            assertThat(extractValue(unfrozenIndexSettings, "index.search.throttled"), nullValue());
            assertThat(Boolean.valueOf(extractValue(frozenIndexSettings, "index.blocks.write")), equalTo(true));
        });
    }

    public void testSourceOnlyRepository() throws Exception {
        runSearchableSnapshotsTest((indexName, numDocs) -> {
            for (int i = 0; i < 10; i++) {
                if (randomBoolean()) {
                    logger.info("clearing searchable snapshots cache for [{}] before search", indexName);
                    clearCache(indexName);
                }
                Map<String, Object> searchResults = search(
                    indexName,
                    QueryBuilders.matchAllQuery(),
                    randomFrom(Boolean.TRUE, Boolean.FALSE, null)
                );
                assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));
            }
        }, true);
    }

    public void testCloseAndReopen() throws Exception {
        runSearchableSnapshotsTest((restoredIndexName, numDocs) -> {
            closeIndex(restoredIndexName);
            ensureGreen(restoredIndexName);

            final Request openRequest = new Request(HttpPost.METHOD_NAME, restoredIndexName + "/_open");
            assertOK(client().performRequest(openRequest));
            ensureGreen(restoredIndexName);

            for (int i = 0; i < 10; i++) {
                assertSearchResults(restoredIndexName, numDocs, randomFrom(Boolean.TRUE, Boolean.FALSE, null));
            }
        });
    }

    public void testStats() throws Exception {
        runSearchableSnapshotsTest((restoredIndexName, numDocs) -> {
            final Map<String, Object> stats = searchableSnapshotStats(restoredIndexName);
            assertThat("Expected searchable snapshots stats for [" + restoredIndexName + ']', stats.size(), greaterThan(0));

            final int nbShards = Integer.valueOf(extractValue(indexSettings(restoredIndexName), IndexMetadata.SETTING_NUMBER_OF_SHARDS));
            assertThat("Expected searchable snapshots stats for " + nbShards + " shards but got " + stats, stats.size(), equalTo(nbShards));
        });
    }

    public void testClearCache() throws Exception {
        @SuppressWarnings("unchecked")
        final Function<Map<?, ?>, Long> sumCachedBytesWritten = stats -> stats.values()
            .stream()
            .filter(o -> o instanceof List)
            .flatMap(o -> ((List) o).stream())
            .filter(o -> o instanceof Map)
            .map(o -> ((Map<?, ?>) o).get("files"))
            .filter(o -> o instanceof List)
            .flatMap(o -> ((List) o).stream())
            .filter(o -> o instanceof Map)
            .map(o -> ((Map<?, ?>) o).get("cached_bytes_written"))
            .filter(o -> o instanceof Map)
            .map(o -> ((Map<?, ?>) o).get("sum"))
            .mapToLong(o -> ((Number) o).longValue())
            .sum();

        runSearchableSnapshotsTest((restoredIndexName, numDocs) -> {

            Map<String, Object> searchResults = search(restoredIndexName, QueryBuilders.matchAllQuery(), Boolean.TRUE);
            assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));

            waitForIdlingSearchableSnapshotsThreadPools();

            final long bytesInCacheBeforeClear = sumCachedBytesWritten.apply(searchableSnapshotStats(restoredIndexName));
            assertThat(bytesInCacheBeforeClear, greaterThan(0L));

            clearCache(restoredIndexName);

            final long bytesInCacheAfterClear = sumCachedBytesWritten.apply(searchableSnapshotStats(restoredIndexName));
            assertThat(bytesInCacheAfterClear, equalTo(bytesInCacheBeforeClear));

            searchResults = search(restoredIndexName, QueryBuilders.matchAllQuery(), Boolean.TRUE);
            assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));

            waitForIdlingSearchableSnapshotsThreadPools();

            assertBusy(() -> {
                final long bytesInCacheAfterSearch = sumCachedBytesWritten.apply(searchableSnapshotStats(restoredIndexName));
                assertThat(bytesInCacheAfterSearch, greaterThanOrEqualTo(bytesInCacheBeforeClear));
            });
        });
    }

    public void testSnapshotOfSearchableSnapshot() throws Exception {
        runSearchableSnapshotsTest((restoredIndexName, numDocs) -> {

            final boolean frozen = randomBoolean();
            if (frozen) {
                logger.info("--> freezing index [{}]", restoredIndexName);
                final Request freezeRequest = new Request(HttpPost.METHOD_NAME, restoredIndexName + "/_freeze");
                freezeRequest.setOptions(expectWarnings(FROZEN_INDICES_WARNING));
                assertOK(client().performRequest(freezeRequest));
            }

            if (randomBoolean()) {
                logger.info("--> closing index [{}]", restoredIndexName);
                final Request closeRequest = new Request(HttpPost.METHOD_NAME, restoredIndexName + "/_close");
                closeRequest.addParameter("wait_for_active_shards", "all");
                assertOK(client().performRequest(closeRequest));
            }

            ensureGreen(restoredIndexName);

            final String snapshot2Name = "snapshotception";

            // Remove the snapshots, if a previous test failed to delete them. This is
            // useful for third party tests that runs the test against a real external service.
            deleteSnapshot(snapshot2Name, true);

            final Request snapshotRequest = new Request(HttpPut.METHOD_NAME, "_snapshot/" + WRITE_REPOSITORY_NAME + '/' + snapshot2Name);
            snapshotRequest.addParameter("wait_for_completion", "true");
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                builder.field("indices", restoredIndexName);
                builder.field("include_global_state", "false");
                builder.endObject();
                snapshotRequest.setEntity(new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON));
            }
            assertOK(client().performRequest(snapshotRequest));

            final List<Map<String, Map<String, Object>>> snapshotShardsStats = extractValue(
                responseAsMap(
                    client().performRequest(
                        new Request(HttpGet.METHOD_NAME, "/_snapshot/" + WRITE_REPOSITORY_NAME + "/" + snapshot2Name + "/_status")
                    )
                ),
                "snapshots.indices." + restoredIndexName + ".shards"
            );

            assertThat(snapshotShardsStats.size(), equalTo(1));
            for (Map<String, Object> value : snapshotShardsStats.get(0).values()) {
                assertThat(extractValue(value, "stats.total.file_count"), equalTo(1));
                assertThat(extractValue(value, "stats.incremental.file_count"), lessThanOrEqualTo(1));
            }

            deleteIndex(restoredIndexName);

            restoreSnapshot(WRITE_REPOSITORY_NAME, snapshot2Name, true);
            ensureGreen(restoredIndexName);

            deleteSnapshot(snapshot2Name, false);

            assertSearchResults(restoredIndexName, numDocs, frozen ? Boolean.FALSE : randomFrom(Boolean.TRUE, Boolean.FALSE, null));
        });
    }

    private void clearCache(String restoredIndexName) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, restoredIndexName + "/_searchable_snapshots/cache/clear");
        assertOK(client().performRequest(request));
    }

    public void assertSearchResults(String indexName, int numDocs, Boolean ignoreThrottled) throws IOException {

        if (randomBoolean()) {
            logger.info("clearing searchable snapshots cache for [{}] before search", indexName);
            clearCache(indexName);
        }

        final int randomTieBreaker = randomIntBetween(0, numDocs - 1);
        Map<String, Object> searchResults;
        switch (randomInt(3)) {
            case 0:
                searchResults = search(indexName, QueryBuilders.termQuery("field", String.valueOf(randomTieBreaker)), ignoreThrottled);
                assertThat(extractValue(searchResults, "hits.total.value"), equalTo(1));
                @SuppressWarnings("unchecked")
                Map<String, Object> searchHit = (Map<String, Object>) ((List<?>) extractValue(searchResults, "hits.hits")).get(0);
                assertThat(extractValue(searchHit, "_index"), equalTo(indexName));
                assertThat(extractValue(searchHit, "_source.field"), equalTo(randomTieBreaker));
                break;
            case 1:
                searchResults = search(indexName, QueryBuilders.rangeQuery("field").lt(randomTieBreaker), ignoreThrottled);
                assertThat(extractValue(searchResults, "hits.total.value"), equalTo(randomTieBreaker));
                break;
            case 2:
                searchResults = search(indexName, QueryBuilders.rangeQuery("field").gte(randomTieBreaker), ignoreThrottled);
                assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs - randomTieBreaker));
                break;
            case 3:
                searchResults = search(indexName, QueryBuilders.matchQuery("text", "document"), ignoreThrottled);
                assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));
                break;
            default:
                fail("Unsupported randomized search query");
        }
    }

    protected static void deleteSnapshot(String snapshot, boolean ignoreMissing) throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "_snapshot/" + WRITE_REPOSITORY_NAME + '/' + snapshot);
        try {
            final Response response = client().performRequest(request);
            assertAcked(
                "Failed to delete snapshot [" + snapshot + "] in repository [" + WRITE_REPOSITORY_NAME + "]: " + response,
                response
            );
        } catch (IOException e) {
            if (ignoreMissing && e instanceof ResponseException) {
                Response response = ((ResponseException) e).getResponse();
                assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.NOT_FOUND.getStatus()));
                return;
            }
            throw e;
        }
    }

    protected static void mountSnapshot(String snapshotIndexName, String mountIndexName, String repositoryName) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, "/_snapshot/" + repositoryName + "/" + SNAPSHOT_NAME + "/_mount");
        request.addParameter("wait_for_completion", Boolean.toString(true));
        request.addParameter("storage", randomFrom("full_copy", "shared_cache"));

        final XContentBuilder builder = JsonXContent.contentBuilder().startObject().field("index", snapshotIndexName);
        if (snapshotIndexName.equals(mountIndexName) == false || randomBoolean()) {
            builder.field("renamed_index", mountIndexName);
        }
        builder.endObject();
        request.setJsonEntity(Strings.toString(builder));

        final Response response = client().performRequest(request);
        assertThat(
            "Failed to restore snapshot [" + SNAPSHOT_NAME + "] in repository [" + repositoryName + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    protected static void deleteIndex(String index) throws IOException {
        final Response response = client().performRequest(new Request("DELETE", "/" + index));
        assertAcked("Fail to delete index [" + index + ']', response);
    }

    private static void assertAcked(String message, Response response) throws IOException {
        final int responseStatusCode = response.getStatusLine().getStatusCode();
        assertThat(
            message + ": expecting response code [200] but got [" + responseStatusCode + ']',
            responseStatusCode,
            equalTo(RestStatus.OK.getStatus())
        );
        final Map<String, Object> responseAsMap = responseAsMap(response);
        assertThat(message + ": response is not acknowledged", extractValue(responseAsMap, "acknowledged"), equalTo(Boolean.TRUE));
    }

    protected static void forceMerge(String index, boolean onlyExpungeDeletes, boolean flush) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, '/' + index + "/_forcemerge");
        request.addParameter("only_expunge_deletes", Boolean.toString(onlyExpungeDeletes));
        request.addParameter("flush", Boolean.toString(flush));
        assertOK(client().performRequest(request));
    }

    protected static Number count(String index) throws IOException {
        final Response response = client().performRequest(new Request(HttpPost.METHOD_NAME, '/' + index + "/_count"));
        assertThat(
            "Failed to execute count request on index [" + index + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );

        final Map<String, Object> responseAsMap = responseAsMap(response);
        assertThat(
            "Shard failures when executing count request on index [" + index + "]: " + response,
            extractValue(responseAsMap, "_shards.failed"),
            equalTo(0)
        );
        return (Number) extractValue(responseAsMap, "count");
    }

    protected static Map<String, Object> search(String index, QueryBuilder query, Boolean ignoreThrottled) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, '/' + index + "/_search");
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().trackTotalHits(true).query(query);
        if (randomBoolean()) {
            // ensures we can always access sequence numbers, even on source-only mounted snapshots
            searchSourceBuilder.sort(SeqNoFieldMapper.NAME, SortOrder.ASC);
        }
        request.setJsonEntity(searchSourceBuilder.toString());
        if (ignoreThrottled != null) {
            request.addParameter("ignore_throttled", ignoreThrottled.toString());
        }

        final Response response = client().performRequest(request);
        assertThat(
            "Failed to execute search request on index [" + index + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );

        final Map<String, Object> responseAsMap = responseAsMap(response);
        assertThat(
            "Shard failures when executing search request on index [" + index + "]: " + response,
            extractValue(responseAsMap, "_shards.failed"),
            equalTo(0)
        );
        return responseAsMap;
    }

    protected static Map<String, Object> searchableSnapshotStats(String index) throws IOException {
        final Request request = new Request(HttpGet.METHOD_NAME, '/' + index + "/_searchable_snapshots/stats");
        request.addParameter("level", "shards");
        final Response response = client().performRequest(request);
        assertThat(
            "Failed to retrieve searchable snapshots stats for on index [" + index + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );

        final Map<String, Object> responseAsMap = responseAsMap(response);
        assertThat(
            "Shard failures when retrieving searchable snapshots stats for index [" + index + "]: " + response,
            extractValue(responseAsMap, "_shards.failed"),
            equalTo(0)
        );
        return extractValue(responseAsMap, "indices." + index + ".shards");
    }

    protected static Map<String, Object> indexSettings(String index) throws IOException {
        final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, '/' + index));
        assertThat(
            "Failed to get settings on index [" + index + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
        return extractValue(responseAsMap(response), index + ".settings");
    }

    @SuppressWarnings("unchecked")
    protected static void waitForIdlingSearchableSnapshotsThreadPools() throws Exception {
        final Set<String> searchableSnapshotsThreadPools = org.elasticsearch.core.Set.of(
            SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME,
            SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME
        );
        assertBusy(() -> {
            final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, "/_nodes/stats/thread_pool"));
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));

            final Map<String, Object> nodes = extractValue(responseAsMap(response), "nodes");
            assertThat(nodes, notNullValue());

            for (String node : nodes.keySet()) {
                final Map<String, Object> threadPools = extractValue((Map<String, Object>) nodes.get(node), "thread_pool");
                searchableSnapshotsThreadPools.forEach(threadPoolName -> {
                    final Map<String, Object> threadPoolStats = (Map<String, Object>) threadPools.get(threadPoolName);
                    assertThat(threadPoolStats, notNullValue());

                    final Number active = extractValue(threadPoolStats, "active");
                    assertThat(threadPoolName + " has still active tasks", active.longValue(), equalTo(0L));

                    final Number queue = extractValue(threadPoolStats, "queue");
                    assertThat(threadPoolName + " has still enqueued tasks", queue.longValue(), equalTo(0L));

                });
            }
        }, 30L, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    protected static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }

    /**
     * The body of a test case, which runs after the searchable snapshot has been created and restored.
     */
    @FunctionalInterface
    interface SearchableSnapshotsTestCaseBody {
        void runTest(String indexName, int numDocs) throws Exception;
    }
}
