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
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractSearchableSnapshotsRestTestCase extends ESRestTestCase {

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
        runSearchableSnapshotsTest(testCaseBody, sourceOnly, randomIntBetween(1, 500), null);
    }

    private void runSearchableSnapshotsTest(
        final SearchableSnapshotsTestCaseBody testCaseBody,
        final boolean sourceOnly,
        final int numDocs,
        @Nullable Settings indexSettings
    ) throws Exception {
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
        logger.info("creating index [{}]", indexName);
        createIndex(indexName, indexSettings != null ? indexSettings : indexSettings(randomIntBetween(1, 5), 0).build(), """
                "properties": {
                    "field": {
                        "type": "integer"
                    },
                    "text": {
                        "type": "text",
                        "fields": {
                            "raw": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            """);
        ensureGreen(indexName);

        logger.info("indexing [{}] documents", numDocs);
        final int indexingThreads = 2;
        final CountDownLatch indexingLatch = new CountDownLatch(indexingThreads);
        final AtomicLong remainingDocs = new AtomicLong(numDocs);
        for (int i = 0; i < indexingThreads; i++) {
            var thread = new Thread(() -> {
                try {
                    do {
                        final StringBuilder bulkBody = new StringBuilder();
                        int bulkSize = 0;

                        long n;
                        while ((n = remainingDocs.decrementAndGet()) >= 0) {
                            bulkBody.append(Strings.format("""
                                    {"index": {"_id":"%d"} }
                                    {"field": %d, "text": "Document number %d"}
                                """, n, n, n));
                            bulkSize += 1;
                            if (bulkSize >= 500) {
                                break;
                            }
                        }
                        if (bulkSize > 0) {
                            Request documents = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
                            documents.addParameter("refresh", Boolean.TRUE.toString());
                            documents.setJsonEntity(bulkBody.toString());
                            assertOK(client().performRequest(documents));
                        }
                    } while (remainingDocs.get() > 0);
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    indexingLatch.countDown();
                }
            });
            thread.start();
        }
        indexingLatch.await();

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

                // takes a snapshot of the searchable snapshot index into the source-only repository should fail
                String sourceOnlySnapshot = "source-only-snap-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + WRITE_REPOSITORY_NAME + '/' + sourceOnlySnapshot);
                request.addParameter("wait_for_completion", "true");
                request.setJsonEntity(Strings.format("""
                    {
                        "include_global_state": false,
                        "indices" : "%s"
                    }
                    """, indexName));

                final Response response = adminClient().performRequest(request);
                assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));

                final List<Map<String, Object>> failures = extractValue(responseAsMap(response), "snapshot.failures");
                assertThat(failures, notNullValue());
                assertThat(failures.size(), greaterThan(0));
                for (Map<String, Object> failure : failures) {
                    assertThat(extractValue(failure, "status"), equalTo(RestStatus.INTERNAL_SERVER_ERROR.toString()));
                    assertThat(
                        extractValue(failure, "reason"),
                        allOf(
                            containsString("is not a regular index"),
                            containsString("cannot be snapshotted into a source-only repository")
                        )
                    );
                }
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
            assertThat("Searchable snapshot cache wasn't cleared", bytesInCacheAfterClear, equalTo(bytesInCacheBeforeClear));

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

            if (randomBoolean()) {
                logger.info("--> closing index [{}]", restoredIndexName);
                final Request closeRequest = new Request(HttpPost.METHOD_NAME, restoredIndexName + "/_close");
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
                assertThat(extractValue(value, "stats.total.file_count"), equalTo(0));
                assertThat(extractValue(value, "stats.incremental.file_count"), equalTo(0));
            }

            deleteIndex(restoredIndexName);

            restoreSnapshot(WRITE_REPOSITORY_NAME, snapshot2Name, true);
            ensureGreen(restoredIndexName);

            deleteSnapshot(snapshot2Name, false);

            assertSearchResults(restoredIndexName, numDocs, randomFrom(Boolean.TRUE, Boolean.FALSE, null));
        });
    }

    public void testQueryScript() throws Exception {
        runSearchableSnapshotsTest((indexName, numDocs) -> {
            final int nbThreads = 5;

            final CyclicBarrier barrier = new CyclicBarrier(nbThreads);
            final AtomicBoolean maybeStop = new AtomicBoolean(false);
            final CountDownLatch done = new CountDownLatch(nbThreads);

            logger.info("--> starting concurrent search queries");
            for (int threadId = 0; threadId < nbThreads; threadId++) {
                int finalThreadId = threadId;
                Thread thread = new Thread(() -> {
                    try {
                        for (int runs = 0; runs < 10; runs++) {
                            if (maybeStop.get()) {
                                return;
                            }
                            barrier.await(30L, TimeUnit.SECONDS);

                            if (finalThreadId == 0) {
                                // we want the cache to be empty so that cached data will have to be fetched
                                clearCache(indexName);
                            }

                            barrier.await(30L, TimeUnit.SECONDS);
                            if (maybeStop.get()) {
                                return;
                            }

                            // we want the thread pools to have no active workers when the first script query will be cached in query cache
                            waitForIdlingSearchableSnapshotsThreadPools();

                            barrier.await(30L, TimeUnit.SECONDS);
                            if (maybeStop.get()) {
                                return;
                            }

                            Request searchRequest = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_search");
                            searchRequest.addParameter("search_type", "query_then_fetch");
                            searchRequest.setJsonEntity(
                                new SearchSourceBuilder().trackTotalHits(true)
                                    .query(
                                        QueryBuilders.scriptQuery(
                                            new Script(
                                                ScriptType.INLINE,
                                                Script.DEFAULT_SCRIPT_LANG,
                                                "doc['text.raw'].value.toString().length() > 0",
                                                Collections.emptyMap()
                                            )
                                        )
                                    )
                                    .toString()
                            );

                            Response searchResponse = client().performRequest(searchRequest);
                            assertThat(extractValue(responseAsMap(searchResponse), "hits.total.value"), equalTo(numDocs));
                            assertOK(searchResponse);
                        }
                    } catch (Exception e) {
                        maybeStop.set(true);
                        throw new AssertionError(e);
                    } finally {
                        done.countDown();
                    }
                });
                thread.start();
            }

            logger.info("--> waiting for searches to complete");
            done.await();
        },
            false,
            10_000,
            indexSettings(1, 0).put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), true)
                .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build()
        );
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
            case 0 -> {
                searchResults = search(indexName, QueryBuilders.termQuery("field", String.valueOf(randomTieBreaker)), ignoreThrottled);
                assertThat(extractValue(searchResults, "hits.total.value"), equalTo(1));
                @SuppressWarnings("unchecked")
                Map<String, Object> searchHit = (Map<String, Object>) ((List<?>) extractValue(searchResults, "hits.hits")).get(0);
                assertThat(extractValue(searchHit, "_index"), equalTo(indexName));
                assertThat(extractValue(searchHit, "_source.field"), equalTo(randomTieBreaker));
            }
            case 1 -> {
                searchResults = search(indexName, QueryBuilders.rangeQuery("field").lt(randomTieBreaker), ignoreThrottled);
                assertThat(extractValue(searchResults, "hits.total.value"), equalTo(randomTieBreaker));
            }
            case 2 -> {
                searchResults = search(indexName, QueryBuilders.rangeQuery("field").gte(randomTieBreaker), ignoreThrottled);
                assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs - randomTieBreaker));
            }
            case 3 -> {
                searchResults = search(indexName, QueryBuilders.matchQuery("text", "document"), ignoreThrottled);
                assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));
            }
            default -> fail("Unsupported randomized search query");
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
        request.setJsonEntity(new SearchSourceBuilder().trackTotalHits(true).query(query).toString());

        // If warning are returned than these must exist in this set:
        Set<String> expectedWarnings = new HashSet<>();
        expectedWarnings.add(TransportSearchAction.FROZEN_INDICES_DEPRECATION_MESSAGE.replace("{}", index));
        if (ignoreThrottled != null) {
            request.addParameter("ignore_throttled", ignoreThrottled.toString());
            expectedWarnings.add(
                "[ignore_throttled] parameter is deprecated because frozen indices have been deprecated. "
                    + "Consider cold or frozen tiers in place of frozen indices."
            );
        }

        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> {
            for (String warning : warnings) {
                if (expectedWarnings.contains(warning) == false) {
                    return true;
                }
            }
            return false;
        }).build();
        request.setOptions(requestOptions);

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
        final Set<String> searchableSnapshotsThreadPools = Set.of(
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
