/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractSearchableSnapshotsRestTestCase extends ESRestTestCase {

    protected abstract String repositoryType();

    protected abstract Settings repositorySettings();

    private void runSearchableSnapshotsTest(SearchableSnapshotsTestCaseBody testCaseBody) throws Exception {
        final String repositoryType = repositoryType();
        final Settings repositorySettings = repositorySettings();

        final String repository = "repository";
        logger.info("creating repository [{}] of type [{}]", repository, repositoryType);
        registerRepository(repository, repositoryType, true, repositorySettings);

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

        final String snapshot = "searchable-snapshot";

        // Remove the snapshots, if a previous test failed to delete them. This is
        // useful for third party tests that runs the test against a real external service.
        deleteSnapshot(repository, snapshot, true);

        logger.info("creating snapshot [{}]", snapshot);
        createSnapshot(repository, snapshot, true);

        logger.info("deleting index [{}]", indexName);
        deleteIndex(indexName);

        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        logger.info("restoring index [{}] from snapshot [{}] as [{}]", indexName, snapshot, restoredIndexName);
        mountSnapshot(repository, snapshot, true, indexName, restoredIndexName, Settings.EMPTY);

        ensureGreen(restoredIndexName);

        final Number count = count(restoredIndexName);
        assertThat("Wrong index count for index " + restoredIndexName, count.intValue(), equalTo(numDocs));

        testCaseBody.runTest(restoredIndexName, numDocs);

        logger.info("deleting snapshot [{}]", snapshot);
        deleteSnapshot(repository, snapshot, false);
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
            assertOK(client().performRequest(freezeRequest));
            ensureGreen(restoredIndexName);
            for (int i = 0; i < 10; i++) {
                assertSearchResults(restoredIndexName, numDocs, Boolean.FALSE);
            }
        });
    }

    public void testCloseAndReopen() throws Exception {
        runSearchableSnapshotsTest((restoredIndexName, numDocs) -> {
            final Request closeRequest = new Request(HttpPost.METHOD_NAME, restoredIndexName + "/_close");
            assertOK(client().performRequest(closeRequest));
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

            final long bytesInCacheBeforeClear = sumCachedBytesWritten.apply(searchableSnapshotStats(restoredIndexName));
            assertThat(bytesInCacheBeforeClear, greaterThan(0L));

            clearCache(restoredIndexName);

            final long bytesInCacheAfterClear = sumCachedBytesWritten.apply(searchableSnapshotStats(restoredIndexName));
            assertThat(bytesInCacheAfterClear, equalTo(bytesInCacheBeforeClear));

            searchResults = search(restoredIndexName, QueryBuilders.matchAllQuery(), Boolean.TRUE);
            assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));

            assertBusy(() -> {
                final long bytesInCacheAfterSearch = sumCachedBytesWritten.apply(searchableSnapshotStats(restoredIndexName));
                assertThat(bytesInCacheAfterSearch, greaterThan(bytesInCacheBeforeClear));
            });
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

    protected static void registerRepository(String repository, String type, boolean verify, Settings settings) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository);
        request.setJsonEntity(Strings.toString(new PutRepositoryRequest(repository).type(type).verify(verify).settings(settings)));

        final Response response = client().performRequest(request);
        assertAcked("Failed to create repository [" + repository + "] of type [" + type + "]: " + response, response);
    }

    protected static void createSnapshot(String repository, String snapshot, boolean waitForCompletion) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        final Response response = client().performRequest(request);
        assertThat(
            "Failed to create snapshot [" + snapshot + "] in repository [" + repository + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    protected static void deleteSnapshot(String repository, String snapshot, boolean ignoreMissing) throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        try {
            final Response response = client().performRequest(request);
            assertAcked("Failed to delete snapshot [" + snapshot + "] in repository [" + repository + "]: " + response, response);
        } catch (IOException e) {
            if (ignoreMissing && e instanceof ResponseException) {
                Response response = ((ResponseException) e).getResponse();
                assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.NOT_FOUND.getStatus()));
                return;
            }
            throw e;
        }
    }

    protected static void mountSnapshot(
        String repository,
        String snapshot,
        boolean waitForCompletion,
        String snapshotIndexName,
        String mountIndexName,
        Settings indexSettings
    ) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, "/_snapshot/" + repository + "/" + snapshot + "/_mount");
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        final XContentBuilder builder = JsonXContent.contentBuilder().startObject().field("index", snapshotIndexName);
        if (snapshotIndexName.equals(mountIndexName) == false || randomBoolean()) {
            builder.field("renamed_index", mountIndexName);
        }
        if (indexSettings.isEmpty() == false) {
            builder.startObject("index_settings");
            indexSettings.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
        builder.endObject();
        request.setJsonEntity(Strings.toString(builder));

        final Response response = client().performRequest(request);
        assertThat(
            "Failed to restore snapshot [" + snapshot + "] in repository [" + repository + "]: " + response,
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
        request.setJsonEntity(new SearchSourceBuilder().trackTotalHits(true).query(query).toString());
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
        final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, '/' + index + "/_searchable_snapshots/stats"));
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

    protected static Map<String, Object> responseAsMap(Response response) throws IOException {
        final XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        assertThat("Unknown XContentType", xContentType, notNullValue());

        BytesReference bytesReference = Streams.readFully(response.getEntity().getContent());

        try (InputStream responseBody = bytesReference.streamInput()) {
            return XContentHelper.convertToMap(xContentType.xContent(), responseBody, true);
        } catch (Exception e) {
            throw new IOException(bytesReference.utf8ToString(), e);
        }
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
