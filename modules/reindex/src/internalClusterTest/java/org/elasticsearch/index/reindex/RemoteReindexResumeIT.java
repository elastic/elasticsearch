/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.ResumeInfo.PitWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.ScrollWorkerResumeInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.reindex.AbstractBulkByPaginatedSearchRequest.DEFAULT_SCROLL_TIMEOUT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * Tests remote reindexing tasks resumed after node relocation.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
@SuppressForbidden(reason = "use http server for mock remote cluster in tests")
public class RemoteReindexResumeIT extends ESIntegTestCase {

    /** Base64-encoded PIT id returned by the mock open-PIT response (decodes to {@code "somepitid"}). */
    private static final String MOCK_PIT_ID_BASE64 = "c29tZXBpdGlk";

    /** Scroll id the mock returns on initial search and scroll requests (until exhausted). */
    private static final String MOCK_SCROLL_SESSION_ID = "mock-remote-scroll-session";

    /** Shared client for priming requests to the mock remote. */
    private static final HttpClient MOCK_REMOTE_HTTP_CLIENT = HttpClient.newHttpClient();

    @AfterClass
    public static void closeMockRemoteHttpClient() {
        MOCK_REMOTE_HTTP_CLIENT.close();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MainRestPlugin.class, TestTelemetryPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .build();
    }

    /**
     * Resume remote reindex using scroll when {@link ReindexPlugin#REINDEX_PIT_SEARCH_ENABLED} is off.
     * The remote URL is this cluster's HTTP endpoint (real Elasticsearch), matching the scroll id obtained locally.
     */
    public void testResumeReindexFromScroll() {
        assumeFalse("reindex with point-in-time search must not be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        int totalDocs = randomIntBetween(20, 100);
        int batchSize = randomIntBetween(1, 10);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        // Manually initiate a scroll search to get a scroll ID
        String scrollId;
        int remainingDocs = totalDocs - batchSize;
        SearchResponse searchResponse = client().prepareSearch(sourceIndex).setScroll(DEFAULT_SCROLL_TIMEOUT).setSize(batchSize).get();
        try {
            scrollId = searchResponse.getScrollId();
            assertNotNull(scrollId);
            assertEquals((int) searchResponse.getHits().getTotalHits().value(), totalDocs);
            assertEquals(searchResponse.getHits().getHits().length, batchSize);
        } finally {
            searchResponse.decRef();
        }

        // Resume reindexing from the manual scroll with remote search
        BulkByScrollTask.Status randomStats = randomStats();
        // random start time in the past to ensure that "took" is updated
        long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));
        InetSocketAddress remoteAddress = randomFrom(cluster().httpAddresses());
        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setRequestsPerSecond(randomStats.getRequestsPerSecond())
            .setRemoteInfo(
                new RemoteInfo(
                    "http",
                    remoteAddress.getHostString(),
                    remoteAddress.getPort(),
                    null,
                    new BytesArray("{\"match_all\":{}}"),
                    null,
                    null,
                    Map.of(),
                    RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                    RemoteInfo.DEFAULT_CONNECT_TIMEOUT
                )
            )
            .setResumeInfo(
                new ResumeInfo(randomOrigin(), new ScrollWorkerResumeInfo(scrollId, startTime, randomStats, Version.CURRENT), null)
            );
        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertStatus(getTaskResponse.getTask(), randomStats, totalDocs, batchSize, remainingDocs);
        // ensure remaining docs were indexed
        assertHitCount(prepareSearch(destIndex), remainingDocs);
        // ensure the scroll is cleared
        assertEquals(0, currentNumberOfScrollContexts());
    }

    /**
     * Resume remote reindex using PIT when {@link ReindexPlugin#REINDEX_PIT_SEARCH_ENABLED} is on.
     * A mock HTTP server reports a remote {@link Version} on {@code GET /} that is 7.10 or later so
     * {@link org.elasticsearch.reindex.Reindexer} takes the remote PIT path
     */
    public void testResumeReindexFromPit_mockRemoteVersionSupportsPit() throws Exception {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        final Version reportedRemoteVersion = randomFrom(Version.V_7_10_0, Version.V_8_0_0, Version.V_8_15_0);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        int totalDocs = randomIntBetween(20, 100);
        int batchSize = randomIntBetween(1, 10);

        HttpServer mockRemote = createMockRemoteElasticsearch(reportedRemoteVersion, totalDocs, batchSize);
        mockRemote.start();
        try {
            String host = mockRemote.getAddress().getHostString();
            int port = mockRemote.getAddress().getPort();

            BytesReference pitId = new BytesArray(Base64.getUrlDecoder().decode(MOCK_PIT_ID_BASE64));
            httpPostEmpty(
                host,
                port,
                "/" + sourceIndex + "/_pit?keep_alive=" + DEFAULT_SCROLL_TIMEOUT.getStringRep() + "&allow_partial_search_results=false"
            );

            String firstSearchBody = buildMockRemotePitSearchBody(pitId, batchSize, null);
            String firstSearchResponse = httpPost(host, port, "/_search?allow_partial_search_results=false", firstSearchBody);
            Object[] searchAfterValues = extractSortFromLastHit(firstSearchResponse);
            assertNotNull(searchAfterValues);

            int remainingDocs = totalDocs - batchSize;
            BulkByScrollTask.Status randomStats = randomStats();
            long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));

            ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
                .setShouldStoreResult(true)
                .setEligibleForRelocationOnShutdown(true)
                .setDestIndex(destIndex)
                .setSourceBatchSize(batchSize)
                .setRefresh(true)
                .setRequestsPerSecond(randomStats.getRequestsPerSecond())
                .setRemoteInfo(
                    new RemoteInfo(
                        "http",
                        host,
                        port,
                        null,
                        new BytesArray("{\"match_all\":{}}"),
                        null,
                        null,
                        Map.of(),
                        RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                        RemoteInfo.DEFAULT_CONNECT_TIMEOUT
                    )
                )
                .setResumeInfo(
                    new ResumeInfo(
                        randomOrigin(),
                        new PitWorkerResumeInfo(pitId, searchAfterValues, startTime, randomStats, reportedRemoteVersion),
                        null
                    )
                );
            request.getSearchRequest()
                .source(
                    new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                        .sort(SortBuilders.pitTiebreaker())
                        .size(batchSize)
                );
            request.getSearchRequest().scroll(null);
            request.getSearchRequest().indices(Strings.EMPTY_ARRAY);

            ResumeBulkByScrollResponse resumeResponse = client().execute(
                ResumeReindexAction.INSTANCE,
                new ResumeBulkByScrollRequest(request)
            ).actionGet();
            GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
                .setWaitForCompletion(true)
                .setTimeout(TimeValue.timeValueSeconds(30))
                .get();

            assertStatus(getTaskResponse.getTask(), randomStats, totalDocs, batchSize, remainingDocs);
            assertHitCount(prepareSearch(destIndex), remainingDocs);
            assertEquals(0, currentNumberOfScrollContexts());
        } finally {
            mockRemote.stop(0);
        }
    }

    /**
     * With PIT enabled locally, a remote still reports {@link Version} &lt; 7.10.0 so reindex falls back to scroll against that cluster.
     */
    public void testResumeReindexFromPit_mockRemoteVersionDoesNotSupportPit() throws Exception {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        final Version reportedRemoteVersion = randomFrom(Version.V_7_9_0, Version.V_7_8_0, Version.V_7_0_0);
        assertTrue(reportedRemoteVersion.before(Version.V_7_10_0));

        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        int totalDocs = randomIntBetween(20, 100);
        int batchSize = randomIntBetween(1, 10);

        HttpServer mockRemote = createMockRemoteElasticsearchScroll(reportedRemoteVersion, totalDocs, batchSize);
        mockRemote.start();
        try {
            String host = mockRemote.getAddress().getHostString();
            int port = mockRemote.getAddress().getPort();

            String initialPath = "/"
                + sourceIndex
                + "/_search?scroll="
                + URLEncoder.encode(DEFAULT_SCROLL_TIMEOUT.getStringRep(), StandardCharsets.UTF_8)
                + "&size="
                + batchSize
                + "&version=false&allow_partial_search_results=false";
            String initialSearchBody = "{\"query\":{\"match_all\":{}},\"_source\":true}";
            String initialResponse = httpPost(host, port, initialPath, initialSearchBody);
            String scrollId = extractScrollId(initialResponse);
            assertEquals(MOCK_SCROLL_SESSION_ID, scrollId);

            int remainingDocs = totalDocs - batchSize;
            BulkByScrollTask.Status randomStats = randomStats();
            long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));

            ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
                .setShouldStoreResult(true)
                .setEligibleForRelocationOnShutdown(true)
                .setDestIndex(destIndex)
                .setSourceBatchSize(batchSize)
                .setRefresh(true)
                .setRequestsPerSecond(randomStats.getRequestsPerSecond())
                .setRemoteInfo(
                    new RemoteInfo(
                        "http",
                        host,
                        port,
                        null,
                        new BytesArray("{\"match_all\":{}}"),
                        null,
                        null,
                        Map.of(),
                        RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                        RemoteInfo.DEFAULT_CONNECT_TIMEOUT
                    )
                )
                .setResumeInfo(
                    new ResumeInfo(
                        randomOrigin(),
                        new ScrollWorkerResumeInfo(scrollId, startTime, randomStats, reportedRemoteVersion),
                        null
                    )
                );

            ResumeBulkByScrollResponse resumeResponse = client().execute(
                ResumeReindexAction.INSTANCE,
                new ResumeBulkByScrollRequest(request)
            ).actionGet();
            GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
                .setWaitForCompletion(true)
                .setTimeout(TimeValue.timeValueSeconds(30))
                .get();

            assertStatus(getTaskResponse.getTask(), randomStats, totalDocs, batchSize, remainingDocs);
            assertHitCount(prepareSearch(destIndex), remainingDocs);
            assertEquals(0, currentNumberOfScrollContexts());
        } finally {
            mockRemote.stop(0);
        }
    }

    public void testRejectRemote_sliced() {
        InetSocketAddress remoteAddress = randomFrom(cluster().httpAddresses());
        RemoteInfo remoteInfo = new RemoteInfo(
            "http",
            remoteAddress.getHostString(),
            remoteAddress.getPort(),
            null,
            new BytesArray("{\"match_all\":{}}"),
            null,
            null,
            Map.of(),
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );
        ReindexRequest request = new ReindexRequest().setSourceIndices("source")
            .setDestIndex("dest")
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRemoteInfo(remoteInfo)
            .setSlices(randomIntBetween(2, 10))
            .setResumeInfo(
                new ResumeInfo(randomOrigin(), new ScrollWorkerResumeInfo("scrollId", 0L, randomStats(), Version.CURRENT), null)
            );

        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request)).actionGet()
        );

        assertTrue(e.getMessage().contains("reindex from remote sources doesn't support slices > 1"));
    }

    public void testRejectRemote_slicedManual() {
        InetSocketAddress remoteAddress = randomFrom(cluster().httpAddresses());
        RemoteInfo remoteInfo = new RemoteInfo(
            "http",
            remoteAddress.getHostString(),
            remoteAddress.getPort(),
            null,
            new BytesArray("{\"match_all\":{}}"),
            null,
            null,
            Map.of(),
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );
        ReindexRequest request = new ReindexRequest().setSourceIndices("source")
            .setDestIndex("dest")
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRemoteInfo(remoteInfo)
            .setSlices(1)
            .setResumeInfo(
                new ResumeInfo(randomOrigin(), new ScrollWorkerResumeInfo("scrollId", 0L, randomStats(), Version.CURRENT), null)
            );
        request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, 0, 2)));

        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request)).actionGet()
        );

        assertTrue(e.getMessage().contains("reindex from remote sources doesn't support source.slice"));
    }

    private static HttpServer createMockRemoteElasticsearch(Version reportedVersion, int totalDocs, int batchSize) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        AtomicInteger nextDocId = new AtomicInteger(0);
        String versionJson = "{\"version\":{\"number\":\"" + reportedVersion + "\"},\"tagline\":\"You Know, for Search\"}";
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();
                if ("GET".equals(method) && ("/".equals(path) || path.isEmpty())) {
                    respondJson(exchange, 200, versionJson);
                } else if ("POST".equals(method) && path.contains("/_pit")) {
                    exchange.getRequestBody().readAllBytes();
                    respondJson(exchange, 200, "{\"id\":\"" + MOCK_PIT_ID_BASE64 + "\"}");
                } else if ("POST".equals(method) && path.contains("/_search")) {
                    exchange.getRequestBody().readAllBytes();
                    int remaining = totalDocs - nextDocId.get();
                    if (remaining <= 0) {
                        respondJson(exchange, 200, mockSearchHitsEmptyJson(totalDocs));
                    } else {
                        int n = Math.min(batchSize, remaining);
                        int start = nextDocId.getAndAdd(n);
                        respondJson(exchange, 200, mockSearchHitsJson(totalDocs, start, n));
                    }
                } else if ("DELETE".equals(method) && path.contains("_pit")) {
                    exchange.getRequestBody().readAllBytes();
                    exchange.sendResponseHeaders(200, -1);
                    exchange.close();
                } else {
                    exchange.sendResponseHeaders(404, -1);
                    exchange.close();
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        return server;
    }

    private static HttpServer createMockRemoteElasticsearchScroll(Version reportedVersion, int totalDocs, int batchSize)
        throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        AtomicInteger nextDocId = new AtomicInteger(0);
        String versionJson = "{\"version\":{\"number\":\"" + reportedVersion + "\"},\"tagline\":\"You Know, for Search\"}";
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();
                if ("GET".equals(method) && ("/".equals(path) || path.isEmpty())) {
                    respondJson(exchange, 200, versionJson);
                } else if ("POST".equals(method) && path.contains("/_search/scroll")) {
                    exchange.getRequestBody().readAllBytes();
                    int remaining = totalDocs - nextDocId.get();
                    if (remaining <= 0) {
                        respondJson(exchange, 200, mockScrollSearchHitsEmptyJson(totalDocs));
                    } else {
                        int n = Math.min(batchSize, remaining);
                        int start = nextDocId.getAndAdd(n);
                        respondJson(exchange, 200, mockScrollSearchHitsJson(totalDocs, start, n));
                    }
                } else if ("POST".equals(method) && path.contains("_search")) {
                    exchange.getRequestBody().readAllBytes();
                    int remaining = totalDocs - nextDocId.get();
                    if (remaining <= 0) {
                        respondJson(exchange, 200, mockScrollSearchHitsEmptyJson(totalDocs));
                    } else {
                        int n = Math.min(batchSize, remaining);
                        int start = nextDocId.getAndAdd(n);
                        respondJson(exchange, 200, mockScrollSearchHitsJson(totalDocs, start, n));
                    }
                } else if ("DELETE".equals(method) && path.contains("_search/scroll")) {
                    exchange.getRequestBody().readAllBytes();
                    exchange.sendResponseHeaders(200, -1);
                    exchange.close();
                } else {
                    exchange.sendResponseHeaders(404, -1);
                    exchange.close();
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        return server;
    }

    private static String mockScrollSearchHitsJson(int totalHitsValue, int startDocId, int nHits) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"timed_out\":false,\"_scroll_id\":\"").append(MOCK_SCROLL_SESSION_ID).append("\",");
        sb.append("\"hits\":{\"total\":{\"value\":").append(totalHitsValue).append(",\"relation\":\"eq\"},\"hits\":[");
        for (int i = 0; i < nHits; i++) {
            int docId = startDocId + i;
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"_index\":\"mock\",\"_id\":\"").append(docId).append("\",");
            sb.append("\"_source\":{\"f\":1}}");
        }
        sb.append("]},\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}");
        return sb.toString();
    }

    private static String mockScrollSearchHitsEmptyJson(int totalHitsValue) {
        return "{\"timed_out\":false,\"_scroll_id\":\""
            + MOCK_SCROLL_SESSION_ID
            + "\",\"hits\":{\"total\":{\"value\":"
            + totalHitsValue
            + ",\"relation\":\"eq\"},\"hits\":[]},\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}";
    }

    private static void respondJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
        exchange.close();
    }

    private static String mockSearchHitsJson(int totalHitsValue, int startDocId, int nHits) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"timed_out\":false,\"pit_id\":\"").append(MOCK_PIT_ID_BASE64).append("\",");
        sb.append("\"hits\":{\"total\":{\"value\":").append(totalHitsValue).append(",\"relation\":\"eq\"},\"hits\":[");
        for (int i = 0; i < nHits; i++) {
            int docId = startDocId + i;
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"_index\":\"mock\",\"_id\":\"").append(docId).append("\",");
            sb.append("\"_source\":{\"f\":1},\"sort\":[").append(docId).append("]}");
        }
        sb.append("]},\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}");
        return sb.toString();
    }

    private static String mockSearchHitsEmptyJson(int totalHitsValue) {
        return "{\"timed_out\":false,\"hits\":{\"total\":{\"value\":"
            + totalHitsValue
            + ",\"relation\":\"eq\"},\"hits\":[]},\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}";
    }

    private static String buildMockRemotePitSearchBody(BytesReference pitId, int batchSize, Object[] searchAfter) throws IOException {
        try (XContentBuilder entity = JsonXContent.contentBuilder()) {
            entity.startObject();
            entity.startObject("pit");
            entity.field("id", Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(pitId)));
            entity.field("keep_alive", DEFAULT_SCROLL_TIMEOUT.getStringRep());
            entity.endObject();
            entity.field("size", batchSize);
            entity.field("version", false);
            entity.startArray("sort");
            SortBuilders.pitTiebreaker().toXContent(entity, ToXContent.EMPTY_PARAMS);
            entity.endArray();
            entity.startObject("query");
            entity.startObject("match_all");
            entity.endObject();
            entity.endObject();
            entity.field("_source", true);
            if (searchAfter != null && searchAfter.length > 0) {
                entity.startArray("search_after");
                for (Object o : searchAfter) {
                    if (o instanceof Number n) {
                        entity.value(n.longValue());
                    } else if (o instanceof String s) {
                        entity.value(s);
                    } else if (o == null) {
                        entity.nullValue();
                    } else {
                        throw new IllegalArgumentException("unsupported search_after value: " + o);
                    }
                }
                entity.endArray();
            }
            entity.endObject();
            return Strings.toString(entity);
        }
    }

    private static String httpPost(String host, int port, String pathAndQuery, String body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://" + host + ":" + port + pathAndQuery))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        HttpResponse<String> response = MOCK_REMOTE_HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    private static void httpPostEmpty(String host, int port, String pathAndQuery) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://" + host + ":" + port + pathAndQuery))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
        HttpResponse<String> response = MOCK_REMOTE_HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    @SuppressWarnings("unchecked")
    private static Object[] extractSortFromLastHit(String json) {
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, true);
        Map<String, Object> hits = (Map<String, Object>) map.get("hits");
        assertNotNull(hits);
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        assertNotNull(hitList);
        assertFalse(hitList.isEmpty());
        Map<String, Object> lastHit = hitList.getLast();
        List<Object> sort = (List<Object>) lastHit.get("sort");
        assertNotNull(sort);
        return sort.toArray();
    }

    private static String extractScrollId(String json) {
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, true);
        return (String) map.get("_scroll_id");
    }

    private BulkByScrollTask.Status randomStats() {
        return randomStats(null, randomNonNegativeLong());
    }

    private BulkByScrollTask.Status randomStats(Integer sliceId, long total) {
        return new BulkByScrollTask.Status(
            sliceId,
            total,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomTimeValue(),
            randomFloatBetween(1000, 10000, true),
            null,
            TimeValue.ZERO
        );
    }

    private long currentNumberOfScrollContexts() {
        final NodesStatsResponse stats = clusterAdmin().prepareNodesStats().clear().setIndices(true).get();
        long total = 0;
        for (var nodeStats : stats.getNodes()) {
            total += nodeStats.getIndices().getSearch().getTotal().getScrollCurrent();
        }
        return total;
    }

    private static void assertStatus(
        TaskResult task,
        BulkByScrollTask.Status resumeStatus,
        long totalDocs,
        int batchSize,
        long remainingDocs
    ) {
        assertTrue(task.isCompleted());
        Map<String, Object> response = task.getResponseAsMap();
        assertNotNull(response);

        // total should equal to total hits from the search
        assertEquals(totalDocs, longFromMap(response, "total"));
        // stats are updated
        assertEquals(remainingDocs + resumeStatus.getCreated(), longFromMap(response, "created"));
        long remainingBatches = remainingDocs / batchSize + (remainingDocs % batchSize == 0 ? 0 : 1);
        assertEquals(remainingBatches + resumeStatus.getBatches(), intFromMap(response, "batches"));
        assertTrue(longFromMap(response, "took") > TimeValue.ONE_HOUR.millis());
        // other stats should be retained
        assertEquals(resumeStatus.getDeleted(), longFromMap(response, "deleted"));
        assertEquals(resumeStatus.getUpdated(), longFromMap(response, "updated"));
        assertEquals(resumeStatus.getVersionConflicts(), longFromMap(response, "version_conflicts"));
        assertEquals(resumeStatus.getNoops(), longFromMap(response, "noops"));
        @SuppressWarnings("unchecked")
        Map<String, Object> retries = (Map<String, Object>) response.get("retries");
        assertNotNull(retries);
        assertEquals(resumeStatus.getBulkRetries(), longFromMap(retries, "bulk"));
        assertEquals(resumeStatus.getSearchRetries(), longFromMap(retries, "search"));
        assertEquals(resumeStatus.getRequestsPerSecond(), floatFromMap(response, "requests_per_second"), 0);
    }

    private static long longFromMap(Map<String, Object> map, String key) {
        return ((Number) map.get(key)).longValue();
    }

    private static int intFromMap(Map<String, Object> map, String key) {
        return ((Number) map.get(key)).intValue();
    }

    private static float floatFromMap(Map<String, Object> map, String key) {
        return ((Number) map.get(key)).floatValue();
    }

    private static long timeAgo(TimeValue period) {
        return System.currentTimeMillis() - period.millis();
    }

    private static ResumeInfo.RelocationOrigin randomOrigin() {
        return new ResumeInfo.RelocationOrigin(
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphanumericOfLength(10), randomNonNegativeLong()),
            randomNonNegativeLong()
        );
    }
}
