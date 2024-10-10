/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fieldcaps;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.index.shard.IndexShardTestCase.closeShardNoCheck;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FieldCapabilitiesIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        XContentBuilder oldIndexMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("distance")
            .field("type", "double")
            .endObject()
            .startObject("route_length_miles")
            .field("type", "alias")
            .field("path", "distance")
            .endObject()
            .startObject("playlist")
            .field("type", "text")
            .field("store", true)
            .endObject()
            .startObject("some_dimension")
            .field("type", "keyword")
            .field("time_series_dimension", true)
            .endObject()
            .startObject("some_metric")
            .field("type", "long")
            .field("time_series_metric", TimeSeriesParams.MetricType.COUNTER)
            .endObject()
            .startObject("secret_soundtrack")
            .field("type", "alias")
            .field("path", "playlist")
            .endObject()
            .startObject("old_field")
            .field("type", "long")
            .endObject()
            .startObject("new_field")
            .field("type", "alias")
            .field("path", "old_field")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("some_dimension"))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2006-01-08T23:40:53.384Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z")
            .build();
        assertAcked(prepareCreate("old_index").setSettings(settings).setMapping(oldIndexMapping));

        XContentBuilder newIndexMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("distance")
            .field("type", "text")
            .endObject()
            .startObject("route_length_miles")
            .field("type", "double")
            .endObject()
            .startObject("new_field")
            .field("type", "long")
            .endObject()
            .startObject("some_dimension")
            .field("type", "keyword")
            .endObject()
            .startObject("some_metric")
            .field("type", "long")
            .field("time_series_metric", TimeSeriesParams.MetricType.GAUGE)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("new_index").setMapping(newIndexMapping));
        assertAcked(indicesAdmin().prepareAliases().addAlias("new_index", "current"));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestMapperPlugin.class, ExceptionOnRewriteQueryPlugin.class, BlockingOnRewriteQueryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testFieldAlias() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("distance", "route_length_miles").get();

        assertIndices(response, "old_index", "new_index");
        // Ensure the response has entries for both requested fields.
        assertTrue(response.get().containsKey("distance"));
        assertTrue(response.get().containsKey("route_length_miles"));

        // Check the capabilities for the 'distance' field.
        Map<String, FieldCapabilities> distance = response.getField("distance");
        assertEquals(2, distance.size());

        assertTrue(distance.containsKey("double"));
        assertEquals(
            new FieldCapabilities(
                "distance",
                "double",
                false,
                true,
                true,
                new String[] { "old_index" },
                null,
                null,
                Collections.emptyMap()
            ),
            distance.get("double")
        );

        assertTrue(distance.containsKey("text"));
        assertEquals(
            new FieldCapabilities("distance", "text", false, true, false, new String[] { "new_index" }, null, null, Collections.emptyMap()),
            distance.get("text")
        );

        // Check the capabilities for the 'route_length_miles' alias.
        Map<String, FieldCapabilities> routeLength = response.getField("route_length_miles");
        assertEquals(1, routeLength.size());

        assertTrue(routeLength.containsKey("double"));
        assertEquals(
            new FieldCapabilities("route_length_miles", "double", false, true, true, null, null, null, Collections.emptyMap()),
            routeLength.get("double")
        );
    }

    public void testFieldAliasWithWildcard() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("route*").get();

        assertIndices(response, "old_index", "new_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("route_length_miles"));
    }

    public void testFieldAliasFiltering() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("secret-soundtrack", "route_length_miles").get();
        assertIndices(response, "old_index", "new_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("route_length_miles"));
    }

    public void testFieldAliasFilteringWithWildcard() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("distance", "secret*").get();
        assertIndices(response, "old_index", "new_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("distance"));
    }

    public void testWithUnmapped() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("new_field", "old_field").setIncludeUnmapped(true).get();
        assertIndices(response, "old_index", "new_index");

        assertEquals(2, response.get().size());
        assertTrue(response.get().containsKey("old_field"));

        Map<String, FieldCapabilities> oldField = response.getField("old_field");
        assertEquals(2, oldField.size());

        assertTrue(oldField.containsKey("long"));
        assertEquals(
            new FieldCapabilities("old_field", "long", false, true, true, new String[] { "old_index" }, null, null, Collections.emptyMap()),
            oldField.get("long")
        );

        assertTrue(oldField.containsKey("unmapped"));
        assertEquals(
            new FieldCapabilities(
                "old_field",
                "unmapped",
                false,
                false,
                false,
                new String[] { "new_index" },
                null,
                null,
                Collections.emptyMap()
            ),
            oldField.get("unmapped")
        );

        Map<String, FieldCapabilities> newField = response.getField("new_field");
        assertEquals(1, newField.size());

        assertTrue(newField.containsKey("long"));
        assertEquals(
            new FieldCapabilities("new_field", "long", false, true, true, null, null, null, Collections.emptyMap()),
            newField.get("long")
        );
    }

    public void testWithIndexAlias() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps("current").setFields("*").get();
        assertIndices(response, "new_index");

        FieldCapabilitiesResponse response1 = client().prepareFieldCaps("current", "old_index").setFields("*").get();
        assertIndices(response1, "old_index", "new_index");
        FieldCapabilitiesResponse response2 = client().prepareFieldCaps("current", "old_index", "new_index").setFields("*").get();
        assertEquals(response1, response2);
    }

    public void testWithIndexFilter() throws InterruptedException {
        assertAcked(prepareCreate("index-1").setMapping("timestamp", "type=date", "field1", "type=keyword"));
        assertAcked(prepareCreate("index-2").setMapping("timestamp", "type=date", "field1", "type=long"));

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex("index-1").setSource("timestamp", "2015-07-08"));
        reqs.add(prepareIndex("index-1").setSource("timestamp", "2018-07-08"));
        reqs.add(prepareIndex("index-2").setSource("timestamp", "2019-10-12"));
        reqs.add(prepareIndex("index-2").setSource("timestamp", "2020-07-08"));
        indexRandom(true, reqs);

        FieldCapabilitiesResponse response = client().prepareFieldCaps("index-*").setFields("*").get();
        assertIndices(response, "index-1", "index-2");
        Map<String, FieldCapabilities> newField = response.getField("field1");
        assertEquals(2, newField.size());
        assertTrue(newField.containsKey("long"));
        assertTrue(newField.containsKey("keyword"));

        response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").gte("2019-11-01"))
            .get();
        assertIndices(response, "index-2");
        newField = response.getField("field1");
        assertEquals(1, newField.size());
        assertTrue(newField.containsKey("long"));

        response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").lte("2017-01-01"))
            .get();
        assertIndices(response, "index-1");
        newField = response.getField("field1");
        assertEquals(1, newField.size());
        assertTrue(newField.containsKey("keyword"));
    }

    public void testMetadataFields() {
        for (int i = 0; i < 2; i++) {
            String[] fields = i == 0 ? new String[] { "*" } : new String[] { "_id", "_test" };
            FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields(fields).get();

            Map<String, FieldCapabilities> idField = response.getField("_id");
            assertEquals(1, idField.size());

            assertTrue(idField.containsKey("_id"));
            assertEquals(
                new FieldCapabilities("_id", "_id", true, true, false, null, null, null, Collections.emptyMap()),
                idField.get("_id")
            );

            Map<String, FieldCapabilities> testField = response.getField("_test");
            assertEquals(1, testField.size());

            assertTrue(testField.containsKey("keyword"));
            assertEquals(
                new FieldCapabilities("_test", "keyword", true, true, true, null, null, null, Collections.emptyMap()),
                testField.get("keyword")
            );
        }
    }

    public void testWithRunntimeMappings() throws InterruptedException {
        Map<String, Object> runtimeFields = new HashMap<>();
        runtimeFields.put("day_of_week", Collections.singletonMap("type", "keyword"));
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setRuntimeFields(runtimeFields).get();
        Map<String, FieldCapabilities> runtimeField = response.getField("day_of_week");
        assertNotNull(runtimeField);
        assertEquals("day_of_week", runtimeField.get("keyword").getName());
        assertEquals("keyword", runtimeField.get("keyword").getType());
        assertTrue(runtimeField.get("keyword").isSearchable());
        assertTrue(runtimeField.get("keyword").isAggregatable());
    }

    public void testFieldMetricsAndDimensions() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps("old_index").setFields("some_dimension", "some_metric").get();
        assertIndices(response, "old_index");
        assertEquals(2, response.get().size());
        assertTrue(response.get().containsKey("some_dimension"));
        assertTrue(response.get().get("some_dimension").get("keyword").isDimension());
        assertNull(response.get().get("some_dimension").get("keyword").nonDimensionIndices());
        assertTrue(response.get().containsKey("some_metric"));
        assertEquals(TimeSeriesParams.MetricType.COUNTER, response.get().get("some_metric").get("long").getMetricType());
        assertNull(response.get().get("some_metric").get("long").metricConflictsIndices());

        response = client().prepareFieldCaps("old_index", "new_index").setFields("some_dimension", "some_metric").get();
        assertIndices(response, "old_index", "new_index");
        assertEquals(2, response.get().size());
        assertTrue(response.get().containsKey("some_dimension"));
        assertFalse(response.get().get("some_dimension").get("keyword").isDimension());
        assertThat(response.get().get("some_dimension").get("keyword").nonDimensionIndices(), array(equalTo("new_index")));
    }

    public void testFailures() throws InterruptedException {
        // in addition to the existing "old_index" and "new_index", create two where the test query throws an error on rewrite
        assertAcked(prepareCreate("index1-error"));
        assertAcked(prepareCreate("index2-error"));
        ensureGreen("index1-error", "index2-error");
        FieldCapabilitiesResponse response = client().prepareFieldCaps()
            .setFields("*")
            .setIndexFilter(new ExceptionOnRewriteQueryBuilder())
            .get();
        assertEquals(1, response.getFailures().size());
        assertEquals(2, response.getFailedIndicesCount());
        assertThat(response.getFailures().get(0).getIndices(), arrayContainingInAnyOrder("index1-error", "index2-error"));
        Exception failure = response.getFailures().get(0).getException();
        assertEquals(IllegalArgumentException.class, failure.getClass());
        assertEquals("I throw because I choose to.", failure.getMessage());

        // the "indices" section should not include failed ones
        assertThat(Arrays.asList(response.getIndices()), containsInAnyOrder("old_index", "new_index"));

        // if all requested indices failed, we fail the request by throwing the exception
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            client().prepareFieldCaps("index1-error", "index2-error").setFields("*").setIndexFilter(new ExceptionOnRewriteQueryBuilder())
        );
        assertEquals("I throw because I choose to.", ex.getMessage());
    }

    private void populateTimeRangeIndices() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(
            prepareCreate("log-index-1").setSettings(indexSettings(between(1, 5), 1))
                .setMapping("timestamp", "type=date", "field1", "type=keyword")
        );
        assertAcked(
            prepareCreate("log-index-2").setSettings(indexSettings(between(1, 5), 1))
                .setMapping("timestamp", "type=date", "field1", "type=long")
        );
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex("log-index-1").setSource("timestamp", "2015-07-08"));
        reqs.add(prepareIndex("log-index-1").setSource("timestamp", "2018-07-08"));
        reqs.add(prepareIndex("log-index-1").setSource("timestamp", "2020-03-03"));
        reqs.add(prepareIndex("log-index-1").setSource("timestamp", "2020-09-09"));
        reqs.add(prepareIndex("log-index-2").setSource("timestamp", "2019-10-12"));
        reqs.add(prepareIndex("log-index-2").setSource("timestamp", "2020-02-02"));
        reqs.add(prepareIndex("log-index-2").setSource("timestamp", "2020-10-10"));
        indexRandom(true, reqs);
        ensureGreen("log-index-1", "log-index-2");
        indicesAdmin().prepareRefresh("log-index-1", "log-index-2").get();
    }

    public void testTargetNodeFails() throws Exception {
        populateTimeRangeIndices();
        try {
            final AtomicBoolean failedRequest = new AtomicBoolean();
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node)
                    .addRequestHandlingBehavior(TransportFieldCapabilitiesAction.ACTION_NODE_NAME, (handler, request, channel, task) -> {
                        if (failedRequest.compareAndSet(false, true)) {
                            channel.sendResponse(new CircuitBreakingException("Simulated", CircuitBreaker.Durability.TRANSIENT));
                        } else {
                            handler.messageReceived(request, channel, task);
                        }
                    });
            }
            FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
            request.indices("log-index-*");
            request.fields("*");
            if (randomBoolean()) {
                request.indexFilter(QueryBuilders.rangeQuery("timestamp").gte("2020-01-01"));
            }
            final FieldCapabilitiesResponse response = client().execute(TransportFieldCapabilitiesAction.TYPE, request).actionGet();
            assertTrue(failedRequest.get());
            assertThat(response.getIndices(), arrayContainingInAnyOrder("log-index-1", "log-index-2"));
            assertThat(response.getField("field1"), aMapWithSize(2));
            assertThat(response.getField("field1"), hasKey("long"));
            assertThat(response.getField("field1"), hasKey("keyword"));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
    }

    public void testNoActiveCopy() throws Exception {
        assertAcked(
            prepareCreate("log-index-inactive").setSettings(
                indexSettings(between(1, 5), 1).put("index.routing.allocation.require._id", "unknown")
            ).setWaitForActiveShards(ActiveShardCount.NONE).setMapping("timestamp", "type=date", "field1", "type=keyword")
        );
        {
            final ElasticsearchException ex = expectThrows(
                ElasticsearchException.class,
                client().prepareFieldCaps("log-index-*").setFields("*")
            );
            assertThat(ex.getMessage(), equalTo("index [log-index-inactive] has no active shard copy"));
        }
        {
            populateTimeRangeIndices();
            FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
            request.indices("log-index-*");
            request.fields("*");
            if (randomBoolean()) {
                request.indexFilter(QueryBuilders.rangeQuery("timestamp").gte("2020-01-01"));
            }
            final FieldCapabilitiesResponse response = client().execute(TransportFieldCapabilitiesAction.TYPE, request).actionGet();
            assertThat(response.getIndices(), arrayContainingInAnyOrder("log-index-1", "log-index-2"));
            assertThat(response.getField("field1"), aMapWithSize(2));
            assertThat(response.getField("field1"), hasKey("long"));
            assertThat(response.getField("field1"), hasKey("long"));

            assertThat(response.getFailures(), hasSize(1));
            final FieldCapabilitiesFailure failure = response.getFailures().get(0);
            assertThat(failure.getIndices(), arrayContainingInAnyOrder("log-index-inactive"));
            assertThat(failure.getException().getMessage(), equalTo("index [log-index-inactive] has no active shard copy"));
        }
    }

    private void moveOrCloseShardsOnNodes(String nodeName) throws Exception {
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                if (randomBoolean()) {
                    closeShardNoCheck(indexShard, randomBoolean());
                } else if (randomBoolean()) {
                    final ShardId shardId = indexShard.shardId();

                    final var targetNodes = new ArrayList<String>();
                    for (final var targetIndicesService : internalCluster().getInstances(IndicesService.class)) {
                        final var targetNode = targetIndicesService.clusterService().localNode();
                        if (targetNode.canContainData() && targetIndicesService.getShardOrNull(shardId) == null) {
                            targetNodes.add(targetNode.getId());
                        }
                    }

                    if (targetNodes.isEmpty()) {
                        continue;
                    }
                    ClusterRerouteUtils.reroute(
                        client(),
                        new MoveAllocationCommand(
                            shardId.getIndexName(),
                            shardId.id(),
                            indicesService.clusterService().localNode().getId(),
                            randomFrom(targetNodes)
                        )
                    );
                }
            }
        }
    }

    public void testRelocation() throws Exception {
        populateTimeRangeIndices();
        try {
            final AtomicBoolean relocated = new AtomicBoolean();
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node)
                    .addRequestHandlingBehavior(TransportFieldCapabilitiesAction.ACTION_NODE_NAME, (handler, request, channel, task) -> {
                        if (relocated.compareAndSet(false, true)) {
                            moveOrCloseShardsOnNodes(node);
                        }
                        handler.messageReceived(request, channel, task);
                    });
            }
            FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
            request.indices("log-index-*");
            request.fields("*");
            if (randomBoolean()) {
                request.indexFilter(QueryBuilders.rangeQuery("timestamp").gte("2020-01-01"));
            }
            final FieldCapabilitiesResponse response = safeGet(client().execute(TransportFieldCapabilitiesAction.TYPE, request));
            assertThat(response.getIndices(), arrayContainingInAnyOrder("log-index-1", "log-index-2"));
            assertThat(response.getField("field1"), aMapWithSize(2));
            assertThat(response.getField("field1"), hasKey("long"));
            assertThat(response.getField("field1"), hasKey("long"));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
    }

    public void testManyIndicesWithSameMapping() {
        final String mapping = """
             {
                 "properties": {
                   "message_field": { "type": "text" },
                   "value_field": { "type": "long" },
                   "multi_field" : { "type" : "ip", "fields" : { "keyword" : { "type" : "keyword" } } },
                   "timestamp": {"type": "date"}
                 }
             }
            """;
        String[] indices = IntStream.range(0, between(1, 9)).mapToObj(n -> "test_many_index_" + n).toArray(String[]::new);
        for (String index : indices) {
            assertAcked(indicesAdmin().prepareCreate(index).setMapping(mapping).get());
        }
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indices("test_many_index_*");
        request.fields("*");
        boolean excludeMultiField = randomBoolean();
        if (excludeMultiField) {
            request.filters("-multifield");
        }
        Consumer<FieldCapabilitiesResponse> verifyResponse = resp -> {
            assertThat(resp.getIndices(), equalTo(indices));
            assertThat(resp.getField("message_field"), hasKey("text"));
            assertThat(resp.getField("message_field").get("text").indices(), nullValue());
            assertTrue(resp.getField("message_field").get("text").isSearchable());
            assertFalse(resp.getField("message_field").get("text").isAggregatable());

            assertThat(resp.getField("value_field"), hasKey("long"));
            assertThat(resp.getField("value_field").get("long").indices(), nullValue());
            assertTrue(resp.getField("value_field").get("long").isSearchable());
            assertTrue(resp.getField("value_field").get("long").isAggregatable());

            assertThat(resp.getField("timestamp"), hasKey("date"));

            assertThat(resp.getField("multi_field"), hasKey("ip"));
            if (excludeMultiField) {
                assertThat(resp.getField("multi_field.keyword"), not(hasKey("keyword")));
            } else {
                assertThat(resp.getField("multi_field.keyword"), hasKey("keyword"));
            }
        };
        // Single mapping
        verifyResponse.accept(client().execute(TransportFieldCapabilitiesAction.TYPE, request).actionGet());

        // add an extra field for some indices
        String[] indicesWithExtraField = randomSubsetOf(between(1, indices.length), indices).stream().sorted().toArray(String[]::new);
        ensureGreen(indices);
        assertAcked(indicesAdmin().preparePutMapping(indicesWithExtraField).setSource("extra_field", "type=integer").get());
        for (String index : indicesWithExtraField) {
            prepareIndex(index).setSource("extra_field", randomIntBetween(1, 1000)).get();
        }
        FieldCapabilitiesResponse resp = client().execute(TransportFieldCapabilitiesAction.TYPE, request).actionGet();
        verifyResponse.accept(resp);
        assertThat(resp.getField("extra_field"), hasKey("integer"));
        assertThat(resp.getField("extra_field").get("integer").indices(), nullValue());
        assertTrue(resp.getField("extra_field").get("integer").isSearchable());
        assertTrue(resp.getField("extra_field").get("integer").isAggregatable());
    }

    @TestLogging(
        value = "org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction:TRACE",
        reason = "verify the log output on cancelled"
    )
    public void testCancel() throws Exception {
        try (var mockLog = MockLog.capture(TransportFieldCapabilitiesAction.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "clear resources",
                    TransportFieldCapabilitiesAction.class.getCanonicalName(),
                    Level.TRACE,
                    "clear index responses on cancellation"
                )
            );
            BlockingOnRewriteQueryBuilder.blockOnRewrite();
            PlainActionFuture<Response> future = new PlainActionFuture<>();
            Request restRequest = new Request("POST", "/_field_caps?fields=*");
            restRequest.setEntity(new StringEntity("""
                      {
                        "index_filter": {
                            "blocking_query": {}
                         }
                      }
                """, ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8)));
            Cancellable cancellable = getRestClient().performRequestAsync(restRequest, wrapAsRestResponseListener(future));
            logger.info("--> waiting for field-caps tasks to be started");
            assertBusy(() -> {
                List<TaskInfo> tasks = clusterAdmin().prepareListTasks()
                    .setActions("indices:data/read/field_caps", "indices:data/read/field_caps[n]")
                    .get()
                    .getTasks();
                assertThat(tasks.size(), greaterThanOrEqualTo(2));
                for (TaskInfo task : tasks) {
                    assertTrue(task.cancellable());
                    assertFalse(task.cancelled());
                }
            }, 30, TimeUnit.SECONDS);
            cancellable.cancel();
            mockLog.awaitAllExpectationsMatched();
            logger.info("--> waiting for field-caps tasks to be cancelled");
            assertBusy(() -> {
                List<TaskInfo> tasks = clusterAdmin().prepareListTasks()
                    .setActions("indices:data/read/field_caps", "indices:data/read/field_caps[n]")
                    .get()
                    .getTasks();
                for (TaskInfo task : tasks) {
                    assertTrue(task.cancellable());
                    assertTrue(task.cancelled());
                }
            }, 30, TimeUnit.SECONDS);
            BlockingOnRewriteQueryBuilder.unblockOnRewrite();
            expectThrows(CancellationException.class, future::actionGet);
        }
    }

    public void testIndexMode() throws Exception {
        Map<String, IndexMode> indexModes = new HashMap<>();
        // metrics
        {
            final String metricsMapping = """
                 {
                     "properties": {
                       "@timestamp": { "type": "date" },
                       "hostname": { "type": "keyword", "time_series_dimension": true },
                       "request_count" : { "type" : "long", "time_series_metric" : "counter" },
                       "cluster": {"type": "keyword"}
                     }
                 }
                """;
            Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("hostname")).build();
            int numIndices = between(1, 5);
            for (int i = 0; i < numIndices; i++) {
                assertAcked(indicesAdmin().prepareCreate("test_metrics_" + i).setSettings(settings).setMapping(metricsMapping).get());
                indexModes.put("test_metrics_" + i, IndexMode.TIME_SERIES);
                assertAcked(indicesAdmin().prepareCreate("test_old_metrics_" + i).setMapping(metricsMapping).get());
                indexModes.put("test_old_metrics_" + i, IndexMode.STANDARD);
            }
        }
        // logsdb
        {
            final String logsMapping = """
                 {
                     "properties": {
                       "@timestamp": { "type": "date" },
                       "hostname": { "type": "keyword"},
                       "request_count" : { "type" : "long"},
                       "cluster": {"type": "keyword"}
                     }
                 }
                """;
            Settings settings = Settings.builder().put("mode", "logsdb").build();
            int numIndices = between(1, 5);
            for (int i = 0; i < numIndices; i++) {
                assertAcked(indicesAdmin().prepareCreate("test_logs_" + i).setSettings(settings).setMapping(logsMapping).get());
                indexModes.put("test_logs_" + i, IndexMode.LOGSDB);
                assertAcked(indicesAdmin().prepareCreate("test_old_logs_" + i).setMapping(logsMapping).get());
                indexModes.put("test_old_logs_" + i, IndexMode.STANDARD);
            }
        }
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.setMergeResults(false);
        request.indices("test_*");
        request.fields(randomFrom("*", "@timestamp", "host*"));
        var resp = client().fieldCaps(request).get();
        assertThat(resp.getFailures(), empty());
        Map<String, IndexMode> actualIndexModes = new HashMap<>();
        for (var indexResp : resp.getIndexResponses()) {
            actualIndexModes.put(indexResp.getIndexName(), indexResp.getIndexMode());
        }
        assertThat(actualIndexModes, equalTo(indexModes));
    }

    private void assertIndices(FieldCapabilitiesResponse response, String... indices) {
        assertNotNull(response.getIndices());
        Arrays.sort(indices);
        Arrays.sort(response.getIndices());
        assertArrayEquals(indices, response.getIndices());
    }

    /**
     * Adds an "exception" query that  throws on rewrite if the index name contains the string "error"
     */
    public static class ExceptionOnRewriteQueryPlugin extends Plugin implements SearchPlugin {

        public ExceptionOnRewriteQueryPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            return singletonList(
                new QuerySpec<>("exception", ExceptionOnRewriteQueryBuilder::new, p -> new ExceptionOnRewriteQueryBuilder())
            );
        }
    }

    static class ExceptionOnRewriteQueryBuilder extends DummyQueryBuilder {

        public static final String NAME = "exception";

        ExceptionOnRewriteQueryBuilder() {}

        ExceptionOnRewriteQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            SearchExecutionContext searchExecutionContext = queryRewriteContext.convertToSearchExecutionContext();
            if (searchExecutionContext != null) {
                if (searchExecutionContext.indexMatches("*error*")) {
                    throw new IllegalArgumentException("I throw because I choose to.");
                }
            }
            return this;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public static class BlockingOnRewriteQueryPlugin extends Plugin implements SearchPlugin {

        public BlockingOnRewriteQueryPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            return List.of(
                new QuerySpec<>("blocking_query", BlockingOnRewriteQueryBuilder::new, BlockingOnRewriteQueryBuilder::fromXContent)
            );
        }
    }

    static class BlockingOnRewriteQueryBuilder extends DummyQueryBuilder {
        private static CountDownLatch blockingLatch = new CountDownLatch(1);
        public static final String NAME = "blocking_query";

        BlockingOnRewriteQueryBuilder() {

        }

        BlockingOnRewriteQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        static void blockOnRewrite() {
            blockingLatch = new CountDownLatch(1);
        }

        static void unblockOnRewrite() {
            blockingLatch.countDown();
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            try {
                blockingLatch.await();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            return this;
        }

        public static BlockingOnRewriteQueryBuilder fromXContent(XContentParser parser) {
            ObjectParser<BlockingOnRewriteQueryBuilder, Void> objectParser = new ObjectParser<>(NAME, BlockingOnRewriteQueryBuilder::new);
            declareStandardFields(objectParser);
            return objectParser.apply(parser, null);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public static final class TestMapperPlugin extends Plugin implements MapperPlugin {
        @Override
        public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
            return Collections.singletonMap(TestMetadataMapper.CONTENT_TYPE, TestMetadataMapper.PARSER);
        }

        @Override
        public Function<String, FieldPredicate> getFieldFilter() {
            return index -> new FieldPredicate() {
                @Override
                public boolean test(String field) {
                    return field.equals("playlist") == false;
                }

                @Override
                public String modifyHash(String hash) {
                    return "not-playlist:" + hash;
                }

                @Override
                public long ramBytesUsed() {
                    return 0;
                }
            };
        }
    }

    private static final class TestMetadataMapper extends MetadataFieldMapper {
        private static final String CONTENT_TYPE = "_test";
        private static final String FIELD_NAME = "_test";

        protected TestMetadataMapper() {
            super(new KeywordFieldMapper.KeywordFieldType(FIELD_NAME));
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) throws IOException {}

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }

        private static final TypeParser PARSER = new FixedTypeParser(c -> new TestMetadataMapper());
    }
}
