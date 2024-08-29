/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.engine.frozen;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.DateFieldRangeInfo;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.frozen.FrozenIndices;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class FrozenIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FrozenIndices.class, LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testTimestampRangeRecalculatedOnStalePrimaryAllocation() throws IOException {
        final List<String> nodeNames = internalCluster().startNodes(2);

        createIndex("index", 1, 1);

        String timestampVal = "2010-01-06T02:03:04.567Z";
        String eventIngestedVal = "2010-01-06T02:03:05.567Z";  // one second later

        final DocWriteResponse indexResponse = prepareIndex("index").setSource(
            DataStream.TIMESTAMP_FIELD_NAME,
            timestampVal,
            IndexMetadata.EVENT_INGESTED_FIELD_NAME,
            eventIngestedVal
        ).get();

        ensureGreen("index");

        assertThat(indicesAdmin().prepareFlush("index").get().getSuccessfulShards(), equalTo(2));
        assertThat(indicesAdmin().prepareRefresh("index").get().getSuccessfulShards(), equalTo(2));

        final String excludeSetting = INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey();
        updateIndexSettings(Settings.builder().put(excludeSetting, nodeNames.get(0)), "index");
        ClusterRerouteUtils.reroute(client(), new CancelAllocationCommand("index", 0, nodeNames.get(0), true));
        assertThat(clusterAdmin().prepareHealth("index").get().getUnassignedShards(), equalTo(1));

        assertThat(client().prepareDelete("index", indexResponse.getId()).get().status(), equalTo(RestStatus.OK));

        assertAcked(
            client().execute(
                FreezeIndexAction.INSTANCE,
                new FreezeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "index").waitForActiveShards(ActiveShardCount.ONE)
            )
        );

        assertThat(
            clusterAdmin().prepareState().get().getState().metadata().index("index").getTimestampRange(),
            sameInstance(IndexLongFieldRange.EMPTY)
        );

        internalCluster().stopNode(nodeNames.get(1));
        assertThat(clusterAdmin().prepareHealth("index").get().getUnassignedShards(), equalTo(2));
        updateIndexSettings(Settings.builder().putNull(excludeSetting), "index");
        assertThat(clusterAdmin().prepareHealth("index").get().getUnassignedShards(), equalTo(2));

        ClusterRerouteUtils.reroute(client(), new AllocateStalePrimaryAllocationCommand("index", 0, nodeNames.get(0), true));

        ensureYellowAndNoInitializingShards("index");

        IndexMetadata indexMetadata = clusterAdmin().prepareState().get().getState().metadata().index("index");
        final IndexLongFieldRange timestampFieldRange = indexMetadata.getTimestampRange();
        assertThat(timestampFieldRange, not(sameInstance(IndexLongFieldRange.UNKNOWN)));
        assertThat(timestampFieldRange, not(sameInstance(IndexLongFieldRange.EMPTY)));
        assertTrue(timestampFieldRange.isComplete());
        assertThat(timestampFieldRange.getMin(), equalTo(Instant.parse(timestampVal).toEpochMilli()));
        assertThat(timestampFieldRange.getMax(), equalTo(Instant.parse(timestampVal).toEpochMilli()));

        IndexLongFieldRange eventIngestedFieldRange = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .index("index")
            .getEventIngestedRange();
        assertThat(eventIngestedFieldRange, not(sameInstance(IndexLongFieldRange.UNKNOWN)));
        assertThat(eventIngestedFieldRange, not(sameInstance(IndexLongFieldRange.EMPTY)));
        assertTrue(eventIngestedFieldRange.isComplete());
        assertThat(eventIngestedFieldRange.getMin(), equalTo(Instant.parse(eventIngestedVal).toEpochMilli()));
        assertThat(eventIngestedFieldRange.getMax(), equalTo(Instant.parse(eventIngestedVal).toEpochMilli()));
    }

    public void testTimestampAndEventIngestedFieldTypeExposedByAllIndicesServices() throws Exception {
        internalCluster().startNodes(between(2, 4));

        final String locale;
        final String date;

        switch (between(1, 3)) {
            case 1 -> {
                locale = "";
                date = "04 Feb 2020 12:01:23Z";
            }
            case 2 -> {
                locale = "en_GB";
                date = "04 Feb 2020 12:01:23Z";
            }
            case 3 -> {
                locale = "fr_FR";
                date = "04 févr. 2020 12:01:23Z";
            }
            default -> throw new AssertionError("impossible");
        }

        assertAcked(
            prepareCreate("index").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject(IndexMetadata.EVENT_INGESTED_FIELD_NAME)
                        .field("type", "date")
                        .field("format", "dd LLL yyyy HH:mm:ssX")
                        .field("locale", locale)
                        .endObject()
                        .startObject(DataStream.TIMESTAMP_FIELD_NAME)
                        .field("type", "date")
                        .field("format", "dd LLL yyyy HH:mm:ssX")
                        .field("locale", locale)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        final Index index = clusterAdmin().prepareState()
            .clear()
            .setIndices("index")
            .setMetadata(true)
            .get()
            .getState()
            .metadata()
            .index("index")
            .getIndex();

        ensureGreen("index");
        if (randomBoolean()) {
            prepareIndex("index").setSource(DataStream.TIMESTAMP_FIELD_NAME, date, IndexMetadata.EVENT_INGESTED_FIELD_NAME, date).get();
        }

        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            assertNull(indicesService.getTimestampFieldTypeInfo(index));
        }

        assertAcked(
            client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "index")).actionGet()
        );
        ensureGreen("index");
        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            final PlainActionFuture<Map<String, DateFieldMapper.DateFieldType>> future = new PlainActionFuture<>();
            assertBusy(() -> {
                DateFieldRangeInfo timestampsFieldTypeInfo = indicesService.getTimestampFieldTypeInfo(index);
                DateFieldMapper.DateFieldType timestampFieldType = timestampsFieldTypeInfo.timestampFieldType();
                DateFieldMapper.DateFieldType eventIngestedFieldType = timestampsFieldTypeInfo.eventIngestedFieldType();
                assertNotNull(eventIngestedFieldType);
                assertNotNull(timestampFieldType);
                future.onResponse(
                    Map.of(
                        DataStream.TIMESTAMP_FIELD_NAME,
                        timestampFieldType,
                        IndexMetadata.EVENT_INGESTED_FIELD_NAME,
                        eventIngestedFieldType
                    )
                );
            });
            assertTrue(future.isDone());
            assertThat(future.get().get(DataStream.TIMESTAMP_FIELD_NAME).dateTimeFormatter().locale().toString(), equalTo(locale));
            assertThat(future.get().get(DataStream.TIMESTAMP_FIELD_NAME).dateTimeFormatter().parseMillis(date), equalTo(1580817683000L));
            assertThat(future.get().get(IndexMetadata.EVENT_INGESTED_FIELD_NAME).dateTimeFormatter().locale().toString(), equalTo(locale));
            assertThat(
                future.get().get(IndexMetadata.EVENT_INGESTED_FIELD_NAME).dateTimeFormatter().parseMillis(date),
                equalTo(1580817683000L)
            );
        }

        assertAcked(
            client().execute(
                FreezeIndexAction.INSTANCE,
                new FreezeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "index").setFreeze(false)
            ).actionGet()
        );
        ensureGreen("index");
        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            assertNull(indicesService.getTimestampFieldTypeInfo(index));
        }
    }

    public void testTimestampOrEventIngestedFieldTypeExposedByAllIndicesServices() throws Exception {
        internalCluster().startNodes(between(2, 4));

        final String locale;
        final String date;

        switch (between(1, 3)) {
            case 1 -> {
                locale = "";
                date = "04 Feb 2020 12:01:23Z";
            }
            case 2 -> {
                locale = "en_GB";
                date = "04 Feb 2020 12:01:23Z";
            }
            case 3 -> {
                locale = "fr_FR";
                date = "04 févr. 2020 12:01:23Z";
            }
            default -> throw new AssertionError("impossible");
        }

        String timeField = randomFrom(IndexMetadata.EVENT_INGESTED_FIELD_NAME, DataStream.TIMESTAMP_FIELD_NAME);
        assertAcked(
            prepareCreate("index").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject(timeField)
                        .field("type", "date")
                        .field("format", "dd LLL yyyy HH:mm:ssX")
                        .field("locale", locale)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        final Index index = clusterAdmin().prepareState()
            .clear()
            .setIndices("index")
            .setMetadata(true)
            .get()
            .getState()
            .metadata()
            .index("index")
            .getIndex();

        ensureGreen("index");
        if (randomBoolean()) {
            prepareIndex("index").setSource(timeField, date).get();
        }

        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            assertNull(indicesService.getTimestampFieldTypeInfo(index));
        }

        assertAcked(
            client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "index")).actionGet()
        );
        ensureGreen("index");
        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            // final PlainActionFuture<DateFieldMapper.DateFieldType> timestampFieldTypeFuture = new PlainActionFuture<>();
            final PlainActionFuture<Map<String, DateFieldMapper.DateFieldType>> future = new PlainActionFuture<>();
            assertBusy(() -> {
                DateFieldRangeInfo timestampsFieldTypeInfo = indicesService.getTimestampFieldTypeInfo(index);
                DateFieldMapper.DateFieldType timestampFieldType = timestampsFieldTypeInfo.timestampFieldType();
                DateFieldMapper.DateFieldType eventIngestedFieldType = timestampsFieldTypeInfo.eventIngestedFieldType();
                if (timeField == DataStream.TIMESTAMP_FIELD_NAME) {
                    assertNotNull(timestampFieldType);
                    assertNull(eventIngestedFieldType);
                    future.onResponse(Map.of(timeField, timestampFieldType));
                } else {
                    assertNull(timestampFieldType);
                    assertNotNull(eventIngestedFieldType);
                    future.onResponse(Map.of(timeField, eventIngestedFieldType));
                }
            });
            assertTrue(future.isDone());
            assertThat(future.get().get(timeField).dateTimeFormatter().locale().toString(), equalTo(locale));
            assertThat(future.get().get(timeField).dateTimeFormatter().parseMillis(date), equalTo(1580817683000L));
        }

        assertAcked(
            client().execute(
                FreezeIndexAction.INSTANCE,
                new FreezeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "index").setFreeze(false)
            ).actionGet()
        );
        ensureGreen("index");
        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            assertNull(indicesService.getTimestampFieldTypeInfo(index));
        }
    }

    public void testRetryPointInTime() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        final List<String> dataNodes = internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .collect(Collectors.toList());
        final String assignedNode = randomFrom(dataNodes);
        final String indexName = "test";
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.require._name", assignedNode)
                        .build()
                )
                .setMapping("{\"properties\":{\"created_date\":{\"type\": \"date\", \"format\": \"yyyy-MM-dd\"}}}")
        );
        int numDocs = randomIntBetween(1, 100);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setSource("created_date", "2011-02-02").get();
        }
        assertAcked(
            client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, indexName))
                .actionGet()
        );
        final OpenPointInTimeRequest openPointInTimeRequest = new OpenPointInTimeRequest(indexName).indicesOptions(
            IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED
        ).keepAlive(TimeValue.timeValueMinutes(2));
        final BytesReference pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openPointInTimeRequest)
            .actionGet()
            .getPointInTimeId();
        try {
            assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), searchResponse -> {
                assertThat(searchResponse.pointInTimeId(), equalTo(pitId));
                assertHitCount(searchResponse, numDocs);
            });
            internalCluster().restartNode(assignedNode);
            ensureGreen(indexName);

            assertNoFailuresAndResponse(
                prepareSearch().setQuery(new RangeQueryBuilder("created_date").gte("2011-01-01").lte("2011-12-12"))
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setPreFilterShardSize(between(1, 10))
                    .setAllowPartialSearchResults(true)
                    .setPointInTime(new PointInTimeBuilder(pitId)),
                searchResponse -> {
                    assertThat(searchResponse.pointInTimeId(), equalTo(pitId));
                    assertHitCount(searchResponse, numDocs);
                }
            );
        } finally {
            assertAcked(
                client().execute(
                    FreezeIndexAction.INSTANCE,
                    new FreezeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, indexName).setFreeze(false)
                ).actionGet()
            );
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
        }
    }

    public void testPointInTimeWithDeletedIndices() {
        createIndex("index-1");
        createIndex("index-2");

        int index1 = randomIntBetween(10, 50);
        for (int i = 0; i < index1; i++) {
            String id = Integer.toString(i);
            prepareIndex("index-1").setId(id).setSource("value", i).get();
        }

        int index2 = randomIntBetween(10, 50);
        for (int i = 0; i < index2; i++) {
            String id = Integer.toString(i);
            prepareIndex("index-2").setId(id).setSource("value", i).get();
        }

        assertAcked(
            client().execute(
                FreezeIndexAction.INSTANCE,
                new FreezeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "index-1", "index-2")
            ).actionGet()
        );
        final OpenPointInTimeRequest openPointInTimeRequest = new OpenPointInTimeRequest("index-*").indicesOptions(
            IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED
        ).keepAlive(TimeValue.timeValueMinutes(2));

        final BytesReference pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openPointInTimeRequest)
            .actionGet()
            .getPointInTimeId();
        try {
            indicesAdmin().prepareDelete("index-1").get();
            // Return partial results if allow partial search result is allowed
            assertResponse(
                prepareSearch().setAllowPartialSearchResults(true).setPointInTime(new PointInTimeBuilder(pitId)),
                searchResponse -> {
                    assertFailures(searchResponse);
                    assertHitCount(searchResponse, index2);
                }
            );
            // Fails if allow partial search result is not allowed
            expectThrows(
                ElasticsearchException.class,
                prepareSearch().setAllowPartialSearchResults(false).setPointInTime(new PointInTimeBuilder(pitId))
            );
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
        }
    }

    public void testOpenPointInTimeWithNoIndexMatched() {
        createIndex("test-index");

        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            prepareIndex("test-index").setId(id).setSource("value", i).get();
        }
        assertAcked(
            client().execute(FreezeIndexAction.INSTANCE, new FreezeRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-index"))
                .actionGet()
        );
        // include the frozen indices
        {
            final OpenPointInTimeRequest openPointInTimeRequest = new OpenPointInTimeRequest("test-*").indicesOptions(
                IndicesOptions.strictExpandOpenAndForbidClosed()
            ).keepAlive(TimeValue.timeValueMinutes(2));
            final BytesReference pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openPointInTimeRequest)
                .actionGet()
                .getPointInTimeId();
            try {
                assertNoFailuresAndResponse(
                    prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)),
                    searchResponse -> assertHitCount(searchResponse, numDocs)
                );
            } finally {
                client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
            }
        }
        // exclude the frozen indices
        {
            final OpenPointInTimeRequest openPointInTimeRequest = new OpenPointInTimeRequest("test-*").keepAlive(
                TimeValue.timeValueMinutes(2)
            );
            final BytesReference pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openPointInTimeRequest)
                .actionGet()
                .getPointInTimeId();
            try {
                assertHitCountAndNoFailures(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), 0);
            } finally {
                client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
            }
        }
    }
}
