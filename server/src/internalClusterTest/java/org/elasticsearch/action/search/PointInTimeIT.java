/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class PointInTimeIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(randomIntBetween(100, 500)))
            .build();
    }

    public void testBasic() {
        createIndex("test");
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("test").setId(id).setSource("value", i).get();
        }
        refresh("test");
        String pitId = openPointInTime(new String[] { "test" }, TimeValue.timeValueMinutes(2));
        SearchResponse resp1 = client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pitId)).get();
        assertThat(resp1.pointInTimeId(), equalTo(pitId));
        assertHitCount(resp1, numDocs);
        int deletedDocs = 0;
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                String id = Integer.toString(i);
                client().prepareDelete("test", id).get();
                deletedDocs++;
            }
        }
        refresh("test");
        if (randomBoolean()) {
            SearchResponse resp2 = client().prepareSearch("test").setPreference(null).setQuery(new MatchAllQueryBuilder()).get();
            assertNoFailures(resp2);
            assertHitCount(resp2, numDocs - deletedDocs);
        }
        try {
            SearchResponse resp3 = client().prepareSearch()
                .setPreference(null)
                .setQuery(new MatchAllQueryBuilder())
                .setPointInTime(new PointInTimeBuilder(pitId))
                .get();
            assertNoFailures(resp3);
            assertHitCount(resp3, numDocs);
            assertThat(resp3.pointInTimeId(), equalTo(pitId));
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testMultipleIndices() {
        int numIndices = randomIntBetween(1, 5);
        for (int i = 1; i <= numIndices; i++) {
            createIndex("index-" + i);
        }
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            String index = "index-" + randomIntBetween(1, numIndices);
            client().prepareIndex(index).setId(id).setSource("value", i).get();
        }
        refresh();
        String pitId = openPointInTime(new String[] { "*" }, TimeValue.timeValueMinutes(2));
        try {
            SearchResponse resp = client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pitId)).get();
            assertNoFailures(resp);
            assertHitCount(resp, numDocs);
            assertNotNull(resp.pointInTimeId());
            assertThat(resp.pointInTimeId(), equalTo(pitId));
            int moreDocs = randomIntBetween(10, 50);
            for (int i = 0; i < moreDocs; i++) {
                String id = "more-" + i;
                String index = "index-" + randomIntBetween(1, numIndices);
                client().prepareIndex(index).setId(id).setSource("value", i).get();
            }
            refresh();
            resp = client().prepareSearch().get();
            assertNoFailures(resp);
            assertHitCount(resp, numDocs + moreDocs);

            resp = client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pitId)).get();
            assertNoFailures(resp);
            assertHitCount(resp, numDocs);
            assertNotNull(resp.pointInTimeId());
            assertThat(resp.pointInTimeId(), equalTo(pitId));
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testRelocation() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        createIndex("test", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 1)).build());
        ensureGreen("test");
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("value", i).get();
        }
        refresh();
        String pitId = openPointInTime(new String[] { "test" }, TimeValue.timeValueMinutes(2));
        try {
            SearchResponse resp = client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pitId)).get();
            assertNoFailures(resp);
            assertHitCount(resp, numDocs);
            assertThat(resp.pointInTimeId(), equalTo(pitId));
            final Set<String> dataNodes = clusterService().state()
                .nodes()
                .getDataNodes()
                .values()
                .stream()
                .map(DiscoveryNode::getId)
                .collect(Collectors.toSet());
            final List<String> excludedNodes = randomSubsetOf(2, dataNodes);
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._id", String.join(",", excludedNodes)), "test");
            if (randomBoolean()) {
                int moreDocs = randomIntBetween(10, 50);
                for (int i = 0; i < moreDocs; i++) {
                    client().prepareIndex("test").setId("more-" + i).setSource("value", i).get();
                }
                refresh();
            }
            resp = client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pitId)).get();
            assertNoFailures(resp);
            assertHitCount(resp, numDocs);
            assertThat(resp.pointInTimeId(), equalTo(pitId));
            assertBusy(() -> {
                final Set<String> assignedNodes = clusterService().state()
                    .routingTable()
                    .allShards()
                    .filter(shr -> shr.index().getName().equals("test") && shr.assignedToNode())
                    .map(ShardRouting::currentNodeId)
                    .collect(Collectors.toSet());
                assertThat(assignedNodes, everyItem(not(in(excludedNodes))));
            }, 30, TimeUnit.SECONDS);
            resp = client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pitId)).get();
            assertNoFailures(resp);
            assertHitCount(resp, numDocs);
            assertThat(resp.pointInTimeId(), equalTo(pitId));
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testPointInTimeNotFound() throws Exception {
        createIndex("index");
        int index1 = randomIntBetween(10, 50);
        for (int i = 0; i < index1; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("index").setId(id).setSource("value", i).get();
        }
        refresh();
        String pit = openPointInTime(new String[] { "index" }, TimeValue.timeValueSeconds(5));
        SearchResponse resp1 = client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pit)).get();
        assertNoFailures(resp1);
        assertHitCount(resp1, index1);
        if (rarely()) {
            assertBusy(() -> {
                final CommonStats stats = indicesAdmin().prepareStats().setSearch(true).get().getTotal();
                assertThat(stats.search.getOpenContexts(), equalTo(0L));
            }, 60, TimeUnit.SECONDS);
        } else {
            closePointInTime(resp1.pointInTimeId());
        }
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pit)).get()
        );
        for (ShardSearchFailure failure : e.shardFailures()) {
            assertThat(ExceptionsHelper.unwrapCause(failure.getCause()), instanceOf(SearchContextMissingException.class));
        }
    }

    public void testIndexNotFound() {
        createIndex("index-1");
        createIndex("index-2");

        int index1 = randomIntBetween(10, 50);
        for (int i = 0; i < index1; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("index-1").setId(id).setSource("value", i).get();
        }

        int index2 = randomIntBetween(10, 50);
        for (int i = 0; i < index2; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("index-2").setId(id).setSource("value", i).get();
        }
        refresh();
        String pit = openPointInTime(new String[] { "index-*" }, TimeValue.timeValueMinutes(2));
        try {
            SearchResponse resp = client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pit)).get();
            assertNoFailures(resp);
            assertHitCount(resp, index1 + index2);
            indicesAdmin().prepareDelete("index-1").get();
            if (randomBoolean()) {
                resp = client().prepareSearch("index-*").get();
                assertNoFailures(resp);
                assertHitCount(resp, index2);
            }

            // Allow partial search result
            resp = client().prepareSearch()
                .setPreference(null)
                .setAllowPartialSearchResults(true)
                .setPointInTime(new PointInTimeBuilder(pit))
                .get();
            assertFailures(resp);
            assertHitCount(resp, index2);

            // Do not allow partial search result
            expectThrows(
                ElasticsearchException.class,
                () -> client().prepareSearch()
                    .setPreference(null)
                    .setAllowPartialSearchResults(false)
                    .setPointInTime(new PointInTimeBuilder(pit))
                    .get()
            );
        } finally {
            closePointInTime(pit);
        }
    }

    public void testAllowNoIndex() {
        var request = new OpenPointInTimeRequest("my_index").indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .keepAlive(TimeValue.timeValueMinutes(between(1, 10)));
        String pit = client().execute(OpenPointInTimeAction.INSTANCE, request).actionGet().getPointInTimeId();
        var closeResp = client().execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(pit)).actionGet();
        assertThat(closeResp.status(), equalTo(RestStatus.OK));
    }

    public void testCanMatch() throws Exception {
        final Settings.Builder settings = indexSettings(randomIntBetween(5, 10), 0).put(
            IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(),
            TimeValue.timeValueMillis(randomIntBetween(50, 100))
        );
        assertAcked(prepareCreate("test").setSettings(settings).setMapping("""
            {"properties":{"created_date":{"type": "date", "format": "yyyy-MM-dd"}}}"""));
        ensureGreen("test");
        String pitId = openPointInTime(new String[] { "test*" }, TimeValue.timeValueMinutes(2));
        try {
            for (String node : internalCluster().nodesInclude("test")) {
                for (IndexService indexService : internalCluster().getInstance(IndicesService.class, node)) {
                    for (IndexShard indexShard : indexService) {
                        assertBusy(() -> assertTrue(indexShard.isSearchIdle()));
                    }
                }
            }
            client().prepareIndex("test").setId("1").setSource("created_date", "2020-01-01").get();
            SearchResponse resp = client().prepareSearch()
                .setQuery(new RangeQueryBuilder("created_date").gte("2020-01-02").lte("2020-01-03"))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPreference(null)
                .setPreFilterShardSize(randomIntBetween(2, 3))
                .setMaxConcurrentShardRequests(randomIntBetween(1, 2))
                .setPointInTime(new PointInTimeBuilder(pitId))
                .get();
            assertThat(resp.getHits().getHits(), arrayWithSize(0));
            for (String node : internalCluster().nodesInclude("test")) {
                for (IndexService indexService : internalCluster().getInstance(IndicesService.class, node)) {
                    for (IndexShard indexShard : indexService) {
                        // all shards are still search-idle as we did not acquire new searchers
                        assertTrue(indexShard.isSearchIdle());
                    }
                }
            }
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testPartialResults() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final List<String> dataNodes = internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .toList();
        final String assignedNodeForIndex1 = randomFrom(dataNodes);

        createIndex(
            "test-1",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", assignedNodeForIndex1)
                .build()
        );
        createIndex(
            "test-2",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.exclude._name", assignedNodeForIndex1)
                .build()
        );

        int numDocs1 = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs1; i++) {
            client().prepareIndex(randomFrom("test-1")).setId(Integer.toString(i)).setSource("value", i).get();
        }
        int numDocs2 = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs2; i++) {
            client().prepareIndex(randomFrom("test-2")).setId(Integer.toString(i)).setSource("value", i).get();
        }
        refresh();
        String pitId = openPointInTime(new String[] { "test-*" }, TimeValue.timeValueMinutes(2));
        try {
            SearchResponse resp = client().prepareSearch().setPreference(null).setPointInTime(new PointInTimeBuilder(pitId)).get();
            assertNoFailures(resp);
            assertHitCount(resp, numDocs1 + numDocs2);
            assertThat(resp.pointInTimeId(), equalTo(pitId));

            internalCluster().restartNode(assignedNodeForIndex1);
            resp = client().prepareSearch()
                .setPreference(null)
                .setAllowPartialSearchResults(true)
                .setPointInTime(new PointInTimeBuilder(pitId))
                .get();
            assertFailures(resp);
            assertThat(resp.pointInTimeId(), equalTo(pitId));
            assertHitCount(resp, numDocs2);
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testPITTiebreak() throws Exception {
        assertAcked(indicesAdmin().prepareDelete("index-*").get());
        int numIndex = randomIntBetween(2, 10);
        int expectedNumDocs = 0;
        for (int i = 0; i < numIndex; i++) {
            String index = "index-" + i;
            createIndex(index, Settings.builder().put("index.number_of_shards", 1).build());
            int numDocs = randomIntBetween(3, 20);
            for (int j = 0; j < numDocs; j++) {
                client().prepareIndex(index).setSource("value", randomIntBetween(0, 2)).get();
                expectedNumDocs++;
            }
        }
        refresh("index-*");
        String pit = openPointInTime(new String[] { "index-*" }, TimeValue.timeValueHours(1));
        try {
            for (int size = 1; size <= numIndex; size++) {
                SortOrder order = randomBoolean() ? SortOrder.ASC : SortOrder.DESC;

                assertPagination(new PointInTimeBuilder(pit), expectedNumDocs, size, SortBuilders.pitTiebreaker().order(order));

                assertPagination(new PointInTimeBuilder(pit), expectedNumDocs, size, SortBuilders.scoreSort());
                assertPagination(
                    new PointInTimeBuilder(pit),
                    expectedNumDocs,
                    size,
                    SortBuilders.scoreSort(),
                    SortBuilders.pitTiebreaker().order(order)
                );

                assertPagination(new PointInTimeBuilder(pit), expectedNumDocs, size, SortBuilders.fieldSort("value"));
                assertPagination(
                    new PointInTimeBuilder(pit),
                    expectedNumDocs,
                    size,
                    SortBuilders.fieldSort("value"),
                    SortBuilders.pitTiebreaker().order(order)
                );
            }
        } finally {
            closePointInTime(pit);
        }
    }

    public void testCloseInvalidPointInTime() {
        expectThrows(Exception.class, () -> client().execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest("")).actionGet());
        List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(ClosePointInTimeAction.NAME).get().getTasks();
        assertThat(tasks, empty());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertPagination(PointInTimeBuilder pit, int expectedNumDocs, int size, SortBuilder<?>... sorts) throws Exception {
        Set<String> seen = new HashSet<>();
        SearchRequestBuilder builder = client().prepareSearch().setSize(size).setPointInTime(pit);
        for (SortBuilder<?> sort : sorts) {
            builder.addSort(sort);
        }
        final SearchRequest searchRequest = builder.request().rewrite(null);

        final List<SortBuilder<?>> expectedSorts = searchRequest.source().sorts();
        final int[] reverseMuls = new int[expectedSorts.size()];
        for (int i = 0; i < expectedSorts.size(); i++) {
            reverseMuls[i] = expectedSorts.get(i).order() == SortOrder.ASC ? 1 : -1;
        }
        SearchResponse response = client().search(searchRequest).get();
        Object[] lastSortValues = null;
        while (response.getHits().getHits().length > 0) {
            Object[] lastHitSortValues = null;
            for (SearchHit hit : response.getHits().getHits()) {
                assertTrue(seen.add(hit.getIndex() + hit.getId()));

                if (lastHitSortValues != null) {
                    for (int i = 0; i < expectedSorts.size(); i++) {
                        Comparable value = (Comparable) hit.getRawSortValues()[i];
                        int cmp = value.compareTo(lastHitSortValues[i]) * reverseMuls[i];
                        if (cmp != 0) {
                            assertThat(cmp, equalTo(1));
                            break;
                        }
                    }
                }
                lastHitSortValues = hit.getRawSortValues();
            }
            int len = response.getHits().getHits().length;
            SearchHit last = response.getHits().getHits()[len - 1];
            if (lastSortValues != null) {
                for (int i = 0; i < expectedSorts.size(); i++) {
                    Comparable value = (Comparable) last.getSortValues()[i];
                    int cmp = value.compareTo(lastSortValues[i]) * reverseMuls[i];
                    if (cmp != 0) {
                        assertThat(cmp, equalTo(1));
                        break;
                    }
                }
            }
            assertThat(last.getSortValues().length, equalTo(expectedSorts.size()));
            lastSortValues = last.getSortValues();
            searchRequest.source().searchAfter(last.getSortValues());
            response = client().search(searchRequest).get();
        }
        assertThat(seen.size(), equalTo(expectedNumDocs));
    }

    private String openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(OpenPointInTimeAction.INSTANCE, request).actionGet();
        return response.getPointInTimeId();
    }

    private void closePointInTime(String readerId) {
        client().execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(readerId)).actionGet();
    }
}
