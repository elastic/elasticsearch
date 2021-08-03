/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.searchable_snapshots.CachesStatsRequest;
import org.elasticsearch.client.searchable_snapshots.CachesStatsResponse;
import org.elasticsearch.client.searchable_snapshots.MountSnapshotRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class SearchableSnapshotsIT extends ESRestHighLevelClientTestCase {

    @Before
    public void init() throws Exception {
        {
            final CreateIndexRequest request = new CreateIndexRequest("index");
            final CreateIndexResponse response = highLevelClient().indices().create(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }

        {
            final BulkRequest request = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < 100; i++) {
                request.add(new IndexRequest("index")
                    .source(XContentType.JSON, "num", i, "text", randomAlphaOfLengthBetween(3, 10)));
            }
            final BulkResponse response = highLevelClient().bulk(request, RequestOptions.DEFAULT);
            assertThat(response.status(), is(RestStatus.OK));
            assertThat(response.hasFailures(), is(false));
        }

        {
            final PutRepositoryRequest request = new PutRepositoryRequest("repository");
            request.settings("{\"location\": \".\"}", XContentType.JSON);
            request.type(FsRepository.TYPE);
            final AcknowledgedResponse response = highLevelClient().snapshot().createRepository(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }

        {
            final CreateSnapshotRequest request =
                new CreateSnapshotRequest("repository", "snapshot").waitForCompletion(true).includeGlobalState(false);
            final CreateSnapshotResponse response = highLevelClient().snapshot().create(request, RequestOptions.DEFAULT);
            assertThat(response.getSnapshotInfo().status(), is(RestStatus.OK));
        }

        {
            final DeleteIndexRequest request = new DeleteIndexRequest("index");
            final AcknowledgedResponse response = highLevelClient().indices().delete(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }
    }

    public void testMountSnapshot() throws IOException {
        {
            final MountSnapshotRequest request = new MountSnapshotRequest("repository", "snapshot", "index")
                .waitForCompletion(true)
                .renamedIndex("renamed_index");
            final SearchableSnapshotsClient client = new SearchableSnapshotsClient(highLevelClient());
            final RestoreSnapshotResponse response = execute(request, client::mountSnapshot, client::mountSnapshotAsync);
            assertThat(response.getRestoreInfo().successfulShards(), is(1));
        }

        {
            final SearchRequest request = new SearchRequest("renamed_index");
            final SearchResponse response = highLevelClient().search(request, RequestOptions.DEFAULT);
            assertThat(response.getHits().getTotalHits().value, is(100L));
            assertThat(response.getHits().getHits()[0].getSourceAsMap(), aMapWithSize(2));
        }
    }

    public void testCacheStats() throws Exception {
        final SearchableSnapshotsClient client = new SearchableSnapshotsClient(highLevelClient());
        {
            final MountSnapshotRequest request = new MountSnapshotRequest("repository", "snapshot", "index")
                .waitForCompletion(true)
                .renamedIndex("mounted_index")
                .storage(MountSnapshotRequest.Storage.SHARED_CACHE);
            final RestoreSnapshotResponse response = execute(request, client::mountSnapshot, client::mountSnapshotAsync);
            assertThat(response.getRestoreInfo().successfulShards(), is(1));
        }

        {
            final SearchRequest request = new SearchRequest("mounted_index")
                .source(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("num").from(50)));
            final SearchResponse response = highLevelClient().search(request, RequestOptions.DEFAULT);
            assertThat(response.getHits().getTotalHits().value, is(50L));
            assertThat(response.getHits().getHits()[0].getSourceAsMap(), aMapWithSize(2));
        }

        {
            assertBusy(() -> {
                final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, "/_nodes/stats/thread_pool"));
                assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));

                @SuppressWarnings("unchecked")
                final Map<String, Object> nodes = (Map<String, Object>) extractValue(responseAsMap(response), "nodes");
                assertThat(nodes, notNullValue());

                for (String node : nodes.keySet()) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> threadPools =
                        (Map<String, Object>) extractValue((Map<String, Object>) nodes.get(node), "thread_pool");
                    assertNotNull("No thread pools on node " + node, threadPools);

                    @SuppressWarnings("unchecked")
                    final Map<String, Object> threadPoolStats =
                        (Map<String, Object>) threadPools.get("searchable_snapshots_cache_fetch_async");
                    assertNotNull("No thread pools stats on node " + node, threadPoolStats);

                    final Number active = (Number) extractValue(threadPoolStats, "active");
                    assertThat(node + " has still active tasks", active, equalTo(0));

                    final Number queue = (Number) extractValue(threadPoolStats, "queue");
                    assertThat(node + " has still enqueued tasks", queue, equalTo(0));
                }
            }, 30L, TimeUnit.SECONDS);
        }

        {
            final CachesStatsRequest request = new CachesStatsRequest();
            final CachesStatsResponse response = execute(request, client::cacheStats, client::cacheStatsAsync);

            final List<CachesStatsResponse.NodeCachesStats> nodesCachesStats = response.getNodeCachesStats();
            assertThat(nodesCachesStats, notNullValue());
            assertThat(nodesCachesStats.size(), equalTo(1));
            assertThat(nodesCachesStats.get(0).getNodeId(), not(emptyOrNullString()));

            final CachesStatsResponse.SharedCacheStats stats = nodesCachesStats.get(0).getSharedCacheStats();
            assertThat(stats.getNumRegions(), equalTo(64));
            assertThat(stats.getSize(), equalTo(ByteSizeUnit.MB.toBytes(1L)));
            assertThat(stats.getRegionSize(), equalTo(ByteSizeUnit.KB.toBytes(16L)));
            assertThat(stats.getWrites(), greaterThanOrEqualTo(1L));
            assertThat(stats.getBytesWritten(), greaterThan(0L));
            assertThat(stats.getReads(), greaterThanOrEqualTo(1L));
            assertThat(stats.getBytesRead(), greaterThan(0L));
            assertThat(stats.getEvictions(), equalTo(0L));
        }
    }


    private static void waitForIdlingSearchableSnapshotsThreadPools() throws Exception {

    }
}
