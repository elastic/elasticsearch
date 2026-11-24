/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.elasticsearch.xpack.parquet.write.table.TableReader;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The search replicas of an index of the write engine follow its Iceberg table: after a flush uploads the rows and a refresh tells the
 * replicas the version of the table, the rows are searchable on the replicas, and only there.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetFlushIT.IcebergWorkerPoolFilter.class }
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ParquetSearchReplicaIT extends ESIntegTestCase {

    private static final String INDEX = "parquet-search";
    private static final String REPOSITORY = "lake";
    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    private static Path repoRoot;

    @BeforeClass
    public static void createRepoRoot() {
        repoRoot = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // the engine has no row path, and the test cluster turns this kill switch off for some seeds
            .put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true)
            .put(Environment.PATH_REPO_SETTING.getKey(), repoRoot)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ParquetWriteEnginePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected void beforeIndexDeletion() {
        // the base class compares the copies of every shard, and the primary of this index cannot be searched
    }

    private void startClusterAndIndex(int replicas, Settings.Builder extra) throws IOException {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(replicas + 1);
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoRoot.resolve(REPOSITORY).toString()))
        );
        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("host.name")
            .field("type", "keyword")
            .endObject()
            .startObject("count")
            .field("type", "long")
            .endObject()
            .startObject("bytes")
            .field("type", "integer")
            .endObject()
            .startObject("ratio")
            .field("type", "double")
            .endObject()
            .startObject("ok")
            .field("type", "boolean")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", replicas)
                        .put("index.parquet.repository", REPOSITORY)
                        .put(extra.build())
                )
                .setMapping(mapping)
        );
        ensureGreen(INDEX);
    }

    private BulkResponse bulk(int from, int to) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = from; i < to; i++) {
            bulk.add(
                new IndexRequest(INDEX).source(
                    "@timestamp",
                    BASE_TIMESTAMP + i * 1000L,
                    "host.name",
                    "host-" + (i % 7),
                    "count",
                    i,
                    "bytes",
                    i % 1000,
                    "ratio",
                    i * 0.5 - 100,
                    "ok",
                    i % 2 == 0
                )
            );
        }
        final BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        return response;
    }

    private long count() {
        final SearchResponse response = prepareSearch(INDEX).setSize(0).setTrackTotalHits(true).get();
        try {
            assertThat(response.getFailedShards(), equalTo(0));
            return response.getHits().getTotalHits().value();
        } finally {
            response.decRef();
        }
    }

    private void flushAndRefresh() {
        indicesAdmin().prepareFlush(INDEX).setForce(true).setWaitIfOngoing(true).get();
        indicesAdmin().prepareRefresh(INDEX).get();
    }

    private IndexShardRoutingTable routing() {
        return clusterService().state().routingTable().index(INDEX).shard(0);
    }

    public void testUploadedRowsAreSearchableOnTheReplica() throws Exception {
        startClusterAndIndex(1, Settings.builder());
        assertThat(routing().primaryShard().role(), equalTo(ShardRouting.Role.INDEX_ONLY));
        assertThat(routing().replicaShards().get(0).role(), equalTo(ShardRouting.Role.SEARCH_ONLY));
        assertThat(count(), equalTo(0L));

        bulk(0, 1000);
        // the rows are only in local files of the primary, which nothing searches
        indicesAdmin().prepareRefresh(INDEX).get();
        assertThat(count(), equalTo(0L));

        flushAndRefresh();
        assertThat(count(), equalTo(1000L));

        assertResponse(
            prepareSearch(INDEX).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("count").gte(100).lt(200))
                .setTrackTotalHits(true)
                .addAggregation(AggregationBuilders.terms("hosts").field("host.name").size(10))
                .addAggregation(AggregationBuilders.max("max_ratio").field("ratio")),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(100L));
                final StringTerms hosts = response.getAggregations().get("hosts");
                assertThat(hosts.getBuckets(), hasSize(7));
                final Max maxRatio = response.getAggregations().get("max_ratio");
                assertThat(maxRatio.value(), equalTo(199 * 0.5 - 100));
            }
        );
        assertResponse(
            prepareSearch(INDEX).setSize(1)
                .setQuery(QueryBuilders.termQuery("ok", true))
                .addSort("count", SortOrder.DESC)
                .setTrackTotalHits(true),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(500L));
                final Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
                assertThat(((Number) source.get("count")).longValue(), equalTo(998L));
                assertThat(source.get("host.name"), equalTo("host-" + (998 % 7)));
            }
        );

        // the _id of a document is its row number on the replica
        final GetResponse get = client().prepareGet(INDEX, "5").setRealtime(false).get();
        assertTrue(get.isExists());
        assertThat(((Number) get.getSourceAsMap().get("count")).longValue(), equalTo(5L));
        assertThat(count(QueryBuilders.idsQuery().addIds("1", "2", "3")), equalTo(3L));

        // rows that are not uploaded stay invisible, rows of the next flush appear
        bulk(1000, 1500);
        indicesAdmin().prepareRefresh(INDEX).get();
        assertThat(count(), equalTo(1000L));
        flushAndRefresh();
        assertThat(count(), equalTo(1500L));
    }

    /**
     * The primary tells its search replicas of each version of the table that it commits, so an upload becomes searchable with no refresh
     * of the index, and with no scheduled refresh either (the index has none).
     */
    public void testAnUploadBecomesSearchableWithoutARefresh() throws Exception {
        // no scheduled refresh, so only the announcement of the primary can make the replica read the table
        startClusterAndIndex(1, Settings.builder().put("index.refresh_interval", "-1"));
        bulk(0, 700);
        indicesAdmin().prepareFlush(INDEX).setForce(true).setWaitIfOngoing(true).get();
        assertBusy(() -> assertThat(count(), equalTo(700L)));

        bulk(700, 1000);
        indicesAdmin().prepareFlush(INDEX).setForce(true).setWaitIfOngoing(true).get();
        assertBusy(() -> assertThat(count(), equalTo(1000L)));
    }

    private long count(org.elasticsearch.index.query.QueryBuilder query) {
        final SearchResponse response = prepareSearch(INDEX).setSize(0).setQuery(query).setTrackTotalHits(true).get();
        try {
            return response.getHits().getTotalHits().value();
        } finally {
            response.decRef();
        }
    }

    public void testRefreshWaitForUploadsAndWaitsForTheReplicas() throws Exception {
        startClusterAndIndex(1, Settings.builder());
        // a bulk with refresh=wait_for returns once its rows are uploaded and the search replicas show them
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        for (int i = 0; i < 50; i++) {
            bulk.add(new IndexRequest(INDEX).source("count", i, "host.name", "h"));
        }
        final BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        assertThat(count(), equalTo(50L));
    }

    public void testNoReadReachesThePrimary() throws Exception {
        startClusterAndIndex(1, Settings.builder());
        bulk(0, 100);
        flushAndRefresh();
        final String primaryNode = clusterService().state().nodes().get(routing().primaryShard().currentNodeId()).getName();
        final String replicaNode = clusterService().state().nodes().get(routing().replicaShards().get(0).currentNodeId()).getName();
        assertThat(primaryNode, not(equalTo(replicaNode)));

        for (String preference : List.of(
            "_only_local",
            "_local",
            "_only_nodes:" + clusterService().state().nodes().get(routing().primaryShard().currentNodeId()).getId()
        )) {
            // a preference only chooses among the searchable copies: the replica answers, or no copy does, and never the primary
            final SearchResponse response;
            try {
                response = client(primaryNode).prepareSearch(INDEX).setPreference(preference).setSize(0).setTrackTotalHits(true).get();
            } catch (SearchPhaseExecutionException e) {
                assertThat(
                    "preference " + preference,
                    ExceptionsHelper.unwrapCause(e.getCause()),
                    instanceOf(NoShardAvailableActionException.class)
                );
                continue;
            }
            try {
                assertThat("preference " + preference, response.getFailedShards(), equalTo(0));
                assertThat("preference " + preference, response.getHits().getTotalHits().value(), equalTo(100L));
            } finally {
                response.decRef();
            }
        }
        // the copy on the replica's node answers a search that prefers that node
        assertResponse(
            client(replicaNode).prepareSearch(INDEX).setPreference("_only_local").setSize(0).setTrackTotalHits(true),
            response -> assertThat(response.getHits().getTotalHits().value(), equalTo(100L))
        );
    }

    public void testACompactionDoesNotChangeTheResults() throws Exception {
        // the replaced files stay for the default delete delay, so that a replica that has not read the new version still reads them
        startClusterAndIndex(1, Settings.builder());
        for (int round = 0; round < 3; round++) {
            bulk(round * 100, (round + 1) * 100);
            indicesAdmin().prepareFlush(INDEX).setForce(true).setWaitIfOngoing(true).get();
        }
        indicesAdmin().prepareRefresh(INDEX).get();
        assertThat(count(), equalTo(300L));
        // the compaction merges the small files in the background into one file
        final String uuid = resolveIndex(INDEX).getUUID();
        final TableReader table = TableReader.open(
            new FsBlobStore(4096, repoRoot.resolve(REPOSITORY), true),
            new TableLocation(
                repoRoot.resolve(REPOSITORY).resolve(INDEX).resolve(uuid).toUri().toString().replaceAll("/+$", ""),
                BlobPath.EMPTY.add(INDEX).add(uuid)
            )
        );
        assertBusy(() -> assertThat(table.read().files(), hasSize(1)));
        // the replica follows the new version, and the results do not change
        assertBusy(() -> {
            indicesAdmin().prepareRefresh(INDEX).get();
            assertThat(count(), equalTo(300L));
            assertResponse(
                prepareSearch(INDEX).setSize(0).addAggregation(AggregationBuilders.max("max").field("count")),
                response -> assertThat(((Max) response.getAggregations().get("max")).value(), equalTo(299.0))
            );
        });
    }

    public void testASearchReplicaCanMove() throws Exception {
        startClusterAndIndex(1, Settings.builder());
        bulk(0, 200);
        flushAndRefresh();
        final String newNode = internalCluster().startDataOnlyNode();
        final ShardRouting replica = routing().replicaShards().get(0);
        final DiscoveryNode from = clusterService().state().nodes().get(replica.currentNodeId());
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(INDEX, 0, from.getName(), newNode));
        ensureGreen(INDEX);
        assertThat(clusterService().state().nodes().get(routing().replicaShards().get(0).currentNodeId()).getName(), equalTo(newNode));
        assertThat(count(), equalTo(200L));
    }
}
