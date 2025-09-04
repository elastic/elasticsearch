/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.MockDeprecatedAggregationBuilder;
import org.elasticsearch.xpack.core.transform.MockDeprecatedQueryBuilder;
import org.elasticsearch.xpack.core.transform.TransformNamedXContentProvider;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.transform.LocalStateTransform;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TransformCCSCanMatchIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";
    private static final TimeValue TIMEOUT = TimeValue.timeValueMinutes(1);

    private NamedXContentRegistry namedXContentRegistry;
    private long timestamp;
    private int oldLocalNumShards;
    private int localOldDocs;
    private int oldRemoteNumShards;
    private int remoteOldDocs;
    private int newLocalNumShards;
    private int localNewDocs;
    private int newRemoteNumShards;
    private int remoteNewDocs;

    @Before
    public void setUpNamedXContentRegistryAndIndices() throws Exception {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                QueryBuilder.class,
                new ParseField(MockDeprecatedQueryBuilder.NAME),
                (p, c) -> MockDeprecatedQueryBuilder.fromXContent(p)
            )
        );
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                BaseAggregationBuilder.class,
                new ParseField(MockDeprecatedAggregationBuilder.NAME),
                (p, c) -> MockDeprecatedAggregationBuilder.fromXContent(p)
            )
        );

        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        namedXContentRegistry = new NamedXContentRegistry(namedXContents);

        timestamp = randomLongBetween(10_000_000, 50_000_000);

        oldLocalNumShards = randomIntBetween(1, 5);
        localOldDocs = createIndexAndIndexDocs(LOCAL_CLUSTER, "local_old_index", oldLocalNumShards, timestamp - 10_000, true);
        oldRemoteNumShards = randomIntBetween(1, 5);
        remoteOldDocs = createIndexAndIndexDocs(REMOTE_CLUSTER, "remote_old_index", oldRemoteNumShards, timestamp - 10_000, true);

        newLocalNumShards = randomIntBetween(1, 5);
        localNewDocs = createIndexAndIndexDocs(LOCAL_CLUSTER, "local_new_index", newLocalNumShards, timestamp, randomBoolean());
        newRemoteNumShards = randomIntBetween(1, 5);
        remoteNewDocs = createIndexAndIndexDocs(REMOTE_CLUSTER, "remote_new_index", newRemoteNumShards, timestamp, randomBoolean());
    }

    @After
    public void cleanup() {
        client().execute(ResetFeatureStateAction.INSTANCE, new ResetFeatureStateRequest(TEST_REQUEST_TIMEOUT)).actionGet();
    }

    private int createIndexAndIndexDocs(String cluster, String index, int numberOfShards, long timestamp, boolean exposeTimestamp)
        throws Exception {
        Client client = client(cluster);
        ElasticsearchAssertions.assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("@timestamp", "type=date", "position", "type=long")
        );
        int numDocs = between(100, 500);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("position", i, "@timestamp", timestamp + i).get();
        }
        if (exposeTimestamp) {
            client.admin().indices().prepareClose(index).get();
            client.admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
                .get();
            client.admin().indices().prepareOpen(index).get();
            assertBusy(() -> {
                IndexLongFieldRange timestampRange = cluster(cluster).clusterService()
                    .state()
                    .metadata()
                    .getProject()
                    .index(index)
                    .getTimestampRange();
                assertTrue(Strings.toString(timestampRange), timestampRange.containsAllShardRanges());
            });
        } else {
            client.admin().indices().prepareRefresh(index).get();
        }
        return numDocs;
    }

    public void testSearchAction_MatchAllQuery() throws ExecutionException, InterruptedException {
        testSearchAction(QueryBuilders.matchAllQuery(), true, localOldDocs + localNewDocs + remoteOldDocs + remoteNewDocs, 0);
        testSearchAction(QueryBuilders.matchAllQuery(), false, localOldDocs + localNewDocs + remoteOldDocs + remoteNewDocs, 0);
    }

    public void testSearchAction_RangeQuery() throws ExecutionException, InterruptedException {
        testSearchAction(
            QueryBuilders.rangeQuery("@timestamp").from(timestamp),  // This query only matches new documents
            true,
            localNewDocs + remoteNewDocs,
            oldLocalNumShards + oldRemoteNumShards
        );
        testSearchAction(
            QueryBuilders.rangeQuery("@timestamp").from(timestamp),  // This query only matches new documents
            false,
            localNewDocs + remoteNewDocs,
            oldLocalNumShards + oldRemoteNumShards
        );
    }

    public void testSearchAction_RangeQueryThatMatchesNoShards() throws ExecutionException, InterruptedException {
        testSearchAction(
            QueryBuilders.rangeQuery("@timestamp").from(100_000_000),  // This query matches no documents
            true,
            0,
            oldLocalNumShards + newLocalNumShards + oldRemoteNumShards + newRemoteNumShards
        );
        testSearchAction(
            QueryBuilders.rangeQuery("@timestamp").from(100_000_000),  // This query matches no documents
            false,
            0,
            oldLocalNumShards + newLocalNumShards + oldRemoteNumShards + newRemoteNumShards
        );
    }

    private void testSearchAction(QueryBuilder query, boolean ccsMinimizeRoundtrips, long expectedHitCount, int expectedSkippedShards)
        throws ExecutionException, InterruptedException {
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest("local_*", "*:remote_*");
        request.source(source).setCcsMinimizeRoundtrips(ccsMinimizeRoundtrips);
        assertResponse(client().search(request), response -> {
            ElasticsearchAssertions.assertHitCount(response, expectedHitCount);
            int expectedTotalShards = oldLocalNumShards + newLocalNumShards + oldRemoteNumShards + newRemoteNumShards;
            assertThat("Response was: " + response, response.getTotalShards(), is(equalTo(expectedTotalShards)));
            assertThat("Response was: " + response, response.getSuccessfulShards(), is(equalTo(expectedTotalShards)));
            assertThat("Response was: " + response, response.getFailedShards(), is(equalTo(0)));
            assertThat("Response was: " + response, response.getSkippedShards(), is(equalTo(expectedSkippedShards)));
        });
    }

    public void testGetCheckpointAction_MatchAllQuery() throws InterruptedException {
        final var threadContext = client().threadPool().getThreadContext();
        testGetCheckpointAction(
            threadContext,
            CheckpointClient.local(client()),
            null,
            new String[] { "local_*" },
            QueryBuilders.matchAllQuery(),
            Set.of("local_old_index", "local_new_index")
        );
        testGetCheckpointAction(
            threadContext,
            CheckpointClient.remote(
                client().getRemoteClusterClient(
                    REMOTE_CLUSTER,
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
                )
            ),
            REMOTE_CLUSTER,
            new String[] { "remote_*" },
            QueryBuilders.matchAllQuery(),
            Set.of("remote_old_index", "remote_new_index")
        );
    }

    public void testGetCheckpointAction_RangeQuery() throws InterruptedException {
        final var threadContext = client().threadPool().getThreadContext();
        testGetCheckpointAction(
            threadContext,
            CheckpointClient.local(client()),
            null,
            new String[] { "local_*" },
            QueryBuilders.rangeQuery("@timestamp").from(timestamp),
            Set.of("local_new_index")
        );
        testGetCheckpointAction(
            threadContext,
            CheckpointClient.remote(
                client().getRemoteClusterClient(
                    REMOTE_CLUSTER,
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
                )
            ),
            REMOTE_CLUSTER,
            new String[] { "remote_*" },
            QueryBuilders.rangeQuery("@timestamp").from(timestamp),
            Set.of("remote_new_index")
        );
    }

    public void testGetCheckpointAction_RangeQueryThatMatchesNoShards() throws InterruptedException {
        final var threadContext = client().threadPool().getThreadContext();
        testGetCheckpointAction(
            threadContext,
            CheckpointClient.local(client()),
            null,
            new String[] { "local_*" },
            QueryBuilders.rangeQuery("@timestamp").from(100_000_000),
            Set.of()
        );
        testGetCheckpointAction(
            threadContext,
            CheckpointClient.remote(
                client().getRemoteClusterClient(
                    REMOTE_CLUSTER,
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
                )
            ),
            REMOTE_CLUSTER,
            new String[] { "remote_*" },
            QueryBuilders.rangeQuery("@timestamp").from(100_000_000),
            Set.of()
        );
    }

    private void testGetCheckpointAction(
        ThreadContext threadContext,
        CheckpointClient client,
        String cluster,
        String[] indices,
        QueryBuilder query,
        Set<String> expectedIndices
    ) throws InterruptedException {
        final GetCheckpointAction.Request request = new GetCheckpointAction.Request(
            indices,
            IndicesOptions.LENIENT_EXPAND_OPEN,
            query,
            cluster,
            TIMEOUT
        );

        CountDownLatch latch = new CountDownLatch(1);
        SetOnce<GetCheckpointAction.Response> finalResponse = new SetOnce<>();
        SetOnce<Exception> finalException = new SetOnce<>();
        ClientHelper.executeAsyncWithOrigin(threadContext, TRANSFORM_ORIGIN, request, ActionListener.wrap(response -> {
            finalResponse.set(response);
            latch.countDown();
        }, e -> {
            finalException.set(e);
            latch.countDown();
        }), client::getCheckpoint);
        latch.await(10, TimeUnit.SECONDS);

        assertThat(finalException.get(), is(nullValue()));
        assertThat("Response was: " + finalResponse.get(), finalResponse.get().getCheckpoints().keySet(), is(equalTo(expectedIndices)));
    }

    public void testTransformLifecycle_MatchAllQuery() throws Exception {
        testTransformLifecycle(QueryBuilders.matchAllQuery(), localOldDocs + localNewDocs + remoteOldDocs + remoteNewDocs);
    }

    public void testTransformLifecycle_RangeQuery() throws Exception {
        testTransformLifecycle(QueryBuilders.rangeQuery("@timestamp").from(timestamp), localNewDocs + remoteNewDocs);
    }

    public void testTransformLifecycle_RangeQueryThatMatchesNoShards() throws Exception {
        testTransformLifecycle(QueryBuilders.rangeQuery("@timestamp").from(100_000_000), 0);
    }

    private void testTransformLifecycle(QueryBuilder query, long expectedHitCount) throws Exception {
        String transformId = "test-transform-lifecycle";
        {
            QueryConfig queryConfig;
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, query.toString())) {
                queryConfig = QueryConfig.fromXContent(parser, true);
                assertNotNull(queryConfig.getQuery());
            }
            TransformConfig transformConfig = TransformConfig.builder()
                .setId(transformId)
                .setSource(new SourceConfig(new String[] { "local_*", "*:remote_*" }, queryConfig, Map.of()))
                .setDest(new DestConfig(transformId + "-dest", null, null))
                .setLatestConfig(new LatestConfig(List.of("position"), "@timestamp"))
                .build();
            PutTransformAction.Request request = new PutTransformAction.Request(transformConfig, false, TIMEOUT);
            AcknowledgedResponse response = client().execute(PutTransformAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());
        }
        {
            StartTransformAction.Request request = new StartTransformAction.Request(transformId, null, TIMEOUT);
            StartTransformAction.Response response = client().execute(StartTransformAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());
        }
        assertBusy(() -> {
            GetTransformStatsAction.Request request = new GetTransformStatsAction.Request(transformId, TIMEOUT, true);
            GetTransformStatsAction.Response response = client().execute(GetTransformStatsAction.INSTANCE, request).actionGet();
            assertThat("Stats were: " + response.getTransformsStats(), response.getTransformsStats(), hasSize(1));
            assertThat(response.getTransformsStats().get(0).getState(), is(equalTo(TransformStats.State.STOPPED)));
            assertThat(response.getTransformsStats().get(0).getIndexerStats().getNumDocuments(), is(equalTo(expectedHitCount)));
            assertThat(response.getTransformsStats().get(0).getIndexerStats().getNumDeletedDocuments(), is(equalTo(0L)));
            assertThat(response.getTransformsStats().get(0).getIndexerStats().getSearchFailures(), is(equalTo(0L)));
            assertThat(response.getTransformsStats().get(0).getIndexerStats().getIndexFailures(), is(equalTo(0L)));
        });
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), LocalStateTransform.class),
            ExposingTimestampEnginePlugin.class
        );
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master, data, ingest, transform, remote_cluster_client")
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    private static class EngineWithExposingTimestamp extends InternalEngine {
        EngineWithExposingTimestamp(EngineConfig engineConfig) {
            super(engineConfig);
            assert IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(config().getIndexSettings().getSettings()) : "require read-only index";
        }

        @Override
        public ShardLongFieldRange getRawFieldRange(String field) {
            try (Searcher searcher = acquireSearcher("test")) {
                final DirectoryReader directoryReader = searcher.getDirectoryReader();

                final byte[] minPackedValue = PointValues.getMinPackedValue(directoryReader, field);
                final byte[] maxPackedValue = PointValues.getMaxPackedValue(directoryReader, field);
                if (minPackedValue == null || maxPackedValue == null) {
                    assert minPackedValue == null && maxPackedValue == null
                        : Arrays.toString(minPackedValue) + "-" + Arrays.toString(maxPackedValue);
                    return ShardLongFieldRange.EMPTY;
                }

                return ShardLongFieldRange.of(LongPoint.decodeDimension(minPackedValue, 0), LongPoint.decodeDimension(maxPackedValue, 0));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static class ExposingTimestampEnginePlugin extends Plugin implements EnginePlugin {

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(EngineWithExposingTimestamp::new);
            } else {
                return Optional.of(new InternalEngineFactory());
            }
        }
    }
}
