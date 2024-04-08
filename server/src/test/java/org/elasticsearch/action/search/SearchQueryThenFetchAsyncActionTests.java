/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;
import static org.elasticsearch.test.VersionUtils.allVersions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class SearchQueryThenFetchAsyncActionTests extends ESTestCase {
    public void testBottomFieldSort() throws Exception {
        testCase(false, false);
    }

    public void testScrollDisableBottomFieldSort() throws Exception {
        testCase(true, false);
    }

    public void testCollapseDisableBottomFieldSort() throws Exception {
        testCase(false, true);
    }

    private void testCase(boolean withScroll, boolean withCollapse) throws Exception {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node2");
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        int numShards = randomIntBetween(10, 20);
        int numConcurrent = randomIntBetween(1, 4);
        AtomicInteger numWithTopDocs = new AtomicInteger();
        AtomicInteger successfulOps = new AtomicInteger();
        AtomicBoolean canReturnNullResponse = new AtomicBoolean(false);
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                SearchActionListener<? super SearchPhaseResult> listener
            ) {
                int shardId = request.shardId().id();
                if (request.canReturnNullResponseIfMatchNoDocs()) {
                    canReturnNullResponse.set(true);
                }
                if (request.getBottomSortValues() != null) {
                    assertNotEquals(shardId, (int) request.getBottomSortValues().getFormattedSortValues()[0]);
                    numWithTopDocs.incrementAndGet();
                }
                QuerySearchResult queryResult = new QuerySearchResult(
                    new ShardSearchContextId("N/A", 123),
                    new SearchShardTarget("node1", new ShardId("idx", "na", shardId), null),
                    null
                );
                try {
                    SortField sortField = new SortField("timestamp", SortField.Type.LONG);
                    if (withCollapse) {
                        queryResult.topDocs(
                            new TopDocsAndMaxScore(
                                new TopFieldGroups(
                                    "collapse_field",
                                    new TotalHits(
                                        1,
                                        withScroll ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                                    ),
                                    new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { request.shardId().id() }) },
                                    new SortField[] { sortField },
                                    new Object[] { 0L }
                                ),
                                Float.NaN
                            ),
                            new DocValueFormat[] { DocValueFormat.RAW }
                        );
                    } else {
                        queryResult.topDocs(
                            new TopDocsAndMaxScore(
                                new TopFieldDocs(
                                    new TotalHits(
                                        1,
                                        withScroll ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                                    ),
                                    new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { request.shardId().id() }) },
                                    new SortField[] { sortField }
                                ),
                                Float.NaN
                            ),
                            new DocValueFormat[] { DocValueFormat.RAW }
                        );
                    }
                    queryResult.from(0);
                    queryResult.size(1);
                    successfulOps.incrementAndGet();
                    queryResult.incRef();
                    new Thread(() -> ActionListener.respondAndRelease(listener, queryResult)).start();
                } finally {
                    queryResult.decRef();
                }
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            randomBoolean(),
            primaryNode,
            replicaNode
        );
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.setMaxConcurrentShardRequests(numConcurrent);
        searchRequest.setBatchedReduceSize(2);
        searchRequest.source(new SearchSourceBuilder().size(1).sort(SortBuilders.fieldSort("timestamp")));
        if (withScroll) {
            searchRequest.scroll(TimeValue.timeValueMillis(100));
        } else {
            searchRequest.source().trackTotalHitsUpTo(2);
        }
        if (withCollapse) {
            searchRequest.source().collapse(new CollapseBuilder("collapse_field"));
        }
        searchRequest.allowPartialSearchResults(false);
        SearchPhaseController controller = new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder());
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        try (
            QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
                searchRequest,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                controller,
                task::isCancelled,
                task.getProgressListener(),
                shardsIter.size(),
                exc -> {}
            )
        ) {
            SearchQueryThenFetchAsyncAction action = new SearchQueryThenFetchAsyncAction(
                logger,
                null,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", AliasFilter.EMPTY),
                Collections.emptyMap(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                resultConsumer,
                searchRequest,
                null,
                shardsIter,
                timeProvider,
                new ClusterState.Builder(new ClusterName("test")).build(),
                task,
                SearchResponse.Clusters.EMPTY
            ) {
                @Override
                protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                    return new SearchPhase("test") {
                        @Override
                        public void run() {
                            latch.countDown();
                        }
                    };
                }
            };
            action.start();
            latch.await();
            assertThat(successfulOps.get(), equalTo(numShards));
            if (withScroll) {
                assertFalse(canReturnNullResponse.get());
                assertThat(numWithTopDocs.get(), equalTo(0));
            } else {
                assertTrue(canReturnNullResponse.get());
                if (withCollapse) {
                    assertThat(numWithTopDocs.get(), equalTo(0));
                } else {
                    assertThat(numWithTopDocs.get(), greaterThanOrEqualTo(1));
                }
            }
            SearchPhaseController.ReducedQueryPhase phase = action.results.reduce();
            assertThat(phase.numReducePhases(), greaterThanOrEqualTo(1));
            if (withScroll) {
                assertThat(phase.totalHits().value, equalTo((long) numShards));
                assertThat(phase.totalHits().relation, equalTo(TotalHits.Relation.EQUAL_TO));
            } else {
                assertThat(phase.totalHits().value, equalTo(2L));
                assertThat(phase.totalHits().relation, equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
            }
            assertThat(phase.sortedTopDocs().scoreDocs().length, equalTo(1));
            assertThat(phase.sortedTopDocs().scoreDocs()[0], instanceOf(FieldDoc.class));
            assertThat(((FieldDoc) phase.sortedTopDocs().scoreDocs()[0]).fields.length, equalTo(1));
            assertThat(((FieldDoc) phase.sortedTopDocs().scoreDocs()[0]).fields[0], equalTo(0));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/101932")
    public void testMinimumVersionSameAsNewVersion() throws Exception {
        var newVersion = VersionInformation.CURRENT;
        var oldVersion = new VersionInformation(
            VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), VersionUtils.getPreviousVersion()),
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersionUtils.randomCompatibleVersion(random())
        );
        testMixedVersionsShardsSearch(newVersion, oldVersion, newVersion.nodeVersion());
    }

    public void testMinimumVersionBetweenNewAndOldVersion() throws Exception {
        var oldVersion = new VersionInformation(
            VersionUtils.getFirstVersion(),
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersionUtils.randomCompatibleVersion(random())
        );

        var newVersion = new VersionInformation(
            VersionUtils.maxCompatibleVersion(VersionUtils.getFirstVersion()),
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersion.current()
        );

        var minVersion = VersionUtils.randomVersionBetween(
            random(),
            allVersions().get(allVersions().indexOf(oldVersion.nodeVersion()) + 1),
            newVersion.nodeVersion()
        );

        testMixedVersionsShardsSearch(newVersion, oldVersion, minVersion);
    }

    private void testMixedVersionsShardsSearch(VersionInformation oldVersion, VersionInformation newVersion, Version minVersion)
        throws Exception {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        int numConcurrent = randomIntBetween(1, 4);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode newVersionNode = DiscoveryNodeUtils.builder("node1").version(newVersion).build();
        DiscoveryNode oldVersionNode = DiscoveryNodeUtils.builder("node2").version(oldVersion).build();
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(newVersionNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(oldVersionNode));

        OriginalIndices idx = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        ArrayList<SearchShardIterator> list = new ArrayList<>();
        ShardRouting routingNewVersionShard = ShardRouting.newUnassigned(
            new ShardId(new Index("idx", "_na_"), 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
            ShardRouting.Role.DEFAULT
        );
        routingNewVersionShard = routingNewVersionShard.initialize(newVersionNode.getId(), "p0", 0);
        routingNewVersionShard.started();
        list.add(new SearchShardIterator(null, new ShardId(new Index("idx", "_na_"), 0), singletonList(routingNewVersionShard), idx));

        ShardRouting routingOldVersionShard = ShardRouting.newUnassigned(
            new ShardId(new Index("idx", "_na_"), 1),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
            ShardRouting.Role.DEFAULT
        );
        routingOldVersionShard = routingOldVersionShard.initialize(oldVersionNode.getId(), "p1", 0);
        routingOldVersionShard.started();
        list.add(new SearchShardIterator(null, new ShardId(new Index("idx", "_na_"), 1), singletonList(routingOldVersionShard), idx));

        GroupShardsIterator<SearchShardIterator> shardsIter = new GroupShardsIterator<>(list);
        final SearchRequest searchRequest = new SearchRequest(minVersion);
        searchRequest.setMaxConcurrentShardRequests(numConcurrent);
        searchRequest.setBatchedReduceSize(2);
        searchRequest.source(new SearchSourceBuilder().size(1));
        searchRequest.allowPartialSearchResults(false);

        SearchTransportService searchTransportService = new SearchTransportService(null, null, null);
        SearchPhaseController controller = new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder());
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            task::isCancelled,
            task.getProgressListener(),
            shardsIter.size(),
            exc -> {}
        );
        final List<Object> responses = new ArrayList<>();
        SearchQueryThenFetchAsyncAction newSearchAsyncAction = new SearchQueryThenFetchAsyncAction(
            logger,
            null,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", AliasFilter.EMPTY),
            Collections.emptyMap(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            resultConsumer,
            searchRequest,
            new ActionListener<>() {
                @Override
                public void onFailure(Exception e) {
                    responses.add(e);
                }

                public void onResponse(SearchResponse response) {
                    responses.add(response);
                };
            },
            shardsIter,
            timeProvider,
            new ClusterState.Builder(new ClusterName("test")).build(),
            task,
            SearchResponse.Clusters.EMPTY
        );

        newSearchAsyncAction.start();
        assertThat(responses, hasSize(1));
        assertThat(responses.get(0), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException e = (SearchPhaseExecutionException) responses.get(0);
        assertThat(e.getCause(), instanceOf(VersionMismatchException.class));
        assertThat(
            e.getCause().getMessage(),
            equalTo("One of the shards is incompatible with the required minimum version [" + minVersion + "]")
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/101932")
    public void testMinimumVersionSameAsOldVersion() throws Exception {
        Version newVersion = Version.CURRENT;
        Version oldVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.CURRENT.minimumCompatibilityVersion(),
            VersionUtils.getPreviousVersion(newVersion)
        );
        Version minVersion = oldVersion;

        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        AtomicInteger successfulOps = new AtomicInteger();

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode newVersionNode = DiscoveryNodeUtils.builder("node1").version(newVersion).build();
        DiscoveryNode oldVersionNode = DiscoveryNodeUtils.builder("node2").version(oldVersion).build();
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(newVersionNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(oldVersionNode));

        OriginalIndices idx = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        ArrayList<SearchShardIterator> list = new ArrayList<>();
        ShardRouting routingNewVersionShard = ShardRouting.newUnassigned(
            new ShardId(new Index("idx", "_na_"), 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
            ShardRouting.Role.DEFAULT
        );
        routingNewVersionShard = routingNewVersionShard.initialize(newVersionNode.getId(), "p0", 0);
        routingNewVersionShard.started();
        list.add(new SearchShardIterator(null, new ShardId(new Index("idx", "_na_"), 0), singletonList(routingNewVersionShard), idx));

        ShardRouting routingOldVersionShard = ShardRouting.newUnassigned(
            new ShardId(new Index("idx", "_na_"), 1),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
            ShardRouting.Role.DEFAULT
        );
        routingOldVersionShard = routingOldVersionShard.initialize(oldVersionNode.getId(), "p1", 0);
        routingOldVersionShard.started();
        list.add(new SearchShardIterator(null, new ShardId(new Index("idx", "_na_"), 1), singletonList(routingOldVersionShard), idx));

        GroupShardsIterator<SearchShardIterator> shardsIter = new GroupShardsIterator<>(list);
        final SearchRequest searchRequest = new SearchRequest(minVersion);
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().size(1).sort(SortBuilders.fieldSort("timestamp")));

        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                SearchActionListener<? super SearchPhaseResult> listener
            ) {
                int shardId = request.shardId().id();
                QuerySearchResult queryResult = new QuerySearchResult(
                    new ShardSearchContextId("N/A", 123),
                    new SearchShardTarget("node1", new ShardId("idx", "na", shardId), null),
                    null
                );
                SortField sortField = new SortField("timestamp", SortField.Type.LONG);
                if (shardId == 0) {
                    queryResult.topDocs(
                        new TopDocsAndMaxScore(
                            new TopFieldDocs(
                                new TotalHits(1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                                new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { shardId }) },
                                new SortField[] { sortField }
                            ),
                            Float.NaN
                        ),
                        new DocValueFormat[] { DocValueFormat.RAW }
                    );
                } else if (shardId == 1) {
                    queryResult.topDocs(
                        new TopDocsAndMaxScore(
                            new TopFieldDocs(
                                new TotalHits(1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                                new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { shardId }) },
                                new SortField[] { sortField }
                            ),
                            Float.NaN
                        ),
                        new DocValueFormat[] { DocValueFormat.RAW }
                    );
                }
                queryResult.from(0);
                queryResult.size(1);
                successfulOps.incrementAndGet();
                new Thread(() -> listener.onResponse(queryResult)).start();
            }
        };
        SearchPhaseController controller = new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder());
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            task::isCancelled,
            task.getProgressListener(),
            shardsIter.size(),
            exc -> {}
        );
        CountDownLatch latch = new CountDownLatch(1);
        SearchQueryThenFetchAsyncAction action = new SearchQueryThenFetchAsyncAction(
            logger,
            null,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", AliasFilter.EMPTY),
            Collections.emptyMap(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            resultConsumer,
            searchRequest,
            null,
            shardsIter,
            timeProvider,
            new ClusterState.Builder(new ClusterName("test")).build(),
            task,
            SearchResponse.Clusters.EMPTY
        ) {
            @Override
            protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                return new SearchPhase("test") {
                    @Override
                    public void run() {
                        latch.countDown();
                    }
                };
            }
        };

        action.start();
        latch.await();
        assertThat(successfulOps.get(), equalTo(2));
        SearchPhaseController.ReducedQueryPhase phase = action.results.reduce();
        assertThat(phase.numReducePhases(), greaterThanOrEqualTo(1));
        assertThat(phase.totalHits().value, equalTo(2L));
        assertThat(phase.totalHits().relation, equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/101932")
    public void testMinimumVersionShardDuringPhaseExecution() throws Exception {
        Version newVersion = Version.CURRENT;
        Version oldVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.CURRENT.minimumCompatibilityVersion(),
            VersionUtils.getPreviousVersion(newVersion)
        );
        Version minVersion = newVersion;

        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        AtomicInteger successfulOps = new AtomicInteger();

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode newVersionNode1 = DiscoveryNodeUtils.builder("node1").version(newVersion).build();
        DiscoveryNode newVersionNode2 = DiscoveryNodeUtils.builder("node2").version(newVersion).build();
        DiscoveryNode oldVersionNode = DiscoveryNodeUtils.builder("node3").version(oldVersion).build();
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(newVersionNode1));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(newVersionNode2));
        lookup.put("node3", new SearchAsyncActionTests.MockConnection(oldVersionNode));

        OriginalIndices idx = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        ArrayList<SearchShardIterator> list = new ArrayList<>();
        ShardRouting routingNewVersionShard1 = ShardRouting.newUnassigned(
            new ShardId(new Index("idx", "_na_"), 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
            ShardRouting.Role.DEFAULT
        );
        routingNewVersionShard1 = routingNewVersionShard1.initialize(newVersionNode1.getId(), "p0", 0);
        routingNewVersionShard1.started();
        list.add(new SearchShardIterator(null, new ShardId(new Index("idx", "_na_"), 0), singletonList(routingNewVersionShard1), idx));

        ShardRouting routingNewVersionShard2 = ShardRouting.newUnassigned(
            new ShardId(new Index("idx", "_na_"), 1),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
            ShardRouting.Role.DEFAULT
        );
        routingNewVersionShard2 = routingNewVersionShard2.initialize(newVersionNode2.getId(), "p1", 0);
        routingNewVersionShard2.started();
        list.add(new SearchShardIterator(null, new ShardId(new Index("idx", "_na_"), 1), singletonList(routingNewVersionShard2), idx));

        GroupShardsIterator<SearchShardIterator> shardsIter = new GroupShardsIterator<>(list);
        final SearchRequest searchRequest = new SearchRequest(minVersion);
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().size(1).sort(SortBuilders.fieldSort("timestamp")));

        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendExecuteQuery(
                Transport.Connection connection,
                ShardSearchRequest request,
                SearchTask task,
                SearchActionListener<? super SearchPhaseResult> listener
            ) {
                int shardId = request.shardId().id();
                QuerySearchResult queryResult = new QuerySearchResult(
                    new ShardSearchContextId("N/A", 123),
                    new SearchShardTarget("node1", new ShardId("idx", "na", shardId), null),
                    null
                );
                SortField sortField = new SortField("timestamp", SortField.Type.LONG);
                if (shardId == 0) {
                    queryResult.topDocs(
                        new TopDocsAndMaxScore(
                            new TopFieldDocs(
                                new TotalHits(1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                                new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { shardId }) },
                                new SortField[] { sortField }
                            ),
                            Float.NaN
                        ),
                        new DocValueFormat[] { DocValueFormat.RAW }
                    );
                } else if (shardId == 1) {
                    queryResult.topDocs(
                        new TopDocsAndMaxScore(
                            new TopFieldDocs(
                                new TotalHits(1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                                new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { shardId }) },
                                new SortField[] { sortField }
                            ),
                            Float.NaN
                        ),
                        new DocValueFormat[] { DocValueFormat.RAW }
                    );
                }
                queryResult.from(0);
                queryResult.size(1);
                successfulOps.incrementAndGet();
                new Thread(() -> listener.onResponse(queryResult)).start();
            }
        };
        SearchPhaseController controller = new SearchPhaseController((t, r) -> InternalAggregationTestCase.emptyReduceContextBuilder());
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            task::isCancelled,
            task.getProgressListener(),
            shardsIter.size(),
            exc -> {}
        );
        CountDownLatch latch = new CountDownLatch(1);
        SearchQueryThenFetchAsyncAction action = new SearchQueryThenFetchAsyncAction(
            logger,
            null,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", AliasFilter.EMPTY),
            Collections.emptyMap(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            resultConsumer,
            searchRequest,
            null,
            shardsIter,
            timeProvider,
            new ClusterState.Builder(new ClusterName("test")).build(),
            task,
            SearchResponse.Clusters.EMPTY
        ) {
            @Override
            protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                return new SearchPhase("test") {
                    @Override
                    public void run() {
                        latch.countDown();
                    }
                };
            }
        };
        ShardRouting routingOldVersionShard = ShardRouting.newUnassigned(
            new ShardId(new Index("idx", "_na_"), 2),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
            ShardRouting.Role.DEFAULT
        );
        SearchShardIterator shardIt = new SearchShardIterator(
            null,
            new ShardId(new Index("idx", "_na_"), 2),
            singletonList(routingOldVersionShard),
            idx
        );
        routingOldVersionShard = routingOldVersionShard.initialize(oldVersionNode.getId(), "p2", 0);
        routingOldVersionShard.started();
        action.start();
        latch.await();
        assertThat(successfulOps.get(), equalTo(2));
        SearchPhaseController.ReducedQueryPhase phase = action.results.reduce();
        assertThat(phase.numReducePhases(), greaterThanOrEqualTo(1));
        assertThat(phase.totalHits().value, equalTo(2L));
        assertThat(phase.totalHits().relation, equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));

        SearchShardTarget searchShardTarget = new SearchShardTarget("node3", shardIt.shardId(), null);
        SearchActionListener<SearchPhaseResult> listener = new SearchActionListener<SearchPhaseResult>(searchShardTarget, 0) {
            @Override
            public void onFailure(Exception e) {}

            @Override
            protected void innerOnResponse(SearchPhaseResult response) {}
        };
        Exception e = expectThrows(VersionMismatchException.class, () -> action.executePhaseOnShard(shardIt, searchShardTarget, listener));
        assertThat(e.getMessage(), equalTo("One of the shards is incompatible with the required minimum version [" + minVersion + "]"));
    }
}
