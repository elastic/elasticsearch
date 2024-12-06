/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexOperator;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.empty;

public class LookupFromIndexIT extends AbstractEsqlIntegTestCase {
    /**
     * Quick and dirty test for looking up data from a lookup index.
     */
    public void testLookupIndex() throws IOException {
        // TODO this should *fail* if the target index isn't a lookup type index - it doesn't now.
        int docCount = between(10, 1000);
        List<String> expected = new ArrayList<>(docCount);
        client().admin()
            .indices()
            .prepareCreate("source")
            .setSettings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1))
            .setMapping("data", "type=keyword")
            .get();
        client().admin()
            .indices()
            .prepareCreate("lookup")
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), "lookup")
                    // TODO lookup index mode doesn't seem to force a single shard. That'll break the lookup command.
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            )
            .setMapping("data", "type=keyword", "l", "type=long")
            .get();
        client().admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForGreenStatus().get();

        String[] data = new String[] { "aa", "bb", "cc", "dd" };
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            docs.add(client().prepareIndex("source").setSource(Map.of("data", data[i % data.length])));
            expected.add(data[i % data.length] + ":" + (i % data.length));
        }
        for (int i = 0; i < data.length; i++) {
            docs.add(client().prepareIndex("lookup").setSource(Map.of("data", data[i], "l", i)));
        }
        Collections.sort(expected);
        indexRandom(true, true, docs);

        /*
         * Find the data node hosting the only shard of the source index.
         */
        SearchService searchService = null;
        String nodeWithShard = null;
        ShardId shardId = null;
        node: for (String node : internalCluster().getNodeNames()) {
            searchService = internalCluster().getInstance(SearchService.class, node);
            for (IndexService idx : searchService.getIndicesService()) {
                if (idx.index().getName().equals("source")) {
                    nodeWithShard = node;
                    shardId = new ShardId(idx.index(), 0);
                    break node;
                }
            }
        }
        if (nodeWithShard == null) {
            throw new IllegalStateException("couldn't find any copy of source index");
        }

        List<String> results = new CopyOnWriteArrayList<>();
        /*
         * Run the Driver.
         */
        try (
            SearchContext searchContext = searchService.createSearchContext(
                new ShardSearchRequest(shardId, System.currentTimeMillis(), AliasFilter.EMPTY, null),
                SearchService.NO_TIMEOUT
            )
        ) {
            ShardContext esqlContext = new EsPhysicalOperationProviders.DefaultShardContext(
                0,
                searchContext.getSearchExecutionContext(),
                AliasFilter.EMPTY
            );
            LuceneSourceOperator.Factory source = new LuceneSourceOperator.Factory(
                List.of(esqlContext),
                ctx -> new MatchAllDocsQuery(),
                DataPartitioning.SEGMENT,
                1,
                10000,
                DocIdSetIterator.NO_MORE_DOCS,
                false // no scoring
            );
            ValuesSourceReaderOperator.Factory reader = new ValuesSourceReaderOperator.Factory(
                List.of(
                    new ValuesSourceReaderOperator.FieldInfo(
                        "data",
                        ElementType.BYTES_REF,
                        shard -> searchContext.getSearchExecutionContext().getFieldType("data").blockLoader(null)
                    )
                ),
                List.of(new ValuesSourceReaderOperator.ShardContext(searchContext.getSearchExecutionContext().getIndexReader(), () -> {
                    throw new IllegalStateException("can't load source here");
                })),
                0
            );
            CancellableTask parentTask = new EsqlQueryTask(
                1,
                "test",
                "test",
                "test",
                null,
                Map.of(),
                Map.of(),
                new AsyncExecutionId("test", TaskId.EMPTY_TASK_ID),
                TEST_REQUEST_TIMEOUT
            );
            LookupFromIndexOperator.Factory lookup = new LookupFromIndexOperator.Factory(
                "test",
                parentTask,
                QueryPragmas.ENRICH_MAX_WORKERS.get(Settings.EMPTY),
                1,
                internalCluster().getInstance(TransportEsqlQueryAction.class, nodeWithShard).getLookupFromIndexService(),
                DataType.KEYWORD,
                "lookup",
                "data",
                List.of(new Alias(Source.EMPTY, "l", new ReferenceAttribute(Source.EMPTY, "l", DataType.LONG))),
                Source.EMPTY
            );
            DriverContext driverContext = driverContext();
            try (
                var driver = new Driver(
                    driverContext,
                    source.get(driverContext),
                    List.of(reader.get(driverContext), lookup.get(driverContext)),
                    new PageConsumerOperator(page -> {
                        try {
                            BytesRefVector dataBlock = page.<BytesRefBlock>getBlock(1).asVector();
                            LongVector loadedBlock = page.<LongBlock>getBlock(2).asVector();
                            for (int p = 0; p < page.getPositionCount(); p++) {
                                results.add(dataBlock.getBytesRef(p, new BytesRef()).utf8ToString() + ":" + loadedBlock.getLong(p));
                            }
                        } finally {
                            page.releaseBlocks();
                        }
                    }),
                    () -> {}
                )
            ) {
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeWithShard);
                var driverRunner = new DriverRunner(threadPool.getThreadContext()) {
                    @Override
                    protected void start(Driver driver, ActionListener<Void> driverListener) {
                        Driver.start(
                            threadPool.getThreadContext(),
                            threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
                            driver,
                            between(1, 10000),
                            driverListener
                        );
                    }
                };
                driverRunner.runToCompletion(List.of(driver), future);
                future.actionGet(TimeValue.timeValueSeconds(30));
                assertMap(results.stream().sorted().toList(), matchesList(expected));
            }
            assertDriverContext(driverContext);
        }
    }

    /**
     * Creates a {@link BigArrays} that tracks releases but doesn't throw circuit breaking exceptions.
     */
    private BigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    /**
     * A {@link DriverContext} that won't throw {@link CircuitBreakingException}.
     */
    protected final DriverContext driverContext() {
        var breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
        return new DriverContext(bigArrays(), BlockFactory.getInstance(breaker, bigArrays()));
    }

    public static void assertDriverContext(DriverContext driverContext) {
        assertTrue(driverContext.isFinished());
        assertThat(driverContext.getSnapshot().releasables(), empty());
    }
}
