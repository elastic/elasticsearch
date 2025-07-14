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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexOperator;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class LookupFromIndexIT extends AbstractEsqlIntegTestCase {
    public void testKeywordKey() throws IOException {
        runLookup(DataType.KEYWORD, new UsingSingleLookupTable(new String[] { "aa", "bb", "cc", "dd" }));
    }

    public void testLongKey() throws IOException {
        runLookup(DataType.LONG, new UsingSingleLookupTable(new Long[] { 12L, 33L, 1L }));
    }

    /**
     * LOOKUP multiple results match.
     */
    public void testLookupIndexMultiResults() throws IOException {
        runLookup(DataType.KEYWORD, new UsingSingleLookupTable(new String[] { "aa", "bb", "bb", "dd" }));
    }

    interface PopulateIndices {
        void populate(int docCount, List<String> expected) throws IOException;
    }

    class UsingSingleLookupTable implements PopulateIndices {
        private final Map<Object, List<Integer>> matches = new HashMap<>();
        private final Object[] lookupData;

        UsingSingleLookupTable(Object[] lookupData) {
            this.lookupData = lookupData;
            for (int i = 0; i < lookupData.length; i++) {
                matches.computeIfAbsent(lookupData[i], k -> new ArrayList<>()).add(i);
            }
        }

        @Override
        public void populate(int docCount, List<String> expected) {
            List<IndexRequestBuilder> docs = new ArrayList<>();
            for (int i = 0; i < docCount; i++) {
                Object key = lookupData[i % lookupData.length];
                docs.add(client().prepareIndex("source").setSource(Map.of("key", key)));
                for (Integer match : matches.get(key)) {
                    expected.add(key + ":" + match);
                }
            }
            for (int i = 0; i < lookupData.length; i++) {
                docs.add(client().prepareIndex("lookup").setSource(Map.of("key", lookupData[i], "l", i)));
            }
            Collections.sort(expected);
            indexRandom(true, true, docs);
        }
    }

    private void runLookup(DataType keyType, PopulateIndices populateIndices) throws IOException {
        client().admin()
            .indices()
            .prepareCreate("source")
            .setSettings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1))
            .setMapping("key", "type=" + keyType.esType())
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
            .setMapping("key", "type=" + keyType.esType(), "l", "type=long")
            .get();
        client().admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForGreenStatus().get();

        int docCount = between(10, 1000);
        List<String> expected = new ArrayList<>(docCount);
        populateIndices.populate(docCount, expected);

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
                searchContext,
                searchContext.getSearchExecutionContext(),
                AliasFilter.EMPTY
            );
            LuceneSourceOperator.Factory source = new LuceneSourceOperator.Factory(
                List.of(esqlContext),
                ctx -> List.of(new LuceneSliceQueue.QueryAndTags(new MatchAllDocsQuery(), List.of())),
                DataPartitioning.SEGMENT,
                1,
                10000,
                DocIdSetIterator.NO_MORE_DOCS,
                false // no scoring
            );
            ValuesSourceReaderOperator.Factory reader = new ValuesSourceReaderOperator.Factory(
                List.of(
                    new ValuesSourceReaderOperator.FieldInfo(
                        "key",
                        PlannerUtils.toElementType(keyType),
                        shard -> searchContext.getSearchExecutionContext().getFieldType("key").blockLoader(blContext())
                    )
                ),
                List.of(new ValuesSourceReaderOperator.ShardContext(searchContext.getSearchExecutionContext().getIndexReader(), () -> {
                    throw new IllegalStateException("can't load source here");
                }, EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(Settings.EMPTY))),
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
            final String finalNodeWithShard = nodeWithShard;
            LookupFromIndexOperator.Factory lookup = new LookupFromIndexOperator.Factory(
                "test",
                parentTask,
                QueryPragmas.ENRICH_MAX_WORKERS.get(Settings.EMPTY),
                1,
                ctx -> internalCluster().getInstance(TransportEsqlQueryAction.class, finalNodeWithShard).getLookupFromIndexService(),
                keyType,
                "lookup",
                "lookup",
                new FieldAttribute.FieldName("key"),
                List.of(new Alias(Source.EMPTY, "l", new ReferenceAttribute(Source.EMPTY, "l", DataType.LONG))),
                Source.EMPTY
            );
            DriverContext driverContext = driverContext();
            try (
                var driver = TestDriverFactory.create(
                    driverContext,
                    source.get(driverContext),
                    List.of(reader.get(driverContext), lookup.get(driverContext)),
                    new PageConsumerOperator(page -> {
                        try {
                            Block keyBlock = page.getBlock(1);
                            LongVector loadedBlock = page.<LongBlock>getBlock(2).asVector();
                            for (int p = 0; p < page.getPositionCount(); p++) {
                                List<Object> key = BlockTestUtils.valuesAtPositions(keyBlock, p, p + 1).get(0);
                                assertThat(key, hasSize(1));
                                Object keyValue = key.get(0);
                                if (keyValue instanceof BytesRef b) {
                                    keyValue = b.utf8ToString();
                                }
                                results.add(keyValue + ":" + loadedBlock.getLong(p));
                            }
                        } finally {
                            page.releaseBlocks();
                        }
                    })
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

    private static MappedFieldType.BlockLoaderContext blContext() {
        return new MappedFieldType.BlockLoaderContext() {
            @Override
            public String indexName() {
                return "test_index";
            }

            @Override
            public IndexSettings indexSettings() {
                var imd = IndexMetadata.builder("test_index")
                    .settings(ESTestCase.indexSettings(IndexVersion.current(), 1, 1).put(Settings.EMPTY))
                    .build();
                return new IndexSettings(imd, Settings.EMPTY);
            }

            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return MappedFieldType.FieldExtractPreference.NONE;
            }

            @Override
            public SearchLookup lookup() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<String> sourcePaths(String name) {
                return Set.of(name);
            }

            @Override
            public String parentField(String field) {
                return null;
            }

            @Override
            public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                return FieldNamesFieldMapper.FieldNamesFieldType.get(true);
            }
        };
    }
}
