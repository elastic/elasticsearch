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
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PhysicalSettings;
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
        runLookup(List.of(DataType.KEYWORD), new UsingSingleLookupTable(new Object[][] { new String[] { "aa", "bb", "cc", "dd" } }));
    }

    public void testJoinOnTwoKeys() throws IOException {
        runLookup(
            List.of(DataType.KEYWORD, DataType.LONG),
            new UsingSingleLookupTable(new Object[][] { new String[] { "aa", "bb", "cc", "dd" }, new Long[] { 12L, 33L, 1L, 42L } })
        );
    }

    public void testJoinOnThreeKeys() throws IOException {
        runLookup(
            List.of(DataType.KEYWORD, DataType.LONG, DataType.KEYWORD),
            new UsingSingleLookupTable(
                new Object[][] {
                    new String[] { "aa", "bb", "cc", "dd" },
                    new Long[] { 12L, 33L, 1L, 42L },
                    new String[] { "one", "two", "three", "four" }, }
            )
        );
    }

    public void testJoinOnFourKeys() throws IOException {
        runLookup(
            List.of(DataType.KEYWORD, DataType.LONG, DataType.KEYWORD, DataType.INTEGER),
            new UsingSingleLookupTable(
                new Object[][] {
                    new String[] { "aa", "bb", "cc", "dd" },
                    new Long[] { 12L, 33L, 1L, 42L },
                    new String[] { "one", "two", "three", "four" },
                    new Integer[] { 1, 2, 3, 4 }, }
            )
        );
    }

    public void testLongKey() throws IOException {
        runLookup(List.of(DataType.LONG), new UsingSingleLookupTable(new Object[][] { new Long[] { 12L, 33L, 1L } }));
    }

    /**
     * LOOKUP multiple results match.
     */
    public void testLookupIndexMultiResults() throws IOException {
        runLookup(List.of(DataType.KEYWORD), new UsingSingleLookupTable(new Object[][] { new String[] { "aa", "bb", "bb", "dd" } }));
    }

    public void testJoinOnTwoKeysMultiResults() throws IOException {
        runLookup(
            List.of(DataType.KEYWORD, DataType.LONG),
            new UsingSingleLookupTable(new Object[][] { new String[] { "aa", "bb", "bb", "dd" }, new Long[] { 12L, 1L, 1L, 42L } })
        );
    }

    public void testJoinOnThreeKeysMultiResults() throws IOException {
        runLookup(
            List.of(DataType.KEYWORD, DataType.LONG, DataType.KEYWORD),
            new UsingSingleLookupTable(
                new Object[][] {
                    new String[] { "aa", "bb", "bb", "dd" },
                    new Long[] { 12L, 1L, 1L, 42L },
                    new String[] { "one", "two", "two", "four" } }
            )
        );
    }

    interface PopulateIndices {
        void populate(int docCount, List<String> expected) throws IOException;
    }

    class UsingSingleLookupTable implements PopulateIndices {
        private final Map<List<Object>, List<Integer>> matches = new HashMap<>();
        private final Object[][] lookupData;

        // Accepts array of arrays, each sub-array is values for a key field
        // All subarrays must have the same length
        UsingSingleLookupTable(Object[][] lookupData) {
            this.lookupData = lookupData;
            int numRows = lookupData[0].length;
            for (int i = 0; i < numRows; i++) {
                List<Object> key = new ArrayList<>();
                for (Object[] col : lookupData) {
                    key.add(col[i]);
                }
                matches.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
            }
        }

        @Override
        public void populate(int docCount, List<String> expected) {
            List<IndexRequestBuilder> docs = new ArrayList<>();
            int numFields = lookupData.length;
            int numRows = lookupData[0].length;
            for (int i = 0; i < docCount; i++) {
                List<Object> key = new ArrayList<>();
                Map<String, Object> sourceDoc = new HashMap<>();
                for (int f = 0; f < numFields; f++) {
                    Object val = lookupData[f][i % numRows];
                    key.add(val);
                    sourceDoc.put("key" + f, val);
                }
                docs.add(client().prepareIndex("source").setSource(sourceDoc));
                String keyString;
                if (key.size() == 1) {
                    keyString = String.valueOf(key.get(0));
                } else {
                    keyString = String.join(",", key.stream().map(String::valueOf).toArray(String[]::new));
                }
                for (Integer match : matches.get(key)) {
                    expected.add(keyString + ":" + match);
                }
            }
            for (int i = 0; i < numRows; i++) {
                Map<String, Object> lookupDoc = new HashMap<>();
                for (int f = 0; f < numFields; f++) {
                    lookupDoc.put("key" + f, lookupData[f][i]);
                }
                lookupDoc.put("l", i);
                docs.add(client().prepareIndex("lookup").setSource(lookupDoc));
            }
            Collections.sort(expected);
            indexRandom(true, true, docs);
        }
    }

    private void runLookup(List<DataType> keyTypes, PopulateIndices populateIndices) throws IOException {
        String[] fieldMappers = new String[keyTypes.size() * 2];
        for (int i = 0; i < keyTypes.size(); i++) {
            fieldMappers[2 * i] = "key" + i;
            fieldMappers[2 * i + 1] = "type=" + keyTypes.get(i).esType();
        }
        client().admin()
            .indices()
            .prepareCreate("source")
            .setSettings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1))
            .setMapping(fieldMappers)
            .get();

        Settings.Builder lookupSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "lookup")
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1);

        String[] lookupMappers = new String[keyTypes.size() * 2 + 2];
        int lookupMappersCounter = 0;
        for (; lookupMappersCounter < keyTypes.size(); lookupMappersCounter++) {
            lookupMappers[2 * lookupMappersCounter] = "key" + lookupMappersCounter;
            lookupMappers[2 * lookupMappersCounter + 1] = "type=" + keyTypes.get(lookupMappersCounter).esType();
        }
        lookupMappers[2 * lookupMappersCounter] = "l";
        lookupMappers[2 * lookupMappersCounter + 1] = "type=long";
        client().admin().indices().prepareCreate("lookup").setSettings(lookupSettings).setMapping(lookupMappers).get();

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
            List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = new ArrayList<>();
            for (int i = 0; i < keyTypes.size(); i++) {
                final int idx = i;
                fieldInfos.add(
                    new ValuesSourceReaderOperator.FieldInfo(
                        "key" + idx,
                        PlannerUtils.toElementType(keyTypes.get(idx)),
                        shard -> searchContext.getSearchExecutionContext().getFieldType("key" + idx).blockLoader(blContext())
                    )
                );
            }
            ValuesSourceReaderOperator.Factory reader = new ValuesSourceReaderOperator.Factory(
                PhysicalSettings.VALUES_LOADING_JUMBO_SIZE.getDefault(Settings.EMPTY),
                fieldInfos,
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
            List<MatchConfig> matchFields = new ArrayList<>();
            for (int i = 0; i < keyTypes.size(); i++) {
                matchFields.add(new MatchConfig(new FieldAttribute.FieldName("key" + i), i + 1, keyTypes.get(i)));
            }
            LookupFromIndexOperator.Factory lookup = new LookupFromIndexOperator.Factory(
                matchFields,
                "test",
                parentTask,
                QueryPragmas.ENRICH_MAX_WORKERS.get(Settings.EMPTY),
                ctx -> internalCluster().getInstance(TransportEsqlQueryAction.class, finalNodeWithShard).getLookupFromIndexService(),
                "lookup",
                "lookup",
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
                            List<Block> keyBlocks = new ArrayList<>();
                            for (int i = 0; i < keyTypes.size(); i++) {
                                keyBlocks.add(page.getBlock(i + 1));
                            }
                            LongVector loadedBlock = page.<LongBlock>getBlock(keyTypes.size() + 1).asVector();
                            for (int p = 0; p < page.getPositionCount(); p++) {
                                StringBuilder result = new StringBuilder();
                                for (int j = 0; j < keyBlocks.size(); j++) {
                                    List<Object> key = BlockTestUtils.valuesAtPositions(keyBlocks.get(j), p, p + 1).get(0);
                                    assertThat(key, hasSize(1));
                                    Object keyValue = key.get(0);
                                    if (keyValue instanceof BytesRef b) {
                                        keyValue = b.utf8ToString();
                                    }
                                    result.append(keyValue);
                                    if (j < keyBlocks.size() - 1) {
                                        result.append(",");
                                    }

                                }
                                result.append(":" + loadedBlock.getLong(p));
                                results.add(result.toString());
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
