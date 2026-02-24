/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DummyBlockLoaderContext;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexOperator;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
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
import java.util.function.Predicate;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class LookupFromIndexIT extends AbstractEsqlIntegTestCase {
    public void testKeywordKey() throws IOException {
        runLookup(List.of(DataType.KEYWORD), new UsingSingleLookupTable(new Object[][] { new String[] { "aa", "bb", "cc", "dd" } }), null);
    }

    public void testJoinOnTwoKeys() throws IOException {
        runLookup(
            List.of(DataType.KEYWORD, DataType.LONG),
            new UsingSingleLookupTable(new Object[][] { new String[] { "aa", "bb", "cc", "dd" }, new Long[] { 12L, 33L, 1L, 42L } }),
            null
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
            ),
            null
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
            ),
            buildGreaterThanFilter(1L)
        );
    }

    public void testLongKey() throws IOException {
        runLookup(
            List.of(DataType.LONG),
            new UsingSingleLookupTable(new Object[][] { new Long[] { 12L, 33L, 1L } }),
            buildGreaterThanFilter(0L)
        );
    }

    /**
     * LOOKUP multiple results match.
     */
    public void testLookupIndexMultiResults() throws IOException {
        runLookup(
            List.of(DataType.KEYWORD),
            new UsingSingleLookupTable(new Object[][] { new String[] { "aa", "bb", "bb", "dd" } }),
            buildGreaterThanFilter(-1L)
        );
    }

    public void testJoinOnTwoKeysMultiResults() throws IOException {
        runLookup(
            List.of(DataType.KEYWORD, DataType.LONG),
            new UsingSingleLookupTable(new Object[][] { new String[] { "aa", "bb", "bb", "dd" }, new Long[] { 12L, 1L, 1L, 42L } }),
            null
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
            ),
            null
        );
    }

    interface PopulateIndices {
        void populate(int docCount, List<String> expected, Predicate<Integer> filter) throws IOException;
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
        public void populate(int docCount, List<String> expected, Predicate<Integer> filter) {
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
                List<Integer> filteredMatches = matches.get(key).stream().filter(filter).toList();
                if (filteredMatches.isEmpty()) {
                    expected.add(keyString + ":null");
                } else {
                    for (Integer match : filteredMatches) {
                        expected.add(keyString + ":" + match);
                    }
                }
            }
            for (int i = 0; i < numRows; i++) {
                Map<String, Object> lookupDoc = new HashMap<>();
                for (int f = 0; f < numFields; f++) {
                    lookupDoc.put("key" + f, lookupData[f][i]);
                    lookupDoc.put("rkey" + f, lookupData[f][i]);
                }
                lookupDoc.put("l", i);
                docs.add(client().prepareIndex("lookup").setSource(lookupDoc));
            }
            Collections.sort(expected);
            indexRandom(true, true, docs);
        }
    }

    private PhysicalPlan buildGreaterThanFilter(long value) {
        FieldAttribute filterAttribute = new FieldAttribute(
            Source.EMPTY,
            "l",
            new EsField("l", DataType.LONG, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Expression greaterThan = new GreaterThan(Source.EMPTY, filterAttribute, new Literal(Source.EMPTY, value, DataType.LONG));
        EsRelation esRelation = new EsRelation(Source.EMPTY, "test", IndexMode.LOOKUP, Map.of(), Map.of(), Map.of(), List.of());
        Filter filter = new Filter(Source.EMPTY, esRelation, greaterThan);
        return new FragmentExec(filter);
    }

    private void runLookup(List<DataType> keyTypes, PopulateIndices populateIndices, PhysicalPlan pushedDownFilter) throws IOException {
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

        String[] lookupMappers = new String[keyTypes.size() * 4 + 2];
        for (int i = 0; i < keyTypes.size(); i++) {
            lookupMappers[2 * i] = "key" + i;
            lookupMappers[2 * i + 1] = "type=" + keyTypes.get(i).esType();
        }
        for (int i = 0; i < keyTypes.size(); i++) {
            lookupMappers[2 * (keyTypes.size() + i)] = "rkey" + i;
            lookupMappers[2 * (keyTypes.size() + i) + 1] = "type=" + keyTypes.get(i).esType();
        }
        lookupMappers[keyTypes.size() * 4] = "l";
        lookupMappers[keyTypes.size() * 4 + 1] = "type=long";
        client().admin().indices().prepareCreate("lookup").setSettings(lookupSettings).setMapping(lookupMappers).get();

        client().admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForGreenStatus().get();

        Predicate<Integer> filterPredicate = l -> true;
        if (pushedDownFilter instanceof FragmentExec fragmentExec && fragmentExec.fragment() instanceof Filter filter) {
            filterPredicate = getPredicateFromFilter(filter);
        }

        int docCount = between(10, 1000);
        List<String> expected = new ArrayList<>(docCount);
        populateIndices.populate(docCount, expected, filterPredicate);

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
                new ShardSearchRequest(shardId, System.currentTimeMillis(), AliasFilter.EMPTY, null, SplitShardCountSummary.UNSET),
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
                new IndexedByShardIdFromSingleton<>(esqlContext),
                ctx -> List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())),
                DataPartitioning.SEGMENT,
                DataPartitioning.AutoStrategy.DEFAULT,
                1,
                10000,
                LuceneOperator.NO_LIMIT,
                false // no scoring
            );
            List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = new ArrayList<>();
            for (int i = 0; i < keyTypes.size(); i++) {
                final int idx = i;
                fieldInfos.add(
                    new ValuesSourceReaderOperator.FieldInfo(
                        "key" + idx,
                        PlannerUtils.toElementType(keyTypes.get(idx)),
                        false,
                        shard -> ValuesSourceReaderOperator.load(
                            searchContext.getSearchExecutionContext().getFieldType("key" + idx).blockLoader(blContext())
                        )
                    )
                );
            }
            ValuesSourceReaderOperator.Factory reader = new ValuesSourceReaderOperator.Factory(
                PlannerSettings.VALUES_LOADING_JUMBO_SIZE.getDefault(Settings.EMPTY),
                fieldInfos,
                new IndexedByShardIdFromSingleton<>(
                    new ValuesSourceReaderOperator.ShardContext(searchContext.getSearchExecutionContext().getIndexReader(), (paths) -> {
                        throw new IllegalStateException("can't load source here");
                    }, EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(Settings.EMPTY))
                ),
                true,
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
            boolean expressionJoin = EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled() ? randomBoolean() : false;
            List<MatchConfig> matchFields = new ArrayList<>();
            List<Expression> joinOnConditions = new ArrayList<>();
            if (expressionJoin) {
                for (int i = 0; i < keyTypes.size(); i++) {
                    FieldAttribute leftAttr = new FieldAttribute(
                        Source.EMPTY,
                        "key" + i,
                        new EsField("key" + i, keyTypes.get(0), Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
                    );
                    FieldAttribute rightAttr = new FieldAttribute(
                        Source.EMPTY,
                        "rkey" + i,
                        new EsField("rkey" + i, keyTypes.get(i), Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
                    );
                    joinOnConditions.add(new Equals(Source.EMPTY, leftAttr, rightAttr));
                    // randomly decide to apply the filter as additional join on filter instead of pushed down filter
                    boolean applyAsJoinOnCondition = EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
                        ? randomBoolean()
                        : false;
                    if (applyAsJoinOnCondition
                        && pushedDownFilter instanceof FragmentExec fragmentExec
                        && fragmentExec.fragment() instanceof Filter filter) {
                        joinOnConditions.add(filter.condition());
                        pushedDownFilter = null;
                    }
                }
            }
            // the matchFields are shared for both types of join
            for (int i = 0; i < keyTypes.size(); i++) {
                matchFields.add(new MatchConfig("key" + i, i + 1, keyTypes.get(i)));
            }
            LookupFromIndexOperator.Factory lookup = new LookupFromIndexOperator.Factory(
                matchFields,
                "test",
                parentTask,
                QueryPragmas.ENRICH_MAX_WORKERS.get(Settings.EMPTY),
                ctx -> internalCluster().getInstance(TransportEsqlQueryAction.class, finalNodeWithShard).getLookupFromIndexService(),
                "lookup",
                "lookup",
                List.of(
                    new FieldAttribute(
                        Source.EMPTY,
                        "l",
                        new EsField("l", DataType.LONG, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
                    )
                ),
                Source.EMPTY,
                pushedDownFilter,
                Predicates.combineAnd(joinOnConditions)
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
                            LongBlock loadedBlock = page.<LongBlock>getBlock(keyTypes.size() + 1);
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
                                if (loadedBlock.isNull(p)) {
                                    result.append(":null");
                                } else {
                                    result.append(":" + loadedBlock.getLong(loadedBlock.getFirstValueIndex(p)));
                                }
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

    private static Predicate<Integer> getPredicateFromFilter(Filter filter) {
        if (filter.condition() instanceof GreaterThan gt
            && gt.left() instanceof FieldAttribute fa
            && fa.name().equals("l")
            && gt.right() instanceof Literal lit) {
            long value = ((Number) lit.value()).longValue();
            return l -> l > value;
        } else {
            fail("Unsupported filter type in test baseline generation: " + filter);
        }
        return null;
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
        return new DriverContext(bigArrays(), BlockFactory.getInstance(breaker, bigArrays()), null);
    }

    public static void assertDriverContext(DriverContext driverContext) {
        assertTrue(driverContext.isFinished());
        assertThat(driverContext.getSnapshot().releasables(), empty());
    }

    private static MappedFieldType.BlockLoaderContext blContext() {
        return new DummyBlockLoaderContext("test_index") {
            @Override
            public IndexSettings indexSettings() {
                var imd = IndexMetadata.builder("test_index")
                    .settings(ESTestCase.indexSettings(IndexVersion.current(), 1, 1).put(Settings.EMPTY))
                    .build();
                return new IndexSettings(imd, Settings.EMPTY);
            }

            @Override
            public Set<String> sourcePaths(String name) {
                return Set.of(name);
            }
        };
    }
}
