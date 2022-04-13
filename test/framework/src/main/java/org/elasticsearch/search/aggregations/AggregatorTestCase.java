/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.AssertingIndexSearcher;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.FieldAliasMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.MultiValueAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationContext.ProductionAggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.aggregations.timeseries.TimeSeriesIndexSearcher;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesPhase;
import org.elasticsearch.search.fetch.subphase.FetchSourcePhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link Aggregator} implementations.
 * Provides helpers for constructing and searching an {@link Aggregator} implementation based on a provided
 * {@link AggregationBuilder} instance.
 */
public abstract class AggregatorTestCase extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;
    private List<AggregationContext> releasables = new ArrayList<>();
    protected ValuesSourceRegistry valuesSourceRegistry;
    private AnalysisModule analysisModule;

    // A list of field types that should not be tested, or are not currently supported
    private static final List<String> TYPE_TEST_BLACKLIST = List.of(
        ObjectMapper.CONTENT_TYPE, // Cannot aggregate objects
        GeoShapeFieldMapper.CONTENT_TYPE, // Cannot aggregate geoshapes (yet)

        NestedObjectMapper.CONTENT_TYPE, // TODO support for nested
        CompletionFieldMapper.CONTENT_TYPE, // TODO support completion
        FieldAliasMapper.CONTENT_TYPE // TODO support alias
    );

    @Before
    public final void initPlugins() {
        List<SearchPlugin> plugins = new ArrayList<>(getSearchPlugins());
        plugins.add(new AggCardinalityUpperBoundPlugin());
        SearchModule searchModule = new SearchModule(Settings.EMPTY, plugins);
        valuesSourceRegistry = searchModule.getValuesSourceRegistry();
        namedWriteableRegistry = new NamedWriteableRegistry(
            Stream.concat(
                searchModule.getNamedWriteables().stream(),
                plugins.stream().flatMap(p -> p instanceof Plugin ? ((Plugin) p).getNamedWriteables().stream() : Stream.empty())
            ).collect(toList())
        );
    }

    @Before
    public void initAnalysisRegistry() throws Exception {
        analysisModule = createAnalysisModule();
    }

    /**
     * @return a new analysis module. Tests that require a fully constructed analysis module (used to create an analysis registry)
     *         should override this method
     */
    protected AnalysisModule createAnalysisModule() throws Exception {
        return null;
    }

    /**
     * Test cases should override this if they have plugins that need to be loaded, e.g. the plugins their aggregators are in.
     */
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of();
    }

    protected <A extends Aggregator> A createAggregator(
        AggregationBuilder aggregationBuilder,
        IndexSearcher searcher,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return createAggregator(aggregationBuilder, createAggregationContext(searcher, new MatchAllDocsQuery(), fieldTypes));
    }

    protected <A extends Aggregator> A createAggregator(AggregationBuilder builder, AggregationContext context) throws IOException {
        QueryRewriteContext rewriteContext = new QueryRewriteContext(
            parserConfig(),
            new NamedWriteableRegistry(List.of()),
            null,
            context::nowInMillis
        );
        @SuppressWarnings("unchecked")
        A aggregator = (A) Rewriteable.rewrite(builder, rewriteContext, true).build(context, null).create(null, CardinalityUpperBound.ONE);
        return aggregator;
    }

    /**
     * Create a {@linkplain AggregationContext} for testing an {@link Aggregator}.
     * While {@linkplain AggregationContext} is {@link Releasable} the caller is
     * not responsible for releasing it. Instead, it is released automatically in
     * in {@link #cleanupReleasables()}.
     */
    protected AggregationContext createAggregationContext(IndexSearcher indexSearcher, Query query, MappedFieldType... fieldTypes)
        throws IOException {
        return createAggregationContext(
            indexSearcher,
            createIndexSettings(),
            query,
            new NoneCircuitBreakerService(),
            AggregationBuilder.DEFAULT_PREALLOCATION * 5, // We don't know how many bytes to preallocate so we grab a hand full
            DEFAULT_MAX_BUCKETS,
            false,
            fieldTypes
        );
    }

    /**
     * Create a {@linkplain AggregationContext} for testing an {@link Aggregator}.
     * While {@linkplain AggregationContext} is {@link Releasable} the caller is
     * not responsible for releasing it. Instead, it is released automatically in
     * in {@link #cleanupReleasables()}.
     */
    protected AggregationContext createAggregationContext(
        IndexSearcher indexSearcher,
        IndexSettings indexSettings,
        Query query,
        CircuitBreakerService breakerService,
        long bytesToPreallocate,
        int maxBucket,
        boolean isInSortOrderExecutionRequired,
        MappedFieldType... fieldTypes
    ) throws IOException {
        MappingLookup mappingLookup = MappingLookup.fromMappers(
            Mapping.EMPTY,
            Arrays.stream(fieldTypes).map(this::buildMockFieldMapper).collect(toList()),
            objectMappers(),
            // Alias all fields to <name>-alias to test aliases
            Arrays.stream(fieldTypes)
                .map(ft -> new FieldAliasMapper(ft.name() + "-alias", ft.name() + "-alias", ft.name()))
                .collect(toList())
        );

        TriFunction<MappedFieldType, String, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataBuilder = (
            fieldType,
            s,
            searchLookup) -> fieldType.fielddataBuilder(indexSettings.getIndex().getName(), searchLookup)
                .build(new IndexFieldDataCache.None(), breakerService);
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetFilterCache.Listener() {
            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {}

            @Override
            public void onCache(ShardId shardId, Accountable accountable) {}
        });
        SearchExecutionContext searchExecutionContext = new SearchExecutionContext(
            0,
            -1,
            indexSettings,
            bitsetFilterCache,
            fieldDataBuilder,
            null,
            mappingLookup,
            null,
            getMockScriptService(),
            parserConfig(),
            writableRegistry(),
            null,
            indexSearcher,
            System::currentTimeMillis,
            null,
            null,
            () -> true,
            valuesSourceRegistry,
            emptyMap()
        );

        MultiBucketConsumer consumer = new MultiBucketConsumer(maxBucket, breakerService.getBreaker(CircuitBreaker.REQUEST));
        AggregationContext context = new ProductionAggregationContext(
            Optional.ofNullable(analysisModule).map(AnalysisModule::getAnalysisRegistry).orElse(null),
            searchExecutionContext,
            new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService),
            bytesToPreallocate,
            () -> query,
            null,
            consumer,
            () -> buildSubSearchContext(indexSettings, searchExecutionContext, bitsetFilterCache),
            bitsetFilterCache,
            randomInt(),
            () -> 0L,
            () -> false,
            q -> q,
            true,
            isInSortOrderExecutionRequired
        );
        releasables.add(context);
        return context;
    }

    /**
     * Build a {@link FieldMapper} to create the {@link MappingLookup} used for the aggs.
     * {@code protected} so subclasses can have it.
     */
    protected FieldMapper buildMockFieldMapper(MappedFieldType ft) {
        return new MockFieldMapper(ft);
    }

    /**
     * {@link ObjectMapper}s to add to the lookup. By default we don't need
     * any {@link ObjectMapper}s but testing nested objects will require adding some.
     */
    protected List<ObjectMapper> objectMappers() {
        return List.of();
    }

    /**
     * Build a {@link SubSearchContext}s to power {@code top_hits}.
     */
    private SubSearchContext buildSubSearchContext(
        IndexSettings indexSettings,
        SearchExecutionContext searchExecutionContext,
        BitsetFilterCache bitsetFilterCache
    ) {
        SearchContext ctx = mock(SearchContext.class);
        QueryCache queryCache = new DisabledQueryCache(indexSettings);
        QueryCachingPolicy queryCachingPolicy = new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {}

            @Override
            public boolean shouldCache(Query query) {
                // never cache a query
                return false;
            }
        };
        try {
            when(ctx.searcher()).thenReturn(
                new ContextIndexSearcher(
                    searchExecutionContext.searcher().getIndexReader(),
                    searchExecutionContext.searcher().getSimilarity(),
                    queryCache,
                    queryCachingPolicy,
                    false
                )
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        when(ctx.fetchPhase()).thenReturn(new FetchPhase(Arrays.asList(new FetchSourcePhase(), new FetchDocValuesPhase())));

        /*
         * Use a QueryShardContext that doesn't contain nested documents so we
         * don't try to fetch them which would require mocking a whole menagerie
         * of stuff.
         */
        SearchExecutionContext subContext = spy(searchExecutionContext);
        MappingLookup disableNestedLookup = MappingLookup.fromMappers(Mapping.EMPTY, Set.of(), Set.of(), Set.of());
        doReturn(new NestedDocuments(disableNestedLookup, bitsetFilterCache::getBitSetProducer)).when(subContext).getNestedDocuments();
        when(ctx.getSearchExecutionContext()).thenReturn(subContext);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(ctx.indexShard()).thenReturn(indexShard);
        return new SubSearchContext(ctx);
    }

    protected IndexSettings createIndexSettings() {
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            Settings.EMPTY
        );
    }

    /**
     * Sub-tests that need scripting can override this method to provide a script service and pre-baked scripts
     */
    protected ScriptService getMockScriptService() {
        return null;
    }

    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return searchAndReduce(createIndexSettings(), searcher, query, builder, DEFAULT_MAX_BUCKETS, fieldTypes);
    }

    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSettings indexSettings,
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return searchAndReduce(indexSettings, searcher, query, builder, DEFAULT_MAX_BUCKETS, fieldTypes);
    }

    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        int maxBucket,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return searchAndReduce(createIndexSettings(), searcher, query, builder, maxBucket, fieldTypes);
    }

    /**
     * Collects all documents that match the provided query {@link Query} and
     * returns the reduced {@link InternalAggregation}.
     * <p>
     * Half the time it aggregates each leaf individually and reduces all
     * results together. The other half the time it aggregates across the entire
     * index at once and runs a final reduction on the single resulting agg.
     */
    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSettings indexSettings,
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        int maxBucket,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return searchAndReduce(indexSettings, searcher, query, builder, maxBucket, randomBoolean(), fieldTypes);
    }

    /**
     * Collects all documents that match the provided query {@link Query} and
     * returns the reduced {@link InternalAggregation}.
     *
     * It runs the aggregation as well using a circuit breaker that randomly throws {@link CircuitBreakingException}
     * in order to mak sure the implementation does not leak.
     *
     * @param splitLeavesIntoSeparateAggregators If true this creates a new {@link Aggregator}
     *          for each leaf as though it were a separate index. If false this aggregates
     *          all leaves together, like we do in production.
     */
    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSettings indexSettings,
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        int maxBucket,
        boolean splitLeavesIntoSeparateAggregators,
        MappedFieldType... fieldTypes
    ) throws IOException {
        // First run it to find circuit breaker leaks on the aggregator
        CircuitBreakerService crankyService = new CrankyCircuitBreakerService();
        for (int i = 0; i < 5; i++) {
            try {
                searchAndReduce(
                    indexSettings,
                    searcher,
                    query,
                    builder,
                    maxBucket,
                    splitLeavesIntoSeparateAggregators,
                    crankyService,
                    fieldTypes
                );
            } catch (CircuitBreakingException e) {
                // expected
            } catch (IOException e) {
                throw e;
            }
        }
        // Second run it to the end
        CircuitBreakerService breakerService = new NoneCircuitBreakerService();
        return searchAndReduce(
            indexSettings,
            searcher,
            query,
            builder,
            maxBucket,
            splitLeavesIntoSeparateAggregators,
            breakerService,
            fieldTypes
        );
    }

    @SuppressWarnings("unchecked")
    private <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSettings indexSettings,
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        int maxBucket,
        boolean splitLeavesIntoSeparateAggregators,
        CircuitBreakerService breakerService,
        MappedFieldType... fieldTypes
    ) throws IOException {
        final IndexReaderContext ctx = searcher.getTopReaderContext();
        final PipelineTree pipelines = builder.buildPipelineTree();
        List<InternalAggregation> aggs = new ArrayList<>();
        Query rewritten = searcher.rewrite(query);

        AggregationContext context = createAggregationContext(
            searcher,
            indexSettings,
            query,
            breakerService,
            randomBoolean() ? 0 : builder.bytesToPreallocate(),
            maxBucket,
            builder.isInSortOrderExecutionRequired(),
            fieldTypes
        );
        C root = createAggregator(builder, context);

        if (splitLeavesIntoSeparateAggregators
            && searcher.getIndexReader().leaves().size() > 0
            && context.isInSortOrderExecutionRequired() == false) {
            assertThat(ctx, instanceOf(CompositeReaderContext.class));
            final CompositeReaderContext compCTX = (CompositeReaderContext) ctx;
            final int size = compCTX.leaves().size();
            final ShardSearcher[] subSearchers = new ShardSearcher[size];
            for (int searcherIDX = 0; searcherIDX < subSearchers.length; searcherIDX++) {
                final LeafReaderContext leave = compCTX.leaves().get(searcherIDX);
                subSearchers[searcherIDX] = new ShardSearcher(leave, compCTX);
            }
            for (ShardSearcher subSearcher : subSearchers) {
                C a = createAggregator(builder, context);
                a.preCollection();
                if (context.isInSortOrderExecutionRequired()) {
                    new TimeSeriesIndexSearcher(subSearcher, List.of()).search(rewritten, a);
                } else {
                    Weight weight = subSearcher.createWeight(rewritten, ScoreMode.COMPLETE, 1f);
                    subSearcher.search(weight, a);
                }
                a.postCollection();
                aggs.add(a.buildTopLevel());
            }
        } else {
            root.preCollection();
            if (context.isInSortOrderExecutionRequired()) {
                new TimeSeriesIndexSearcher(searcher, List.of()).search(rewritten, MultiBucketCollector.wrap(true, List.of(root)));
            } else {
                searcher.search(rewritten, MultiBucketCollector.wrap(true, List.of(root)));
            }
            root.postCollection();
            aggs.add(root.buildTopLevel());
        }
        assertRoundTrip(aggs);
        if (randomBoolean() && aggs.size() > 1) {
            // sometimes do an incremental reduce
            int toReduceSize = aggs.size();
            Collections.shuffle(aggs, random());
            int r = randomIntBetween(1, toReduceSize);
            List<InternalAggregation> toReduce = aggs.subList(0, r);
            AggregationReduceContext reduceContext = new AggregationReduceContext.ForPartial(
                context.bigArrays(),
                getMockScriptService(),
                () -> false,
                builder
            );
            A reduced = (A) aggs.get(0).reduce(toReduce, reduceContext);
            aggs = new ArrayList<>(aggs.subList(r, toReduceSize));
            aggs.add(reduced);
            assertRoundTrip(aggs);
        }

        // now do the final reduce
        MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumer(
            maxBucket,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            context.bigArrays(),
            getMockScriptService(),
            () -> false,
            builder,
            reduceBucketConsumer,
            pipelines
        );

        @SuppressWarnings("unchecked")
        A internalAgg = (A) aggs.get(0).reduce(aggs, reduceContext);
        assertRoundTrip(internalAgg);

        // materialize any parent pipelines
        internalAgg = (A) internalAgg.reducePipelines(internalAgg, reduceContext, pipelines);

        // materialize any sibling pipelines at top level
        for (PipelineAggregator pipelineAggregator : pipelines.aggregators()) {
            internalAgg = (A) pipelineAggregator.reduce(internalAgg, reduceContext);
        }
        doAssertReducedMultiBucketConsumer(internalAgg, reduceBucketConsumer);
        assertRoundTrip(internalAgg);

        if (builder instanceof ValuesSourceAggregationBuilder.MetricsAggregationBuilder<?, ?>) {
            verifyMetricNames((ValuesSourceAggregationBuilder.MetricsAggregationBuilder<?, ?>) builder, internalAgg);
        }
        return internalAgg;
    }

    protected void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        InternalAggregationTestCase.assertMultiBucketConsumer(agg, bucketConsumer);
    }

    protected <T extends AggregationBuilder, V extends InternalAggregation> void testCase(
        T aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<V> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        boolean timeSeries = aggregationBuilder.isInSortOrderExecutionRequired();
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random()));
            if (timeSeries) {
                Sort sort = new Sort(
                    new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, false),
                    new SortedNumericSortField(DataStreamTimestampFieldMapper.DEFAULT_PATH, SortField.Type.LONG, true)
                );
                config.setIndexSort(sort);
            }
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, config);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader indexReader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                V agg = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldTypes);
                verify.accept(agg);

                verifyOutputFieldNames(aggregationBuilder, agg);
            }
        }
    }

    protected void withIndex(
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        CheckedConsumer<IndexSearcher, IOException> consume
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            buildIndex.accept(iw);
            iw.close();
            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader indexReader = wrapDirectoryReader(unwrapped)) {
                consume.accept(newIndexSearcher(indexReader));
            }
        }
    }

    protected void withNonMergingIndex(
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        CheckedConsumer<IndexSearcher, IOException> consume
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(
                random(),
                directory,
                LuceneTestCase.newIndexWriterConfig(random(), new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            );
            buildIndex.accept(iw);
            iw.close();
            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader indexReader = wrapDirectoryReader(unwrapped)) {
                consume.accept(newIndexSearcher(indexReader));
            }
        }
    }

    /**
     * Execute and aggregation and collect its {@link Aggregator#collectDebugInfo debug}
     * information. Unlike {@link #testCase} this doesn't randomly create an
     * {@link Aggregator} per leaf and perform partial reductions. It always
     * creates a single {@link Aggregator} so we can get consistent debug info.
     */
    protected <R extends InternalAggregation> void debugTestCase(
        AggregationBuilder builder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        TriConsumer<R, Class<? extends Aggregator>, Map<String, Map<String, Object>>> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        withIndex(buildIndex, searcher -> debugTestCase(builder, query, searcher, verify, fieldTypes));
    }

    /**
     * Execute and aggregation and collect its {@link Aggregator#collectDebugInfo debug}
     * information. Unlike {@link #testCase} this doesn't randomly create an
     * {@link Aggregator} per leaf and perform partial reductions. It always
     * creates a single {@link Aggregator} so we can get consistent debug info.
     */
    protected <R extends InternalAggregation> void debugTestCase(
        AggregationBuilder builder,
        Query query,
        IndexSearcher searcher,
        TriConsumer<R, Class<? extends Aggregator>, Map<String, Map<String, Object>>> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        // Don't use searchAndReduce because we only want a single aggregator.
        CircuitBreakerService breakerService = new NoneCircuitBreakerService();
        AggregationContext context = createAggregationContext(
            searcher,
            createIndexSettings(),
            searcher.rewrite(query),
            breakerService,
            builder.bytesToPreallocate(),
            DEFAULT_MAX_BUCKETS,
            builder.isInSortOrderExecutionRequired(),
            fieldTypes
        );
        Aggregator aggregator = createAggregator(builder, context);
        aggregator.preCollection();
        searcher.search(context.query(), aggregator);
        aggregator.postCollection();
        InternalAggregation r = aggregator.buildTopLevel();
        r = r.reduce(
            List.of(r),
            new AggregationReduceContext.ForFinal(
                context.bigArrays(),
                getMockScriptService(),
                () -> false,
                builder,
                context.multiBucketConsumer(),
                builder.buildPipelineTree()
            )
        );
        @SuppressWarnings("unchecked") // We'll get a cast error in the test if we're wrong here and that is ok
        R result = (R) r;
        assertRoundTrip(result);
        Map<String, Map<String, Object>> debug = new HashMap<>();
        collectDebugInfo("", aggregator, debug);
        verify.apply(result, aggregator.getClass(), debug);
        verifyOutputFieldNames(builder, result);
    }

    private void collectDebugInfo(String prefix, Aggregator aggregator, Map<String, Map<String, Object>> allDebug) {
        Map<String, Object> debug = new HashMap<>();
        aggregator.collectDebugInfo((key, value) -> {
            Object old = debug.put(key, value);
            assertNull("debug info duplicate key [" + key + "] was [" + old + "] is [" + value + "]", old);
        });
        allDebug.put(prefix + aggregator.name(), debug);
        for (Aggregator sub : aggregator.subAggregators()) {
            collectDebugInfo(aggregator.name() + ".", sub, allDebug);
        }
    }

    protected void withAggregator(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        CheckedBiConsumer<IndexSearcher, Aggregator, IOException> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader indexReader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(indexReader);
                AggregationContext context = createAggregationContext(searcher, query, fieldTypes);
                verify.accept(searcher, createAggregator(aggregationBuilder, context));
            }
        }
    }

    private void verifyMetricNames(
        ValuesSourceAggregationBuilder.MetricsAggregationBuilder<?, ?> aggregationBuilder,
        InternalAggregation agg
    ) {
        for (String metric : aggregationBuilder.metricNames()) {
            try {
                agg.getProperty(List.of(metric));
            } catch (IllegalArgumentException ex) {
                fail("Cannot access metric [" + metric + "]");
            }
        }
    }

    protected <T extends AggregationBuilder, V extends InternalAggregation> void verifyOutputFieldNames(T aggregationBuilder, V agg)
        throws IOException {
        if (aggregationBuilder.getOutputFieldNames().isEmpty()) {
            // aggregation does not support output field names yet
            return;
        }

        Set<String> valueNames = new HashSet<>();

        if (agg instanceof NumericMetricsAggregation.MultiValue multiValueAgg) {
            for (String name : multiValueAgg.valueNames()) {
                valueNames.add(name);
            }
        } else if (agg instanceof MultiValueAggregation multiValueAgg) {
            for (String name : multiValueAgg.valueNames()) {
                valueNames.add(name);
            }
        } else {
            assert false : "only multi value aggs are supported";
        }

        assertEquals(aggregationBuilder.getOutputFieldNames().get(), valueNames);
    }

    /**
     * Override to wrap the {@linkplain DirectoryReader} for aggs like
     * {@link NestedAggregationBuilder}.
     */
    protected IndexReader wrapDirectoryReader(DirectoryReader reader) throws IOException {
        return reader;
    }

    private static class ShardSearcher extends IndexSearcher {
        private final List<LeafReaderContext> ctx;

        ShardSearcher(LeafReaderContext ctx, IndexReaderContext parent) {
            super(parent);
            this.ctx = Collections.singletonList(ctx);
        }

        public void search(Weight weight, Collector collector) throws IOException {
            search(ctx, weight, collector);
        }

        @Override
        public String toString() {
            return "ShardSearcher(" + ctx.get(0) + ")";
        }
    }

    protected static DirectoryReader wrapInMockESDirectoryReader(DirectoryReader directoryReader) throws IOException {
        return ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId(new Index("_index", "_na_"), 0));
    }

    /**
     * Added to randomly run with more assertions on the index searcher level,
     * like {@link org.apache.lucene.tests.util.LuceneTestCase#newSearcher(IndexReader)}, which can't be used because it also
     * wraps in the IndexSearcher's IndexReader with other implementations that we can't handle. (e.g. ParallelCompositeReader)
     */
    protected static IndexSearcher newIndexSearcher(IndexReader indexReader) {
        if (randomBoolean()) {
            // this executes basic query checks and asserts that weights are normalized only once etc.
            return new AssertingIndexSearcher(random(), indexReader);
        } else {
            return new IndexSearcher(indexReader);
        }
    }

    /**
     * Added to randomly run with more assertions on the index reader level,
     * like {@link org.apache.lucene.tests.util.LuceneTestCase#wrapReader(IndexReader)}, which can't be used because it also
     * wraps in the IndexReader with other implementations that we can't handle. (e.g. ParallelCompositeReader)
     */
    protected static IndexReader maybeWrapReaderEs(DirectoryReader reader) throws IOException {
        if (randomBoolean()) {
            return new AssertingDirectoryReader(reader);
        } else {
            return reader;
        }
    }

    /**
     * Implementors should return a list of {@link ValuesSourceType} that the aggregator supports.
     * This is used to test the matrix of supported/unsupported field types against the aggregator
     * and verify it works (or doesn't) as expected.
     *
     * If this method is implemented, {@link AggregatorTestCase#createAggBuilderForTypeTest(MappedFieldType, String)}
     * should be implemented as well.
     *
     * @return list of supported ValuesSourceTypes
     */
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        // If aggs don't override this method, an empty list allows the test to be skipped.
        // Once all aggs implement this method we should make it abstract and not allow skipping.
        return Collections.emptyList();
    }

    /**
     * This method is invoked each time a field type is tested in {@link AggregatorTestCase#testSupportedFieldTypes()}.
     * The field type and name are provided, and the implementor is expected to return an AggBuilder accordingly.
     * The AggBuilder should be returned even if the aggregation does not support the field type, because
     * the test will check if an exception is thrown in that case.
     *
     * The list of supported types are provided by {@link AggregatorTestCase#getSupportedValuesSourceTypes()},
     * which must also be implemented.
     *
     * @param fieldType the type of the field that will be tested
     * @param fieldName the name of the field that will be test
     * @return an aggregation builder to test against the field
     */
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        throw new UnsupportedOperationException(
            "If getSupportedValuesSourceTypes() is implemented, " + "createAggBuilderForTypeTest() must be implemented as well."
        );
    }

    /**
     * A method that allows implementors to specifically blacklist particular field types (based on their content_name).
     * This is needed in some areas where the ValuesSourceType is not granular enough, for example integer values
     * vs floating points, or `keyword` bytes vs `binary` bytes (which are not searchable)
     *
     * This is a blacklist instead of a whitelist because there are vastly more field types than ValuesSourceTypes,
     * and it's expected that these unsupported cases are exceptional rather than common
     */
    protected List<String> unsupportedMappedFieldTypes() {
        return Collections.emptyList();
    }

    /**
     * This test will validate that an aggregator succeeds or fails to run against all the field types
     * that are registered in {@link IndicesModule} (e.g. all the core field types).  An aggregator
     * is provided by the implementor class, and it is executed against each field type in turn.  If
     * an exception is thrown when the field is supported, that will fail the test.  Similarly, if
     * an exception _is not_ thrown when a field is unsupported, that will also fail the test.
     *
     * Exception types/messages are not currently checked, just presence/absence of an exception.
     */
    public void testSupportedFieldTypes() throws IOException {
        MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
        String fieldName = "typeTestFieldName";
        List<ValuesSourceType> supportedVSTypes = getSupportedValuesSourceTypes();
        List<String> unsupportedMappedFieldTypes = unsupportedMappedFieldTypes();

        if (supportedVSTypes.isEmpty()) {
            // If the test says it doesn't support any VStypes, it has not been converted yet so skip
            return;
        }

        for (Map.Entry<String, Mapper.TypeParser> mappedType : mapperRegistry.getMapperParsers().entrySet()) {

            // Some field types should not be tested, or require more work and are not ready yet
            if (TYPE_TEST_BLACKLIST.contains(mappedType.getKey())) {
                continue;
            }

            Map<String, Object> source = new HashMap<>();
            source.put("type", mappedType.getKey());

            // Text is the only field that doesn't support DVs, instead FD
            if (mappedType.getKey().equals(TextFieldMapper.CONTENT_TYPE) == false) {
                source.put("doc_values", "true");
            }

            IndexSettings indexSettings = createIndexSettings();
            Mapper.Builder builder = mappedType.getValue().parse(fieldName, source, new MockParserContext(indexSettings));
            FieldMapper mapper = (FieldMapper) builder.build(MapperBuilderContext.ROOT);

            MappedFieldType fieldType = mapper.fieldType();

            // Non-aggregatable fields are not testable (they will throw an error on all aggs anyway), so skip
            if (fieldType.isAggregatable() == false) {
                continue;
            }

            try (Directory directory = newDirectory()) {
                RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
                writeTestDoc(fieldType, fieldName, indexWriter);
                indexWriter.close();

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    AggregationBuilder aggregationBuilder = createAggBuilderForTypeTest(fieldType, fieldName);

                    ValuesSourceType vst = fieldToVST(fieldType);
                    // TODO in the future we can make this more explicit with expectThrows(), when the exceptions are standardized
                    AssertionError failure = null;
                    try {
                        InternalAggregation internalAggregation = searchAndReduce(
                            indexSearcher,
                            new MatchAllDocsQuery(),
                            aggregationBuilder,
                            fieldType
                        );
                        // We should make sure if the builder says it supports sampling, that the internal aggregations returned override
                        // finalizeSampling
                        if (aggregationBuilder.supportsSampling()) {
                            SamplingContext randomSamplingContext = new SamplingContext(randomDoubleBetween(1e-8, 0.1, false), randomInt());
                            InternalAggregation sampledResult = internalAggregation.finalizeSampling(randomSamplingContext);
                            assertThat(sampledResult.getClass(), equalTo(internalAggregation.getClass()));
                        }
                        if (supportedVSTypes.contains(vst) == false || unsupportedMappedFieldTypes.contains(fieldType.typeName())) {
                            failure = new AssertionError(
                                "Aggregator ["
                                    + aggregationBuilder.getType()
                                    + "] should not support field type ["
                                    + fieldType.typeName()
                                    + "] but executing against the field did not throw an exception"
                            );
                        }
                    } catch (Exception | AssertionError e) {
                        if (supportedVSTypes.contains(vst) && unsupportedMappedFieldTypes.contains(fieldType.typeName()) == false) {
                            failure = new AssertionError(
                                "Aggregator ["
                                    + aggregationBuilder.getType()
                                    + "] supports field type ["
                                    + fieldType.typeName()
                                    + "] but executing against the field threw an exception: ["
                                    + e.getMessage()
                                    + "]",
                                e
                            );
                        }
                    }
                    if (failure != null) {
                        throw failure;
                    }
                }
            }
        }
    }

    private ValuesSourceType fieldToVST(MappedFieldType fieldType) {
        return fieldType.fielddataBuilder("", () -> { throw new UnsupportedOperationException(); }).build(null, null).getValuesSourceType();
    }

    /**
     * Helper method to write a single document with a single value specific to the requested fieldType.
     *
     * Throws an exception if it encounters an unknown field type, to prevent new ones from sneaking in without
     * being tested.
     */
    private void writeTestDoc(MappedFieldType fieldType, String fieldName, RandomIndexWriter iw) throws IOException {

        String typeName = fieldType.typeName();
        ValuesSourceType vst = fieldToVST(fieldType);
        Document doc = new Document();
        String json;

        if (vst.equals(CoreValuesSourceType.NUMERIC)) {
            long v;
            if (typeName.equals(NumberFieldMapper.NumberType.DOUBLE.typeName())) {
                double d = Math.abs(randomDouble());
                v = NumericUtils.doubleToSortableLong(d);
                json = "{ \"" + fieldName + "\" : \"" + d + "\" }";
            } else if (typeName.equals(NumberFieldMapper.NumberType.FLOAT.typeName())) {
                float f = Math.abs(randomFloat());
                v = NumericUtils.floatToSortableInt(f);
                json = "{ \"" + fieldName + "\" : \"" + f + "\" }";
            } else if (typeName.equals(NumberFieldMapper.NumberType.HALF_FLOAT.typeName())) {
                // Generate a random float that respects the limits of half float
                float f = Math.abs((randomFloat() * 2 - 1) * 65504);
                v = HalfFloatPoint.halfFloatToSortableShort(f);
                json = "{ \"" + fieldName + "\" : \"" + f + "\" }";
            } else {
                // smallest numeric is a byte so we select the smallest
                v = Math.abs(randomByte());
                json = "{ \"" + fieldName + "\" : \"" + v + "\" }";
            }
            doc.add(new SortedNumericDocValuesField(fieldName, v));

        } else if (vst.equals(CoreValuesSourceType.KEYWORD)) {
            if (typeName.equals(BinaryFieldMapper.CONTENT_TYPE)) {
                doc.add(new BinaryFieldMapper.CustomBinaryDocValuesField(fieldName, new BytesRef("a").bytes));
                json = "{ \"" + fieldName + "\" : \"a\" }";
            } else {
                doc.add(new SortedSetDocValuesField(fieldName, new BytesRef("a")));
                json = "{ \"" + fieldName + "\" : \"a\" }";
            }
        } else if (vst.equals(CoreValuesSourceType.DATE)) {
            // positive integer because date_nanos gets unhappy with large longs
            long v;
            v = Math.abs(randomInt());
            doc.add(new SortedNumericDocValuesField(fieldName, v));
            json = "{ \"" + fieldName + "\" : \"" + v + "\" }";
        } else if (vst.equals(CoreValuesSourceType.BOOLEAN)) {
            long v;
            v = randomBoolean() ? 0 : 1;
            doc.add(new SortedNumericDocValuesField(fieldName, v));
            json = "{ \"" + fieldName + "\" : \"" + (v == 0 ? "false" : "true") + "\" }";
        } else if (vst.equals(CoreValuesSourceType.IP)) {
            InetAddress ip = randomIp(randomBoolean());
            json = "{ \"" + fieldName + "\" : \"" + NetworkAddress.format(ip) + "\" }";
            doc.add(new SortedSetDocValuesField(fieldName, new BytesRef(InetAddressPoint.encode(ip))));
        } else if (vst.equals(CoreValuesSourceType.RANGE)) {
            Object start;
            Object end;
            RangeType rangeType;

            if (typeName.equals(RangeType.DOUBLE.typeName())) {
                start = randomDouble();
                end = RangeType.DOUBLE.nextUp(start);
                rangeType = RangeType.DOUBLE;
            } else if (typeName.equals(RangeType.FLOAT.typeName())) {
                start = randomFloat();
                end = RangeType.FLOAT.nextUp(start);
                rangeType = RangeType.DOUBLE;
            } else if (typeName.equals(RangeType.IP.typeName())) {
                boolean v4 = randomBoolean();
                start = randomIp(v4);
                end = RangeType.IP.nextUp(start);
                rangeType = RangeType.IP;
            } else if (typeName.equals(RangeType.LONG.typeName())) {
                start = randomLong();
                end = RangeType.LONG.nextUp(start);
                rangeType = RangeType.LONG;
            } else if (typeName.equals(RangeType.INTEGER.typeName())) {
                start = randomInt();
                end = RangeType.INTEGER.nextUp(start);
                rangeType = RangeType.INTEGER;
            } else if (typeName.equals(RangeType.DATE.typeName())) {
                start = randomNonNegativeLong();
                end = RangeType.DATE.nextUp(start);
                rangeType = RangeType.DATE;
            } else {
                throw new IllegalStateException("Unknown type of range [" + typeName + "]");
            }

            final RangeFieldMapper.Range range = new RangeFieldMapper.Range(rangeType, start, end, true, true);
            doc.add(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(Collections.singleton(range))));
            json = """
                { "%s" : { "gte" : "%s", "lte" : "%s" } }
                """.formatted(fieldName, start, end);
        } else if (vst.equals(CoreValuesSourceType.GEOPOINT)) {
            double lat = randomDouble();
            double lon = randomDouble();
            doc.add(new LatLonDocValuesField(fieldName, lat, lon));
            json = """
                { "%s" : "[%s,%s]" }""".formatted(fieldName, lon, lat);
        } else {
            throw new IllegalStateException("Unknown field type [" + typeName + "]");
        }

        doc.add(new StoredField("_source", new BytesRef(json)));
        iw.addDocument(doc);
    }

    private static class MockParserContext extends MappingParserContext {
        MockParserContext(IndexSettings indexSettings) {
            super(null, null, null, Version.CURRENT, null, null, ScriptCompiler.NONE, null, indexSettings, null);
        }

        @Override
        public Settings getSettings() {
            return Settings.EMPTY;
        }

        @Override
        public IndexAnalyzers getIndexAnalyzers() {
            NamedAnalyzer defaultAnalyzer = new NamedAnalyzer(
                AnalysisRegistry.DEFAULT_ANALYZER_NAME,
                AnalyzerScope.GLOBAL,
                new StandardAnalyzer()
            );
            return new IndexAnalyzers(Map.of(AnalysisRegistry.DEFAULT_ANALYZER_NAME, defaultAnalyzer), Map.of(), Map.of());
        }

    }

    @After
    public void cleanupReleasables() {
        Releasables.close(releasables);
        releasables.clear();
    }

    /**
     * Hook for checking things after all {@link Aggregator}s have been closed.
     */
    protected void afterClose() {}

    /**
     * Make a {@linkplain DateFieldMapper.DateFieldType} for a {@code date}.
     */
    protected DateFieldMapper.DateFieldType dateField(String name, DateFieldMapper.Resolution resolution) {
        return new DateFieldMapper.DateFieldType(name, resolution);
    }

    /**
     * Make a {@linkplain NumberFieldMapper.NumberFieldType} for a {@code double}.
     */
    protected NumberFieldMapper.NumberFieldType doubleField(String name) {
        return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.DOUBLE);
    }

    /**
     * Make a {@linkplain GeoPointFieldMapper.GeoPointFieldType} for a {@code geo_point}.
     */
    protected GeoPointFieldMapper.GeoPointFieldType geoPointField(String name) {
        return new GeoPointFieldMapper.GeoPointFieldType(name);
    }

    /**
     * Make a {@linkplain DateFieldMapper.DateFieldType} for a {@code date}.
     */
    protected KeywordFieldMapper.KeywordFieldType keywordField(String name) {
        return new KeywordFieldMapper.KeywordFieldType(name);
    }

    /**
     * Make a {@linkplain NumberFieldMapper.NumberFieldType} for a {@code long}.
     */
    protected NumberFieldMapper.NumberFieldType longField(String name) {
        return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
    }

    /**
     * Make a {@linkplain NumberFieldMapper.NumberFieldType} for a {@code range}.
     */
    protected RangeFieldMapper.RangeFieldType rangeField(String name, RangeType rangeType) {
        if (rangeType == RangeType.DATE) {
            return new RangeFieldMapper.RangeFieldType(name, RangeFieldMapper.Defaults.DATE_FORMATTER);
        }
        return new RangeFieldMapper.RangeFieldType(name, rangeType);
    }

    private void assertRoundTrip(List<InternalAggregation> result) throws IOException {
        for (InternalAggregation i : result) {
            assertRoundTrip(i);
        }
    }

    private void assertRoundTrip(InternalAggregation result) throws IOException {
        InternalAggregation roundTripped = copyNamedWriteable(result, writableRegistry(), InternalAggregation.class);
        assertThat(roundTripped, not(sameInstance(result)));
        assertThat(roundTripped, equalTo(result));
        assertThat(roundTripped.hashCode(), equalTo(result.hashCode()));
    }

    @Override
    protected final NamedWriteableRegistry writableRegistry() {
        return namedWriteableRegistry;
    }

    /**
     * Request an aggregation that returns the {@link CardinalityUpperBound}
     * that was passed to its ctor.
     */
    public static AggregationBuilder aggCardinalityUpperBound(String name) {
        return new AggCardinalityUpperBoundAggregationBuilder(name);
    }

    private static class AggCardinalityUpperBoundAggregationBuilder extends AbstractAggregationBuilder<
        AggCardinalityUpperBoundAggregationBuilder> {

        AggCardinalityUpperBoundAggregationBuilder(String name) {
            super(name);
        }

        @Override
        protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subfactoriesBuilder)
            throws IOException {
            return new AggregatorFactory(name, context, parent, subfactoriesBuilder, metadata) {
                @Override
                protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
                    throws IOException {
                    return new MetricsAggregator(name, context, parent, metadata) {
                        @Override
                        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
                            return LeafBucketCollector.NO_OP_COLLECTOR;
                        }

                        @Override
                        public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
                            return new InternalAggCardinalityUpperBound(name, cardinality, metadata);
                        }

                        @Override
                        public InternalAggregation buildEmptyAggregation() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.ONE;
        }

        @Override
        public String getType() {
            return InternalAggCardinalityUpperBound.NAME;
        }

        @Override
        protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();

        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_EMPTY;
        }
    }

    public static class InternalAggCardinalityUpperBound extends InternalAggregation {
        private static final String NAME = "ctor_cardinality_upper_bound";

        private final CardinalityUpperBound cardinality;

        protected InternalAggCardinalityUpperBound(String name, CardinalityUpperBound cardinality, Map<String, Object> metadata) {
            super(name, metadata);
            this.cardinality = cardinality;
        }

        public InternalAggCardinalityUpperBound(StreamInput in) throws IOException {
            super(in);
            this.cardinality = CardinalityUpperBound.ONE.multiply(in.readVInt());
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeVInt(cardinality.map(i -> i));
        }

        public CardinalityUpperBound cardinality() {
            return cardinality;
        }

        @Override
        public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
            aggregations.forEach(ia -> { assertThat(((InternalAggCardinalityUpperBound) ia).cardinality, equalTo(cardinality)); });
            return new InternalAggCardinalityUpperBound(name, cardinality, metadata);
        }

        @Override
        protected boolean mustReduceOnSingleInternalAgg() {
            return true;
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder.array("cardinality", cardinality);
        }

        @Override
        public Object getProperty(List<String> path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    private static class AggCardinalityUpperBoundPlugin implements SearchPlugin {
        @Override
        public List<AggregationSpec> getAggregations() {
            return singletonList(
                new AggregationSpec(
                    InternalAggCardinalityUpperBound.NAME,
                    in -> null,
                    (ContextParser<String, AggCardinalityUpperBoundAggregationBuilder>) (p, c) -> null
                ).addResultReader(InternalAggCardinalityUpperBound::new)
            );
        }
    }

    private static class CrankyCircuitBreakerService extends CircuitBreakerService {

        private final CircuitBreaker breaker = new CircuitBreaker() {
            @Override
            public void circuitBreak(String fieldName, long bytesNeeded) {

            }

            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                if (random().nextInt(20) == 0) {
                    throw new CircuitBreakingException("fake error", Durability.PERMANENT);
                }
            }

            @Override
            public void addWithoutBreaking(long bytes) {

            }

            @Override
            public long getUsed() {
                return 0;
            }

            @Override
            public long getLimit() {
                return 0;
            }

            @Override
            public double getOverhead() {
                return 0;
            }

            @Override
            public long getTrippedCount() {
                return 0;
            }

            @Override
            public String getName() {
                return CircuitBreaker.FIELDDATA;
            }

            @Override
            public Durability getDurability() {
                return null;
            }

            @Override
            public void setLimitAndOverhead(long limit, double overhead) {

            }
        };

        @Override
        public CircuitBreaker getBreaker(String name) {
            return breaker;
        }

        @Override
        public AllCircuitBreakerStats stats() {
            return new AllCircuitBreakerStats(new CircuitBreakerStats[] { stats(CircuitBreaker.FIELDDATA) });
        }

        @Override
        public CircuitBreakerStats stats(String name) {
            return new CircuitBreakerStats(CircuitBreaker.FIELDDATA, -1, -1, 0, 0);
        }
    }
}
