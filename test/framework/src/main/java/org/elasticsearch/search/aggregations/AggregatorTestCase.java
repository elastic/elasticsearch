/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.FieldAliasMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IdLoader;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.PassThroughObjectMapper;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.MultiValueAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationContext.ProductionAggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.aggregations.support.TimeSeriesIndexSearcher;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesPhase;
import org.elasticsearch.search.fetch.subphase.FetchSourcePhase;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiFunction;
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
    private final List<Releasable> releasables = new ArrayList<>();
    protected ValuesSourceRegistry valuesSourceRegistry;
    private AnalysisModule analysisModule;

    // A list of field types that should not be tested, or are not currently supported
    private static final List<String> TYPE_TEST_BLACKLIST = List.of(
        ObjectMapper.CONTENT_TYPE, // Cannot aggregate objects
        DenseVectorFieldMapper.CONTENT_TYPE, // Cannot aggregate dense vectors
        SparseVectorFieldMapper.CONTENT_TYPE, // Sparse vectors are no longer supported

        NestedObjectMapper.CONTENT_TYPE, // TODO support for nested
        PassThroughObjectMapper.CONTENT_TYPE, // TODO support for passthrough
        CompletionFieldMapper.CONTENT_TYPE, // TODO support completion
        FieldAliasMapper.CONTENT_TYPE // TODO support alias
    );
    ThreadPool threadPool;
    ThreadPoolExecutor threadPoolExecutor;

    @Before
    public final void initPlugins() {
        threadPool = new TestThreadPool(AggregatorTestCase.class.getName());
        threadPoolExecutor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
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

    protected <A extends Aggregator> A createAggregator(AggregationBuilder aggregationBuilder, AggregationContext context)
        throws IOException {
        return createAggregator(new AggregatorFactories.Builder().addAggregator(aggregationBuilder), context);
    }

    private <A extends Aggregator> A createAggregator(AggregatorFactories.Builder builder, AggregationContext context) throws IOException {
        Aggregator[] aggregators = builder.build(context, null).createTopLevelAggregators();
        assertThat(aggregators.length, equalTo(1));
        @SuppressWarnings("unchecked")
        A aggregator = (A) aggregators[0];
        return aggregator;
    }

    /**
     * Create a {@linkplain AggregationContext} for testing an {@link Aggregator}.
     * While {@linkplain AggregationContext} is {@link Releasable} the caller is
     * not responsible for releasing it. Instead, it is released automatically in
     * in {@link #cleanupReleasables()}.
     *
     * Deprecated - this will be made private in a future update
     */
    @Deprecated
    protected AggregationContext createAggregationContext(IndexReader indexReader, Query query, MappedFieldType... fieldTypes)
        throws IOException {
        return createAggregationContext(
            indexReader,
            createIndexSettings(),
            query,
            new NoneCircuitBreakerService(),
            AggregationBuilder.DEFAULT_PREALLOCATION * 5, // We don't know how many bytes to preallocate so we grab a hand full
            DEFAULT_MAX_BUCKETS,
            false,
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
        IndexReader indexReader,
        IndexSettings indexSettings,
        Query query,
        CircuitBreakerService breakerService,
        long bytesToPreallocate,
        int maxBucket,
        boolean isInSortOrderExecutionRequired,
        boolean supportsParallelCollection,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return createAggregationContext(
            newIndexSearcher(indexReader, supportsParallelCollection),
            indexSettings,
            query,
            breakerService,
            bytesToPreallocate,
            maxBucket,
            isInSortOrderExecutionRequired,
            fieldTypes
        );
    }

    private AggregationContext createAggregationContext(
        IndexSearcher searcher,
        IndexSettings indexSettings,
        Query query,
        CircuitBreakerService breakerService,
        long bytesToPreallocate,
        int maxBucket,
        boolean isInSortOrderExecutionRequired,
        MappedFieldType... fieldTypes
    ) {
        return createAggregationContext(
            searcher,
            indexSettings,
            query,
            breakerService,
            bytesToPreallocate,
            maxBucket,
            isInSortOrderExecutionRequired,
            () -> false,
            fieldTypes
        );
    }

    /**
     * Creates an aggregation context that will randomly report that the query has been cancelled
     */
    private AggregationContext createCancellingAggregationContext(
        IndexSearcher searcher,
        IndexSettings indexSettings,
        Query query,
        CircuitBreakerService breakerService,
        long bytesToPreallocate,
        int maxBucket,
        boolean isInSortOrderExecutionRequired,
        MappedFieldType... fieldTypes
    ) {
        return createAggregationContext(
            searcher,
            indexSettings,
            query,
            breakerService,
            bytesToPreallocate,
            maxBucket,
            isInSortOrderExecutionRequired,
            () -> ESTestCase.random().nextInt(20) == 0,
            fieldTypes
        );
    }

    private AggregationContext createAggregationContext(
        IndexSearcher searcher,
        IndexSettings indexSettings,
        Query query,
        CircuitBreakerService breakerService,
        long bytesToPreallocate,
        int maxBucket,
        boolean isInSortOrderExecutionRequired,
        Supplier<Boolean> isCancelled,
        MappedFieldType... fieldTypes
    ) {
        MappingLookup mappingLookup = MappingLookup.fromMappers(
            Mapping.EMPTY,
            Arrays.stream(fieldTypes).map(this::buildMockFieldMapper).collect(toList()),
            objectMappers(),
            // Alias all fields to <name>-alias to test aliases
            Arrays.stream(fieldTypes)
                .map(ft -> new FieldAliasMapper(ft.name() + "-alias", ft.name() + "-alias", ft.name()))
                .collect(toList()),
            List.of()
        );
        BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> fieldDataBuilder = (fieldType, context) -> fieldType
            .fielddataBuilder(
                new FieldDataContext(
                    indexSettings.getIndex().getName(),
                    indexSettings,
                    context.lookupSupplier(),
                    context.sourcePathsLookup(),
                    context.fielddataOperation()
                )
            ).build(new IndexFieldDataCache.None(), breakerService);
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, BitsetFilterCache.Listener.NOOP);
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
            searcher,
            System::currentTimeMillis,
            null,
            null,
            () -> true,
            valuesSourceRegistry,
            emptyMap(),
            MapperMetrics.NOOP
        ) {
            @Override
            public Iterable<MappedFieldType> dimensionFields() {
                return Arrays.stream(fieldTypes).filter(MappedFieldType::isDimension).toList();
            }
        };

        AggregationContext context = new ProductionAggregationContext(
            Optional.ofNullable(analysisModule).map(AnalysisModule::getAnalysisRegistry).orElse(null),
            searchExecutionContext,
            new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService),
            ClusterSettings.createBuiltInClusterSettings(),
            bytesToPreallocate,
            () -> query,
            null,
            maxBucket,
            () -> buildSubSearchContext(indexSettings, searchExecutionContext, bitsetFilterCache),
            bitsetFilterCache,
            randomInt(),
            () -> 0L,
            isCancelled,
            q -> q,
            true,
            isInSortOrderExecutionRequired
        );
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
        try {
            when(ctx.searcher()).thenReturn(
                new ContextIndexSearcher(
                    searchExecutionContext.searcher().getIndexReader(),
                    searchExecutionContext.searcher().getSimilarity(),
                    DisabledQueryCache.INSTANCE,
                    TrivialQueryCachingPolicy.NEVER,
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
        MappingLookup disableNestedLookup = MappingLookup.fromMappers(Mapping.EMPTY, Set.of(), Set.of());
        doReturn(new NestedDocuments(disableNestedLookup, bitsetFilterCache::getBitSetProducer, indexSettings.getIndexVersionCreated()))
            .when(subContext)
            .getNestedDocuments();
        when(ctx.getSearchExecutionContext()).thenReturn(subContext);
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(randomBoolean()),
            new ShardId("index", "indexUUID", 0),
            0,
            1,
            AliasFilter.EMPTY,
            1f,
            0L,
            null
        );
        when(ctx.request()).thenReturn(request);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(ctx.indexShard()).thenReturn(indexShard);
        when(ctx.newSourceLoader()).thenAnswer(inv -> searchExecutionContext.newSourceLoader(false));
        when(ctx.newIdLoader()).thenReturn(IdLoader.fromLeafStoredFieldLoader());
        when(ctx.innerHits()).thenReturn(new InnerHitsContext());
        var res = new SubSearchContext(ctx);
        releasables.add(res); // TODO: nasty workaround for not getting the standard resource handling behavior of a real search context
        return res;
    }

    protected IndexSettings createIndexSettings() {
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
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

    /**
     * Create a RandomIndexWriter that uses the LogDocMergePolicy.
     *
     * The latest lucene upgrade adds a new merge policy that reverses the order of the documents and it is not compatible with some
     * aggregation types. This writer avoids randomization by hardcoding the merge policy to LogDocMergePolicy.
     */
    protected static RandomIndexWriter newRandomIndexWriterWithLogDocMergePolicy(Directory directory) throws IOException {
        final IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(new LogDocMergePolicy());
        return new RandomIndexWriter(random(), directory, conf);
    }

    /**
     * Collects all documents that match the provided query {@link Query} and
     * returns the reduced {@link InternalAggregation}.
     * <p>
     * It runs the aggregation as well using a circuit breaker that randomly throws {@link CircuitBreakingException}
     * in order to mak sure the implementation does not leak.
     */
    protected <A extends InternalAggregation> A searchAndReduce(IndexReader reader, AggTestConfig aggTestConfig) throws IOException {
        IndexSearcher searcher = newIndexSearcher(
            reader,
            aggTestConfig.builder.supportsParallelCollection(field -> getCardinality(reader, field))
        );
        IndexSettings indexSettings = createIndexSettings();
        // First run it to find circuit breaker leaks on the aggregator
        runWithCrankyCircuitBreaker(indexSettings, searcher, aggTestConfig);
        CircuitBreakerService breakerService = new NoneCircuitBreakerService();
        // Next, try with random cancellations, again looking for leaks
        runWithCancellingConfig(indexSettings, searcher, breakerService, aggTestConfig);
        // Finally, run it to the end
        return searchAndReduce(indexSettings, searcher, breakerService, aggTestConfig, this::createAggregationContext);
    }

    /**
     * Run an aggregation test against the {@link CrankyCircuitBreakerService}
     * which fails randomly. This is extracted into a separate function so that
     * stack traces will indicate if a bad allocation happened in the cranky CB
     * run or the happy path run.
     */
    private void runWithCrankyCircuitBreaker(IndexSettings indexSettings, IndexSearcher searcher, AggTestConfig aggTestConfig)
        throws IOException {
        CircuitBreakerService crankyService = new CrankyCircuitBreakerService();
        for (int i = 0; i < 5; i++) {
            try {
                searchAndReduce(indexSettings, searcher, crankyService, aggTestConfig, this::createAggregationContext);
            } catch (CircuitBreakingException e) {
                // Circuit breaks from the cranky breaker are expected - it randomly fails, after all
                assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
            }
        }
    }

    private void runWithCancellingConfig(
        IndexSettings indexSettings,
        IndexSearcher searcher,
        CircuitBreakerService breakerService,
        AggTestConfig aggTestConfig
    ) throws IOException {
        for (int i = 0; i < 5; i++) {
            try {
                searchAndReduce(indexSettings, searcher, breakerService, aggTestConfig, this::createCancellingAggregationContext);
            } catch (TaskCancelledException e) {
                // we don't want to expectThrows this because the randomizer might just never report cancellation,
                // but it's also normal that it should throw here.
            }
        }
    }

    @FunctionalInterface
    public interface AggregationcContextSupplier {
        AggregationContext get(
            IndexSearcher searcher,
            IndexSettings indexSettings,
            Query query,
            CircuitBreakerService breakerService,
            long bytesToPreallocate,
            int maxBucket,
            boolean isInSortOrderExecutionRequired,
            MappedFieldType... fieldTypes
        );
    }

    @SuppressWarnings("unchecked")
    private <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSettings indexSettings,
        IndexSearcher searcher,
        CircuitBreakerService breakerService,
        AggTestConfig aggTestConfig,
        AggregationcContextSupplier contextSupplier
    ) throws IOException {
        Query query = aggTestConfig.query();
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(aggTestConfig.builder());
        int maxBucket = aggTestConfig.maxBuckets();
        boolean splitLeavesIntoSeparateAggregators = aggTestConfig.splitLeavesIntoSeparateAggregators();
        boolean shouldBeCached = aggTestConfig.shouldBeCached();
        MappedFieldType[] fieldTypes = aggTestConfig.fieldTypes();

        final IndexReaderContext ctx = searcher.getTopReaderContext();
        List<InternalAggregations> internalAggs = new ArrayList<>();
        Query rewritten = searcher.rewrite(query);
        BigArrays bigArraysForReduction = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService);

        if (splitLeavesIntoSeparateAggregators
            && searcher.getIndexReader().leaves().size() > 0
            && builder.isInSortOrderExecutionRequired() == false) {
            assertThat(ctx, instanceOf(CompositeReaderContext.class));
            final CompositeReaderContext compCTX = (CompositeReaderContext) ctx;
            final int size = compCTX.leaves().size();
            final ShardSearcher[] subSearchers = new ShardSearcher[size];
            for (int searcherIDX = 0; searcherIDX < subSearchers.length; searcherIDX++) {
                final LeafReaderContext leave = compCTX.leaves().get(searcherIDX);
                subSearchers[searcherIDX] = new ShardSearcher(leave, compCTX);
            }
            for (ShardSearcher subSearcher : subSearchers) {
                AggregationContext context = contextSupplier.get(
                    subSearcher,
                    indexSettings,
                    query,
                    breakerService,
                    randomBoolean() ? 0 : builder.bytesToPreallocate(),
                    maxBucket,
                    builder.isInSortOrderExecutionRequired(),
                    fieldTypes
                );
                try {
                    C a = createAggregator(builder, context);
                    a.preCollection();
                    if (context.isInSortOrderExecutionRequired()) {
                        new TimeSeriesIndexSearcher(subSearcher, List.of()).search(rewritten, a);
                    } else {
                        Weight weight = subSearcher.createWeight(rewritten, ScoreMode.COMPLETE, 1f);
                        subSearcher.search(weight, a.asCollector());
                        a.postCollection();
                    }
                    assertEquals(shouldBeCached, context.isCacheable());
                    List<InternalAggregation> internalAggregations = List.of(a.buildTopLevel());
                    aggTestConfig.checkAggregator().accept(a);
                    assertRoundTrip(internalAggregations);
                    internalAggs.add(InternalAggregations.from(internalAggregations));
                } finally {
                    Releasables.close(context);
                }
            }
        } else {
            AggregationContext context = contextSupplier.get(
                searcher,
                indexSettings,
                query,
                breakerService,
                randomBoolean() ? 0 : builder.bytesToPreallocate(),
                maxBucket,
                builder.isInSortOrderExecutionRequired(),
                fieldTypes
            );
            try {
                List<C> aggregators = new ArrayList<>();
                if (context.isInSortOrderExecutionRequired()) {
                    C root = createAggregator(builder, context);
                    root.preCollection();
                    aggregators.add(root);
                    new TimeSeriesIndexSearcher(searcher, List.of()).search(rewritten, MultiBucketCollector.wrap(true, List.of(root)));
                    List<InternalAggregation> internalAggregations = List.of(root.buildTopLevel());
                    assertRoundTrip(internalAggregations);
                    internalAggs.add(InternalAggregations.from(internalAggregations));
                } else {
                    Supplier<AggregatorCollector> aggregatorSupplier = () -> {
                        try {
                            Aggregator aggregator = createAggregator(builder, context);
                            aggregator.preCollection();
                            BucketCollector bucketCollector = MultiBucketCollector.wrap(true, List.of(aggregator));
                            return new AggregatorCollector(new Aggregator[] { aggregator }, bucketCollector);
                        } catch (IOException e) {
                            throw new AggregationInitializationException("Could not initialize aggregators", e);
                        }
                    };
                    Supplier<AggregationReduceContext> reduceContextSupplier = () -> new AggregationReduceContext.ForPartial(
                        bigArraysForReduction,
                        getMockScriptService(),
                        () -> false,
                        builder,
                        b -> {}
                    );
                    AggregatorCollectorManager aggregatorCollectorManager = new AggregatorCollectorManager(
                        aggregatorSupplier,
                        internalAggs::add,
                        reduceContextSupplier
                    );
                    searcher.search(rewritten, aggregatorCollectorManager);
                }
            } finally {
                Releasables.close(context);
            }
        }
        if (aggTestConfig.incrementalReduce() && internalAggs.size() > 1) {
            // sometimes do an incremental reduce
            int toReduceSize = internalAggs.size();
            Collections.shuffle(internalAggs, random());
            int r = randomIntBetween(1, toReduceSize);
            List<InternalAggregations> toReduce = internalAggs.subList(0, r);
            AggregationReduceContext reduceContext = new AggregationReduceContext.ForPartial(
                bigArraysForReduction,
                getMockScriptService(),
                () -> false,
                builder,
                b -> {}
            );
            internalAggs = new ArrayList<>(internalAggs.subList(r, toReduceSize));
            internalAggs.add(InternalAggregations.topLevelReduce(toReduce, reduceContext));
            for (InternalAggregations internalAggregation : internalAggs) {
                assertRoundTrip(internalAggregation.copyResults());
            }
        }
        /* Verify that cancellation during final reduce correctly throws.
         * We check reduce time cancellation only when consuming buckets.
         */
        if (aggTestConfig.testReductionCancellation()) {
            try {
                // I can't remember if we mutate the InternalAggregations list, so make a defensive copy
                List<InternalAggregations> internalAggsCopy = new ArrayList<>(internalAggs);
                A internalAgg = doFinalReduce(maxBucket, bigArraysForReduction, builder, internalAggsCopy, true);
                if (internalAgg instanceof MultiBucketsAggregation mb) {
                    // Empty mutli-bucket aggs are expected to return before even getting to the cancellation check
                    assertEquals("Got non-empty result for a cancelled reduction", 0, mb.getBuckets().size());
                } // other cases?
            } catch (TaskCancelledException e) {
                /* We may not always honor cancellation in reduce, for example if we are returning no results, so we can't
                 * just expectThrows here.
                 */
            }
        }

        // now do the final reduce
        A internalAgg = doFinalReduce(maxBucket, bigArraysForReduction, builder, internalAggs, false);
        assertRoundTrip(internalAgg);
        if (aggTestConfig.builder instanceof ValuesSourceAggregationBuilder.MetricsAggregationBuilder<?>) {
            verifyMetricNames((ValuesSourceAggregationBuilder.MetricsAggregationBuilder<?>) aggTestConfig.builder, internalAgg);
        }
        return internalAgg;
    }

    private <A extends InternalAggregation> A doFinalReduce(
        int maxBucket,
        BigArrays bigArraysForReduction,
        Builder builder,
        List<InternalAggregations> internalAggs,
        boolean cancelled
    ) throws IOException {
        MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumer(
            maxBucket,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            bigArraysForReduction,
            getMockScriptService(),
            () -> cancelled,
            builder,
            reduceBucketConsumer
        );

        @SuppressWarnings("unchecked")
        A internalAgg = (A) doInternalAggregationsReduce(internalAggs, reduceContext);
        assertRoundTrip(internalAgg);

        doAssertReducedMultiBucketConsumer(internalAgg, reduceBucketConsumer);
        return internalAgg;
    }

    private InternalAggregation doReduce(List<InternalAggregation> aggregators, AggregationReduceContext reduceContext) {
        final List<InternalAggregations> internalAggregations = new ArrayList<>(aggregators.size());
        for (InternalAggregation aggregator : aggregators) {
            internalAggregations.add(InternalAggregations.from(List.of(aggregator)));
        }
        return doInternalAggregationsReduce(internalAggregations, reduceContext);
    }

    private InternalAggregation doInternalAggregationsReduce(
        List<InternalAggregations> internalAggregations,
        AggregationReduceContext reduceContext
    ) {
        InternalAggregations aggregations = InternalAggregations.topLevelReduce(internalAggregations, reduceContext);
        List<InternalAggregation> reduced = aggregations.copyResults();
        assertThat(reduced.size(), equalTo(1));
        return reduced.get(0);
    }

    protected void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        InternalAggregationTestCase.assertMultiBucketConsumer(agg, bucketConsumer);
    }

    protected <T extends AggregationBuilder, V extends InternalAggregation> void testCase(
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<V> verify,
        AggTestConfig aggTestConfig
    ) throws IOException {
        boolean timeSeries = aggTestConfig.builder().isInSortOrderExecutionRequired();
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random()));
            if (aggTestConfig.useLogDocMergePolicy()) {
                // Use LogDocMergePolicy to avoid randomization issues with the doc retrieval order for nested aggs.
                config.setMergePolicy(new LogDocMergePolicy());
            }
            if (timeSeries) {
                Sort sort = new Sort(
                    new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, false),
                    new SortedNumericSortField(DataStreamTimestampFieldMapper.DEFAULT_PATH, SortField.Type.LONG, true)
                );
                config.setParentField(Engine.ROOT_DOC_FIELD_NAME);
                config.setIndexSort(sort);
            }
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, config);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (
                DirectoryReader unwrapped = DirectoryReader.open(directory);
                DirectoryReader indexReader = wrapDirectoryReader(unwrapped)
            ) {
                V agg = searchAndReduce(indexReader, aggTestConfig);
                verify.accept(agg);

                verifyOutputFieldNames(aggTestConfig.builder(), agg);
            }
        }
    }

    protected void withIndex(CheckedConsumer<RandomIndexWriter, IOException> buildIndex, CheckedConsumer<IndexReader, IOException> consume)
        throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            buildIndex.accept(iw);
            iw.close();
            try (
                DirectoryReader unwrapped = DirectoryReader.open(directory);
                DirectoryReader indexReader = wrapDirectoryReader(unwrapped)
            ) {
                consume.accept(indexReader);
            }
        }
    }

    protected void withNonMergingIndex(
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        CheckedConsumer<IndexReader, IOException> consume
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(
                random(),
                directory,
                LuceneTestCase.newIndexWriterConfig(random(), new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            );
            buildIndex.accept(iw);
            iw.close();
            try (
                DirectoryReader unwrapped = DirectoryReader.open(directory);
                DirectoryReader indexReader = wrapDirectoryReader(unwrapped)
            ) {
                consume.accept(indexReader);
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
        withIndex(buildIndex, reader -> debugTestCase(builder, query, reader, verify, null, fieldTypes));
    }

    /**
     * Execute and aggregation and collect its {@link Aggregator#collectDebugInfo debug}
     * information. Unlike {@link #testCase} this doesn't randomly create an
     * {@link Aggregator} per leaf and perform partial reductions. It always
     * creates a single {@link Aggregator} so we can get consistent debug info.
     */
    protected <R extends InternalAggregation> void debugTestCase(
        AggregationBuilder aggregationBuilder,
        Query query,
        IndexReader reader,
        TriConsumer<R, Class<? extends Aggregator>, Map<String, Map<String, Object>>> verify,
        QueryCachingPolicy queryCachingPolicy,
        MappedFieldType... fieldTypes
    ) throws IOException {
        // Don't use searchAndReduce because we only want a single aggregator, disable parallel collection too.
        IndexSearcher searcher = newIndexSearcher(reader, false);
        if (queryCachingPolicy != null) {
            searcher.setQueryCachingPolicy(queryCachingPolicy);
        }
        CircuitBreakerService breakerService = new NoneCircuitBreakerService();
        AggregationContext context = createAggregationContext(
            searcher,
            createIndexSettings(),
            searcher.rewrite(query),
            breakerService,
            aggregationBuilder.bytesToPreallocate(),
            DEFAULT_MAX_BUCKETS,
            aggregationBuilder.isInSortOrderExecutionRequired(),
            fieldTypes
        );
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(aggregationBuilder);
        try {
            Aggregator aggregator = createAggregator(builder, context);
            aggregator.preCollection();
            searcher.search(context.query(), new CollectorManager<Collector, Void>() {
                boolean called = false;

                @Override
                public Collector newCollector() {
                    assert called == false : "newCollector called multiple times";
                    called = true;
                    return aggregator.asCollector();
                }

                @Override
                public Void reduce(Collection<Collector> collectors) {
                    return null;
                }
            });
            InternalAggregation r = aggregator.buildTopLevel();
            r = doReduce(
                List.of(r),
                new AggregationReduceContext.ForFinal(
                    context.bigArrays(),
                    getMockScriptService(),
                    () -> false,
                    builder,
                    new MultiBucketConsumer(context.maxBuckets(), context.breaker())
                )
            );
            @SuppressWarnings("unchecked") // We'll get a cast error in the test if we're wrong here and that is ok
            R result = (R) r;
            assertRoundTrip(result);
            Map<String, Map<String, Object>> debug = new HashMap<>();
            collectDebugInfo("", aggregator, debug);
            verify.apply(result, aggregator.getClass(), debug);
            verifyOutputFieldNames(aggregationBuilder, result);
        } finally {
            Releasables.close(context);
        }
    }

    private static void collectDebugInfo(String prefix, Aggregator aggregator, Map<String, Map<String, Object>> allDebug) {
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
        CheckedBiConsumer<IndexReader, Aggregator, IOException> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (
                DirectoryReader unwrapped = DirectoryReader.open(directory);
                DirectoryReader indexReader = wrapDirectoryReader(unwrapped)
            ) {
                try (AggregationContext context = createAggregationContext(indexReader, query, fieldTypes)) {
                    verify.accept(indexReader, createAggregator(aggregationBuilder, context));
                }
            }
        }
    }

    private static void verifyMetricNames(
        ValuesSourceAggregationBuilder.MetricsAggregationBuilder<?> aggregationBuilder,
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
    protected DirectoryReader wrapDirectoryReader(DirectoryReader reader) throws IOException {
        return reader;
    }

    private static class ShardSearcher extends IndexSearcher {
        private final LeafReaderContextPartition[] ctx;

        ShardSearcher(LeafReaderContext ctx, IndexReaderContext parent) {
            super(parent);
            this.ctx = new LeafReaderContextPartition[] { IndexSearcher.LeafReaderContextPartition.createForEntireSegment(ctx) };
        }

        public void search(Weight weight, Collector collector) throws IOException {
            search(ctx, weight, collector);
        }

        @Override
        public String toString() {
            return "ShardSearcher(" + ctx[0] + ")";
        }
    }

    protected static DirectoryReader wrapInMockESDirectoryReader(DirectoryReader directoryReader) throws IOException {
        return ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId(new Index("_index", "_na_"), 0));
    }

    /**
     * Creates a {@link ContextIndexSearcher} that supports concurrency running each segment in a different thread.
     */
    private IndexSearcher newIndexSearcher(IndexReader indexReader, boolean supportsParallelCollection) throws IOException {
        return new ContextIndexSearcher(
            indexReader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            indexReader instanceof DirectoryReader ? randomBoolean() : false, // we can only wrap DirectoryReader instances
            this.threadPoolExecutor,
            this.threadPoolExecutor.getMaximumPoolSize(),
            supportsParallelCollection ? 1 /* forces multiple slices */ : Integer.MAX_VALUE // one slice
        );
    }

    /**
     * Added to randomly run with more assertions on the index reader level,
     * like {@link org.apache.lucene.tests.util.LuceneTestCase#wrapReader(IndexReader)}, which can't be used because it also
     * wraps in the IndexReader with other implementations that we can't handle. (e.g. ParallelCompositeReader)
     */
    protected static DirectoryReader maybeWrapReaderEs(DirectoryReader reader) throws IOException {
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
        String fieldName = "typeTestFieldName";
        List<ValuesSourceType> supportedVSTypes = getSupportedValuesSourceTypes();
        List<String> unsupportedMappedFieldTypes = unsupportedMappedFieldTypes();

        if (supportedVSTypes.isEmpty()) {
            // If the test says it doesn't support any VStypes, it has not been converted yet so skip
            return;
        }

        for (Map.Entry<String, Mapper.TypeParser> mappedType : IndicesModule.getMappers(List.of()).entrySet()) {

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
            FieldMapper mapper = (FieldMapper) builder.build(MapperBuilderContext.root(false, false));

            MappedFieldType fieldType = mapper.fieldType();

            // Non-aggregatable fields are not testable (they will throw an error on all aggs anyway), so skip
            if (fieldType.isAggregatable() == false) {
                continue;
            }

            try (Directory directory = newDirectory()) {
                RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
                writeTestDoc(fieldType, fieldName, indexWriter);
                indexWriter.close();

                try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                    AggregationBuilder aggregationBuilder = createAggBuilderForTypeTest(fieldType, fieldName);

                    ValuesSourceType vst = fieldToVST(fieldType);
                    // TODO in the future we can make this more explicit with expectThrows(), when the exceptions are standardized
                    AssertionError failure = null;
                    try {
                        InternalAggregation internalAggregation = searchAndReduce(
                            indexReader,
                            new AggTestConfig(aggregationBuilder, fieldType)
                        );
                        // We should make sure if the builder says it supports sampling, that the internal aggregations returned override
                        // finalizeSampling
                        if (aggregationBuilder.supportsSampling()) {
                            SamplingContext randomSamplingContext = new SamplingContext(
                                randomDoubleBetween(1e-8, 0.1, false),
                                randomInt(),
                                randomBoolean() ? null : randomInt()
                            );
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

    private static ValuesSourceType fieldToVST(MappedFieldType fieldType) {
        return fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test")).build(null, null).getValuesSourceType();
    }

    /**
     * Helper method to write a single document with a single value specific to the requested fieldType.
     *
     * Throws an exception if it encounters an unknown field type, to prevent new ones from sneaking in without
     * being tested.
     */
    private static void writeTestDoc(MappedFieldType fieldType, String fieldName, RandomIndexWriter iw) throws IOException {

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
            json = Strings.format("""
                { "%s" : { "gte" : "%s", "lte" : "%s" } }
                """, fieldName, start, end);
        } else if (vst.equals(CoreValuesSourceType.GEOPOINT)) {
            double lat = randomDouble();
            double lon = randomDouble();
            doc.add(new LatLonDocValuesField(fieldName, lat, lon));
            json = Strings.format("""
                { "%s" : "[%s,%s]" }""", fieldName, lon, lat);
        } else {
            throw new IllegalStateException("Unknown field type [" + typeName + "]");
        }

        doc.add(new StoredField("_source", new BytesRef(json)));
        iw.addDocument(doc);
    }

    private static class MockParserContext extends MappingParserContext {
        MockParserContext(IndexSettings indexSettings) {
            super(
                null,
                null,
                null,
                IndexVersion.current(),
                () -> TransportVersion.current(),
                null,
                ScriptCompiler.NONE,
                null,
                indexSettings,
                null,
                query -> {
                    throw new UnsupportedOperationException();
                }
            );
        }

        @Override
        public Settings getSettings() {
            return Settings.EMPTY;
        }

        @Override
        public IndexAnalyzers getIndexAnalyzers() {
            return (type, name) -> Lucene.STANDARD_ANALYZER;
        }

    }

    @After
    public void cleanupReleasables() {
        Releasables.close(releasables);
        releasables.clear();
        threadPoolExecutor.shutdown();
        terminate(threadPool);
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

    long getCardinality(IndexReader reader, String field) {
        try {
            final TermsEnum[] subs = new TermsEnum[reader.leaves().size()];
            final long[] weights = new long[reader.leaves().size()];
            for (int i = 0; i < reader.leaves().size(); i++) {
                LeafReaderContext context = reader.leaves().get(i);
                FieldInfos fieldInfos = context.reader().getFieldInfos();
                FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
                if (fieldInfo == null) {
                    return -1;
                }
                switch (fieldInfo.getDocValuesType()) {
                    case SORTED -> {
                        SortedDocValues sortedDocValues = context.reader().getSortedDocValues(field);
                        subs[i] = sortedDocValues.termsEnum();
                        weights[i] = sortedDocValues.getValueCount();
                    }
                    case SORTED_SET -> {
                        SortedSetDocValues sortedDocValues = context.reader().getSortedSetDocValues(field);
                        subs[i] = sortedDocValues.termsEnum();
                        weights[i] = sortedDocValues.getValueCount();
                    }
                    case NUMERIC, SORTED_NUMERIC -> {
                        final byte[] min = PointValues.getMinPackedValue(reader, field);
                        final byte[] max = PointValues.getMaxPackedValue(reader, field);
                        if (min != null && max != null) {
                            if (min.length == 4) {
                                return NumericUtils.sortableBytesToInt(max, 0) - NumericUtils.sortableBytesToInt(min, 0);
                            } else if (min.length == 8) {
                                return NumericUtils.sortableBytesToLong(max, 0) - NumericUtils.sortableBytesToLong(min, 0);
                            }
                        }
                        return -1;
                    }
                    default -> {
                        return -1;
                    }
                }
            }
            final OrdinalMap ordinalMap = OrdinalMap.build(null, subs, weights, PackedInts.DEFAULT);
            return ordinalMap.getValueCount();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
                        protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
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
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ZERO;
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
        protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
            return new AggregatorReducer() {
                @Override
                public void accept(InternalAggregation aggregation) {
                    assertThat(((InternalAggCardinalityUpperBound) aggregation).cardinality, equalTo(cardinality));
                }

                @Override
                public InternalAggregation get() {
                    return new InternalAggCardinalityUpperBound(name, cardinality, metadata);
                }
            };
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

    public record AggTestConfig(
        Query query,
        AggregationBuilder builder,
        int maxBuckets,
        boolean splitLeavesIntoSeparateAggregators,
        boolean shouldBeCached,
        boolean incrementalReduce,

        boolean useLogDocMergePolicy,
        boolean testReductionCancellation,
        Consumer<Aggregator> checkAggregator,
        MappedFieldType... fieldTypes
    ) {

        public AggTestConfig(AggregationBuilder builder, MappedFieldType... fieldTypes) {
            this(
                new MatchAllDocsQuery(),
                builder,
                DEFAULT_MAX_BUCKETS,
                randomBoolean(),
                true,
                randomBoolean(),
                false,
                true,
                a -> {},
                fieldTypes
            );
        }

        public AggTestConfig withQuery(Query query) {
            return new AggTestConfig(
                query,
                builder,
                maxBuckets,
                splitLeavesIntoSeparateAggregators,
                shouldBeCached,
                incrementalReduce,
                useLogDocMergePolicy,
                testReductionCancellation,
                checkAggregator,
                fieldTypes
            );
        }

        public AggTestConfig withSplitLeavesIntoSeperateAggregators(boolean splitLeavesIntoSeparateAggregators) {
            return new AggTestConfig(
                query,
                builder,
                maxBuckets,
                splitLeavesIntoSeparateAggregators,
                shouldBeCached,
                incrementalReduce,
                useLogDocMergePolicy,
                testReductionCancellation,
                checkAggregator,
                fieldTypes
            );
        }

        public AggTestConfig withShouldBeCached(boolean shouldBeCached) {
            return new AggTestConfig(
                query,
                builder,
                maxBuckets,
                splitLeavesIntoSeparateAggregators,
                shouldBeCached,
                incrementalReduce,
                useLogDocMergePolicy,
                testReductionCancellation,
                checkAggregator,
                fieldTypes
            );
        }

        public AggTestConfig withMaxBuckets(int maxBuckets) {
            return new AggTestConfig(
                query,
                builder,
                maxBuckets,
                splitLeavesIntoSeparateAggregators,
                shouldBeCached,
                incrementalReduce,
                useLogDocMergePolicy,
                testReductionCancellation,
                checkAggregator,
                fieldTypes
            );
        }

        public AggTestConfig withIncrementalReduce(boolean incrementalReduce) {
            return new AggTestConfig(
                query,
                builder,
                maxBuckets,
                splitLeavesIntoSeparateAggregators,
                shouldBeCached,
                incrementalReduce,
                useLogDocMergePolicy,
                testReductionCancellation,
                checkAggregator,
                fieldTypes
            );
        }

        public AggTestConfig withLogDocMergePolicy() {
            return new AggTestConfig(
                query,
                builder,
                maxBuckets,
                splitLeavesIntoSeparateAggregators,
                shouldBeCached,
                incrementalReduce,
                true,
                testReductionCancellation,
                checkAggregator,
                fieldTypes
            );
        }

        public AggTestConfig noReductionCancellation() {
            return new AggTestConfig(
                query,
                builder,
                maxBuckets,
                splitLeavesIntoSeparateAggregators,
                shouldBeCached,
                incrementalReduce,
                useLogDocMergePolicy,
                false,
                checkAggregator,
                fieldTypes
            );
        }

        public AggTestConfig withCheckAggregator(Consumer<Aggregator> checkAggregator) {
            return new AggTestConfig(
                query,
                builder,
                maxBuckets,
                splitLeavesIntoSeparateAggregators,
                shouldBeCached,
                incrementalReduce,
                useLogDocMergePolicy,
                testReductionCancellation,
                checkAggregator,
                fieldTypes
            );
        }
    }
}
