/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.search.aggregations;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.PreallocatedCircuitBreakerService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.NameOrDefinition;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.BucketedSort.ExtraData;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Benchmarks the overhead of constructing {@link Aggregator}s in many
 * parallel threads. Machines with different numbers of cores will see
 * wildly different results running this from running this with more
 * cores seeing more benefits from preallocation.
 */
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(Threads.MAX)
public class AggConstructionContentionBenchmark {
    private final SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final PageCacheRecycler recycler = new PageCacheRecycler(Settings.EMPTY);
    private final Index index = new Index("test", "uuid");
    private final IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(
        Settings.EMPTY,
        new IndexFieldDataCache.Listener() {
        }
    );

    private CircuitBreakerService breakerService;
    private BigArrays bigArrays;
    private boolean preallocateBreaker;

    @Param({ "noop", "real", "preallocate" })
    private String breaker;

    @Setup
    public void setup() {
        breakerService = switch (breaker) {
            case "real", "preallocate" -> new HierarchyCircuitBreakerService(
                CircuitBreakerMetrics.NOOP,
                Settings.EMPTY,
                List.of(),
                clusterSettings
            );
            case "noop" -> new NoneCircuitBreakerService();
            default -> throw new UnsupportedOperationException();
        };
        preallocateBreaker = breaker.equals("preallocate");
        bigArrays = new BigArrays(recycler, breakerService, "request");
    }

    @Benchmark
    public void sum() throws IOException {
        buildFactories(new AggregatorFactories.Builder().addAggregator(new SumAggregationBuilder("s").field("int_1")));
    }

    @Benchmark
    public void termsSum() throws IOException {
        buildFactories(
            new AggregatorFactories.Builder().addAggregator(
                new TermsAggregationBuilder("t").field("int_1").subAggregation(new SumAggregationBuilder("s").field("int_2"))
            )
        );
    }

    @Benchmark
    public void termsSixtySums() throws IOException {
        TermsAggregationBuilder b = new TermsAggregationBuilder("t").field("int_1");
        for (int i = 0; i < 60; i++) {
            b.subAggregation(new SumAggregationBuilder("s" + i).field("int_" + i));
        }
        buildFactories(new AggregatorFactories.Builder().addAggregator(b));
    }

    private void buildFactories(AggregatorFactories.Builder factories) throws IOException {
        try (DummyAggregationContext context = new DummyAggregationContext(factories.bytesToPreallocate())) {
            factories.build(context, null).createTopLevelAggregators();
        }
    }

    private class DummyAggregationContext extends AggregationContext {
        private final Query query = new MatchAllDocsQuery();
        private final List<Releasable> releaseMe = new ArrayList<>();

        private final CircuitBreaker breaker;
        private final PreallocatedCircuitBreakerService preallocated;

        DummyAggregationContext(long bytesToPreallocate) {
            CircuitBreakerService breakerService;
            if (preallocateBreaker) {
                breakerService = preallocated = new PreallocatedCircuitBreakerService(
                    AggConstructionContentionBenchmark.this.breakerService,
                    CircuitBreaker.REQUEST,
                    bytesToPreallocate,
                    "aggregations"
                );
            } else {
                breakerService = AggConstructionContentionBenchmark.this.breakerService;
                preallocated = null;
            }
            breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        }

        @Override
        public Query query() {
            return query;
        }

        @Override
        public Aggregator profileIfEnabled(Aggregator agg) throws IOException {
            return agg;
        }

        @Override
        public boolean profiling() {
            return false;
        }

        @Override
        public long nowInMillis() {
            return 0;
        }

        @Override
        public Analyzer getNamedAnalyzer(String analyzer) {
            return null;
        }

        @Override
        public Analyzer buildCustomAnalyzer(
            IndexSettings indexSettings,
            boolean normalizer,
            NameOrDefinition tokenizer,
            List<NameOrDefinition> charFilters,
            List<NameOrDefinition> tokenFilters
        ) {
            return null;
        }

        @Override
        protected IndexFieldData<?> buildFieldData(MappedFieldType ft) {
            IndexFieldDataCache indexFieldDataCache = indicesFieldDataCache.buildIndexFieldDataCache(new IndexFieldDataCache.Listener() {
            }, index, ft.name());
            return ft.fielddataBuilder(FieldDataContext.noRuntimeFields("benchmark")).build(indexFieldDataCache, breakerService);
        }

        @Override
        public MappedFieldType getFieldType(String path) {
            if (path.startsWith("int")) {
                return new NumberFieldMapper.NumberFieldType(path, NumberType.INTEGER);
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getMatchingFieldNames(String pattern) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SearchLookup lookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValuesSourceRegistry getValuesSourceRegistry() {
            return searchModule.getValuesSourceRegistry();
        }

        @Override
        public BigArrays bigArrays() {
            return bigArrays;
        }

        @Override
        public IndexSearcher searcher() {
            return null;
        }

        @Override
        public Query buildQuery(QueryBuilder builder) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Query filterQuery(Query query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexSettings getIndexSettings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClusterSettings getClusterSettings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public NestedLookup nestedLookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NestedScope nestedScope() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SubSearchContext subSearchContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addReleasable(Aggregator aggregator) {
            releaseMe.add(aggregator);
        }

        @Override
        public void removeReleasable(Aggregator aggregator) {
            releaseMe.remove(aggregator);
        }

        @Override
        public int maxBuckets() {
            return Integer.MAX_VALUE;
        }

        @Override
        public BitsetFilterCache bitsetFilterCache() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BucketedSort buildBucketedSort(SortBuilder<?> sort, int size, ExtraData values) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int shardRandomSeed() {
            return 0;
        }

        @Override
        public long getRelativeTimeInMillis() {
            return 0;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public CircuitBreaker breaker() {
            return breaker;
        }

        @Override
        public Analyzer getIndexAnalyzer(Function<String, NamedAnalyzer> unindexedFieldAnalyzer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCacheable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enableRewriteToFilterByFilter() {
            return true;
        }

        @Override
        public boolean isInSortOrderExecutionRequired() {
            return false;
        }

        @Override
        public Set<String> sourcePath(String fullName) {
            return Set.of(fullName);
        }

        @Override
        public void close() {
            List<Releasable> releaseMe = new ArrayList<>(this.releaseMe);
            releaseMe.add(preallocated);
            Releasables.close(releaseMe);
        }
    }
}
