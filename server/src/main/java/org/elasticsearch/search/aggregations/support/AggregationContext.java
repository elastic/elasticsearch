/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.PreallocatedCircuitBreakerService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.NameOrDefinition;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.bucket.filter.FilterByFilterAggregator;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.profile.aggregation.AggregationProfiler;
import org.elasticsearch.search.profile.aggregation.ProfilingAggregator;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Everything used to build and execute aggregations and the
 * {@link ValuesSource data sources} that power them.
 * <p>
 * In production we always use the {@link ProductionAggregationContext} but
 * this is {@code abstract} so that tests can build it without creating the
 * massing {@link SearchExecutionContext}.
 * <p>
 * {@linkplain AggregationContext}s are {@link Releasable} because they track
 * the {@link Aggregator}s they build and {@link Aggregator#close} them when
 * the request is done. {@linkplain AggregationContext} may also preallocate
 * bytes on the "REQUEST" breaker and is responsible for releasing those bytes.
 */
public abstract class AggregationContext implements Releasable {
    /**
     * The query at the top level of the search in which these aggregations are running.
     */
    public abstract Query query();

    /**
     * Wrap the aggregator for profiling if profiling is enabled.
     */
    public abstract Aggregator profileIfEnabled(Aggregator agg) throws IOException;

    /**
     * Are we profiling the aggregation?
     */
    public abstract boolean profiling();

    /**
     * The time in milliseconds that is shared across all resources involved. Even across shards and nodes.
     */
    public abstract long nowInMillis();

    /**
     * Lookup the context for a field.
     */
    public final FieldContext buildFieldContext(String field) {
        MappedFieldType ft = getFieldType(field);
        if (ft == null) {
            // The field is unmapped
            return null;
        }
        return new FieldContext(field, buildFieldData(ft), ft);
    }

    /**
     * Returns an existing registered analyzer that should NOT be closed when finished being used.
     * @param analyzer The custom analyzer name
     * @return The existing named analyzer.
     */
    public abstract Analyzer getNamedAnalyzer(String analyzer) throws IOException;

    /**
     * Creates a new custom analyzer that should be closed when finished being used.
     * @param indexSettings The current index settings or null
     * @param normalizer Is a normalizer
     * @param tokenizer The tokenizer name or definition to use
     * @param charFilters The char filter name or definition to use
     * @param tokenFilters The token filter name or definition to use
     * @return A new custom analyzer
     */
    public abstract Analyzer buildCustomAnalyzer(
        IndexSettings indexSettings,
        boolean normalizer,
        NameOrDefinition tokenizer,
        List<NameOrDefinition> charFilters,
        List<NameOrDefinition> tokenFilters
    ) throws IOException;

    /**
     * Lookup the context for an already resolved field type.
     */
    public final FieldContext buildFieldContext(MappedFieldType ft) {
        return new FieldContext(ft.name(), buildFieldData(ft), ft);
    }

    /**
     * Build field data.
     */
    protected abstract IndexFieldData<?> buildFieldData(MappedFieldType ft);

    /**
     * Lookup a {@link MappedFieldType} by path.
     */
    public abstract MappedFieldType getFieldType(String path);

    /**
     * Returns a set of field names that match a regex-like pattern
     * All field names in the returned set are guaranteed to resolve to a field
     */
    public abstract Set<String> getMatchingFieldNames(String pattern);

    /**
     * Compile a script.
     */
    public abstract <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context);

    /**
     * Fetch the shared {@link SearchLookup}.
     */
    public abstract SearchLookup lookup();

    /**
     * The {@link ValuesSourceRegistry} to resolve {@link Aggregator}s and the like.
     */
    public abstract ValuesSourceRegistry getValuesSourceRegistry();

    /**
     * The {@link AggregationUsageService} used to track which aggregations are
     * actually used.
     */
    public final AggregationUsageService getUsageService() {
        return getValuesSourceRegistry().getUsageService();
    }

    /**
     * Utility to share and track large arrays.
     */
    public abstract BigArrays bigArrays();

    /**
     * The searcher that will execute this query.
     */
    public abstract IndexSearcher searcher();

    /**
     * Build a query.
     */
    public abstract Query buildQuery(QueryBuilder builder) throws IOException;

    /**
     * Add filters from slice or filtered aliases. If you make a new query
     * and don't combine it with the {@link #query() top level query} then
     * you must provide it to this method.
     */
    public abstract Query filterQuery(Query query);

    /**
     * The settings for the index against which this search is running.
     */
    public abstract IndexSettings getIndexSettings();

    /**
     * The settings for the cluster against which this search is running.
     */
    public abstract ClusterSettings getClusterSettings();

    /**
     * Compile a sort.
     */
    public abstract Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders) throws IOException;

    /**
     * Get the {@link NestedLookup} of this index
     */
    public abstract NestedLookup nestedLookup();

    /**
     * Access the nested scope. Stay away from this unless you are dealing with nested.
     */
    public abstract NestedScope nestedScope();

    /**
     * Build a {@linkplain SubSearchContext} to power an aggregation fetching top hits.
     * Try to avoid using this because it pulls in a ton of dependencies.
     */
    public abstract SubSearchContext subSearchContext();

    /**
     * Cause this aggregation to be released when the search is finished.
     */
    public abstract void addReleasable(Aggregator aggregator);

    /**
     * Cause this aggregation to be released when the search is finished.
     */
    public abstract void removeReleasable(Aggregator aggregator);

    /**
     * Max buckets provided by the search.max_buckets setting
     */
    public abstract int maxBuckets();

    /**
     * Get the filter cache.
     */
    public abstract BitsetFilterCache bitsetFilterCache();
    // TODO it is unclear why we can't just use the IndexSearcher which already caches

    /**
     * Build a collector for sorted values specialized for aggregations.
     */
    public abstract BucketedSort buildBucketedSort(SortBuilder<?> sort, int size, BucketedSort.ExtraData values) throws IOException;

    /**
     * Get a deterministic random seed based for this particular shard.
     */
    public abstract int shardRandomSeed();

    /**
     * How many millis have passed since we started the search?
     */
    public abstract long getRelativeTimeInMillis();

    /**
     * Has the search been cancelled?
     * <p>
     * This'll require a {@code volatile} read.
     */
    public abstract boolean isCancelled();

    /**
     * The circuit breaker used to account for aggs.
     */
    public abstract CircuitBreaker breaker();

    /**
     * Return the index-time analyzer for the current index
     * @param unindexedFieldAnalyzer    a function that builds an analyzer for unindexed fields
     */
    public abstract Analyzer getIndexAnalyzer(Function<String, NamedAnalyzer> unindexedFieldAnalyzer);

    /**
     * Is this request cacheable? Requests that have
     * non-deterministic queries or scripts aren't cachable.
     */
    public abstract boolean isCacheable();

    /**
     * Are aggregations allowed to try to rewrite themselves into
     * {@link FilterByFilterAggregator} aggregations? <strong>Often</strong>
     * {@linkplain FilterByFilterAggregator} is faster to execute, but it isn't
     * always. For now this just hooks into a cluster level setting
     * so users can disable the behavior when the existing heuristics
     * don't detect cases where its slower.
     */
    public abstract boolean enableRewriteToFilterByFilter();

    /**
     * Return true if any of the aggregations in this context is a time-series aggregation that requires an in-sort order execution.
     *
     * A side-effect of such execution is that all leaves are walked simultaneously and therefore we can no longer rely on
     * {@link BucketCollector#getLeafCollector(AggregationExecutionContext)} to be called only after the
     * previous leaf was fully collected.
     */
    public abstract boolean isInSortOrderExecutionRequired();

    public abstract Set<String> sourcePath(String fullName);

    /**
     * Returns the MappingLookup for the index, if one is initialized.
     */
    @Nullable
    public MappingLookup getMappingLookup() {
        return null;
    }

    /**
     * Does this index have a {@code _doc_count} field in any segment?
     */
    public final boolean hasDocCountField() throws IOException {
        /*
         * When we add the second filter we check if there are any _doc_count
         * fields and bail out of filter-by filter mode if there are. _doc_count
         * fields are expensive to decode and the overhead of iterating per
         * filter causes us to decode doc counts over and over again.
         */
        Term term = new Term(DocCountFieldMapper.NAME, DocCountFieldMapper.NAME);
        for (LeafReaderContext c : searcher().getLeafContexts()) {
            if (c.reader().docFreq(term) > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Implementation of {@linkplain AggregationContext} for production usage
     * that wraps our ubiquitous {@link SearchExecutionContext} and anything else
     * specific to aggregations. Unit tests should generally avoid using this
     * because it requires a <strong>huge</strong> portion of a real
     * Elasticsearch node.
     */
    public static class ProductionAggregationContext extends AggregationContext {
        private final SearchExecutionContext context;
        private final PreallocatedCircuitBreakerService preallocatedBreakerService;
        private final BigArrays bigArrays;

        private final ClusterSettings clusterSettings;

        private final Supplier<Query> topLevelQuery;
        private final AggregationProfiler profiler;
        private final int maxBuckets;
        private final Supplier<SubSearchContext> subSearchContextBuilder;
        private final BitsetFilterCache bitsetFilterCache;
        private final int randomSeed;
        private final LongSupplier relativeTimeInMillis;
        private final Supplier<Boolean> isCancelled;
        private final Function<Query, Query> filterQuery;
        private final boolean enableRewriteToFilterByFilter;
        private final boolean inSortOrderExecutionRequired;
        private final AnalysisRegistry analysisRegistry;

        private final List<Aggregator> releaseMe = new ArrayList<>();

        public ProductionAggregationContext(
            AnalysisRegistry analysisRegistry,
            SearchExecutionContext context,
            BigArrays bigArrays,
            ClusterSettings clusterSettings,
            long bytesToPreallocate,
            Supplier<Query> topLevelQuery,
            @Nullable AggregationProfiler profiler,
            int maxBuckets,
            Supplier<SubSearchContext> subSearchContextBuilder,
            BitsetFilterCache bitsetFilterCache,
            int randomSeed,
            LongSupplier relativeTimeInMillis,
            Supplier<Boolean> isCancelled,
            Function<Query, Query> filterQuery,
            boolean enableRewriteToFilterByFilter,
            boolean inSortOrderExecutionRequired
        ) {
            this.analysisRegistry = analysisRegistry;
            this.context = context;
            this.clusterSettings = clusterSettings;
            if (bytesToPreallocate == 0) {
                /*
                 * Its possible if a bit strange for the aggregations to ask
                 * to preallocate 0 bytes. Mostly this is for testing other
                 * things, but we should honor it and just not preallocate
                 * anything. Setting the breakerService reference to null will
                 * cause us to skip it when we close this context.
                 */
                this.preallocatedBreakerService = null;
                this.bigArrays = bigArrays.withCircuitBreaking();
            } else {
                this.preallocatedBreakerService = new PreallocatedCircuitBreakerService(
                    bigArrays.breakerService(),
                    CircuitBreaker.REQUEST,
                    bytesToPreallocate,
                    "aggregations"
                );
                this.bigArrays = bigArrays.withBreakerService(preallocatedBreakerService).withCircuitBreaking();
            }
            this.topLevelQuery = topLevelQuery;
            this.profiler = profiler;
            this.maxBuckets = maxBuckets;
            this.subSearchContextBuilder = subSearchContextBuilder;
            this.bitsetFilterCache = bitsetFilterCache;
            this.randomSeed = randomSeed;
            this.relativeTimeInMillis = relativeTimeInMillis;
            this.isCancelled = isCancelled;
            this.filterQuery = filterQuery;
            this.enableRewriteToFilterByFilter = enableRewriteToFilterByFilter;
            this.inSortOrderExecutionRequired = inSortOrderExecutionRequired;
        }

        @Override
        public Query query() {
            return topLevelQuery.get();
        }

        @Override
        public Aggregator profileIfEnabled(Aggregator agg) {
            if (profiler == null) {
                return agg;
            }
            return new ProfilingAggregator(agg, profiler);
        }

        @Override
        public boolean profiling() {
            return profiler != null;
        }

        @Override
        public long nowInMillis() {
            return context.nowInMillis();
        }

        @Override
        public Analyzer getNamedAnalyzer(String analyzer) throws IOException {
            return analysisRegistry.getAnalyzer(analyzer);
        }

        @Override
        public Analyzer buildCustomAnalyzer(
            IndexSettings indexSettings,
            boolean normalizer,
            NameOrDefinition tokenizer,
            List<NameOrDefinition> charFilters,
            List<NameOrDefinition> tokenFilters
        ) throws IOException {
            return analysisRegistry.buildCustomAnalyzer(
                IndexService.IndexCreationContext.RELOAD_ANALYZERS,
                indexSettings,
                normalizer,
                tokenizer,
                charFilters,
                tokenFilters
            );
        }

        @Override
        protected IndexFieldData<?> buildFieldData(MappedFieldType ft) {
            return context.getForField(ft, MappedFieldType.FielddataOperation.SEARCH);
        }

        @Override
        public MappedFieldType getFieldType(String path) {
            return context.getFieldType(path);
        }

        @Override
        public Set<String> getMatchingFieldNames(String pattern) {
            return context.getMatchingFieldNames(pattern);
        }

        @Override
        public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> scriptContext) {
            return context.compile(script, scriptContext);
        }

        @Override
        public SearchLookup lookup() {
            return context.lookup();
        }

        @Override
        public ValuesSourceRegistry getValuesSourceRegistry() {
            return context.getValuesSourceRegistry();
        }

        @Override
        public BigArrays bigArrays() {
            return bigArrays;
        }

        @Override
        public IndexSearcher searcher() {
            return context.searcher();
        }

        @Override
        public Query buildQuery(QueryBuilder builder) throws IOException {
            return Rewriteable.rewrite(builder, context, true).toQuery(context);
        }

        @Override
        public Query filterQuery(Query query) {
            return filterQuery.apply(query);
        }

        @Override
        public IndexSettings getIndexSettings() {
            return context.getIndexSettings();
        }

        @Override
        public ClusterSettings getClusterSettings() {
            return clusterSettings;
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders) throws IOException {
            return SortBuilder.buildSort(sortBuilders, context);
        }

        @Override
        public NestedLookup nestedLookup() {
            return context.nestedLookup();
        }

        @Override
        public NestedScope nestedScope() {
            return context.nestedScope();
        }

        @Override
        public SubSearchContext subSearchContext() {
            return subSearchContextBuilder.get();
        }

        @Override
        public void addReleasable(Aggregator aggregator) {
            assert releaseMe.contains(aggregator) == false
                : "adding aggregator [" + aggregator.name() + "] twice in the aggregation context";
            releaseMe.add(aggregator);
        }

        @Override
        public synchronized void removeReleasable(Aggregator aggregator) {
            // Removing an aggregator is done after calling Aggregator#buildTopLevel which happens on an executor thread.
            // We need to synchronize the removal because he AggregatorContext it is shared between executor threads.
            assert releaseMe.contains(aggregator)
                : "removing non-existing aggregator [" + aggregator.name() + "] from the the aggregation context";
            releaseMe.remove(aggregator);
        }

        @Override
        public int maxBuckets() {
            return maxBuckets;
        }

        @Override
        public BitsetFilterCache bitsetFilterCache() {
            return bitsetFilterCache;
        }

        @Override
        public BucketedSort buildBucketedSort(SortBuilder<?> sort, int bucketSize, BucketedSort.ExtraData extra) throws IOException {
            return sort.buildBucketedSort(context, bigArrays, bucketSize, extra);
        }

        @Override
        public int shardRandomSeed() {
            return randomSeed;
        }

        @Override
        public long getRelativeTimeInMillis() {
            return relativeTimeInMillis.getAsLong();
        }

        @Override
        public boolean isCancelled() {
            return isCancelled.get();
        }

        @Override
        public CircuitBreaker breaker() {
            // preallocatedBreakerService may be null if we haven't preallocated so use the one in bigArrays.
            return bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        }

        @Override
        public Analyzer getIndexAnalyzer(Function<String, NamedAnalyzer> unindexedFieldAnalyzer) {
            return context.getIndexAnalyzer(unindexedFieldAnalyzer);
        }

        @Override
        public boolean isCacheable() {
            return context.isCacheable();
        }

        @Override
        public boolean enableRewriteToFilterByFilter() {
            return enableRewriteToFilterByFilter;
        }

        @Override
        public boolean isInSortOrderExecutionRequired() {
            return inSortOrderExecutionRequired;
        }

        @Override
        public Set<String> sourcePath(String fullName) {
            return context.sourcePath(fullName);
        }

        @Override
        public MappingLookup getMappingLookup() {
            return context.getMappingLookup();
        }

        @Override
        public void close() {
            /*
             * Add the breakerService to the end of the list so we release it
             * after all the aggregations that allocate bytes on it.
             */
            List<Releasable> releaseMe = new ArrayList<>(this.releaseMe);
            releaseMe.add(preallocatedBreakerService);
            Releasables.close(releaseMe);
        }
    }
}
