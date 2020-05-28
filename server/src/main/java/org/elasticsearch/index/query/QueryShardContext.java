/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.AggregationUsageService;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Context object used to create lucene queries on the shard level.
 */
public class QueryShardContext extends QueryRewriteContext {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(QueryShardContext.class));
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using the _type field " +
        "in queries and aggregations is deprecated, prefer to use a field instead.";

    private final ScriptService scriptService;
    private final IndexSettings indexSettings;
    private final BigArrays bigArrays;
    private final MapperService mapperService;
    private final SimilarityService similarityService;
    private final BitsetFilterCache bitsetFilterCache;
    private final BiFunction<MappedFieldType, String, IndexFieldData<?>> indexFieldDataService;
    private final int shardId;
    private final IndexSearcher searcher;
    private boolean cacheable = true;
    private final SetOnce<Boolean> frozen = new SetOnce<>();

    private final Index fullyQualifiedIndex;
    private final Predicate<String> indexNameMatcher;
    private final BooleanSupplier allowExpensiveQueries;

    private final Map<String, Query> namedQueries = new HashMap<>();
    private boolean allowUnmappedFields;
    private boolean mapUnmappedFieldAsString;
    private NestedScope nestedScope;
    private final ValuesSourceRegistry valuesSourceRegistry;

    public QueryShardContext(int shardId,
                             IndexSettings indexSettings,
                             BigArrays bigArrays,
                             BitsetFilterCache bitsetFilterCache,
                             BiFunction<MappedFieldType, String, IndexFieldData<?>> indexFieldDataLookup,
                             MapperService mapperService,
                             SimilarityService similarityService,
                             ScriptService scriptService,
                             NamedXContentRegistry xContentRegistry,
                             NamedWriteableRegistry namedWriteableRegistry,
                             Client client,
                             IndexSearcher searcher,
                             LongSupplier nowInMillis,
                             String clusterAlias,
                             Predicate<String> indexNameMatcher,
                             BooleanSupplier allowExpensiveQueries,
                             ValuesSourceRegistry valuesSourceRegistry) {
        this(shardId, indexSettings, bigArrays, bitsetFilterCache, indexFieldDataLookup, mapperService, similarityService,
                scriptService, xContentRegistry, namedWriteableRegistry, client, searcher, nowInMillis, indexNameMatcher,
                new Index(RemoteClusterAware.buildRemoteIndexName(clusterAlias, indexSettings.getIndex().getName()),
                        indexSettings.getIndex().getUUID()), allowExpensiveQueries, valuesSourceRegistry);
    }

    public QueryShardContext(QueryShardContext source) {
        this(source.shardId, source.indexSettings, source.bigArrays, source.bitsetFilterCache, source.indexFieldDataService,
            source.mapperService, source.similarityService, source.scriptService, source.getXContentRegistry(),
            source.getWriteableRegistry(), source.client, source.searcher, source.nowInMillis, source.indexNameMatcher,
            source.fullyQualifiedIndex, source.allowExpensiveQueries, source.valuesSourceRegistry);
    }

    private QueryShardContext(int shardId,
                              IndexSettings indexSettings,
                              BigArrays bigArrays,
                              BitsetFilterCache bitsetFilterCache,
                              BiFunction<MappedFieldType, String, IndexFieldData<?>> indexFieldDataLookup,
                              MapperService mapperService,
                              SimilarityService similarityService,
                              ScriptService scriptService,
                              NamedXContentRegistry xContentRegistry,
                              NamedWriteableRegistry namedWriteableRegistry,
                              Client client,
                              IndexSearcher searcher,
                              LongSupplier nowInMillis,
                              Predicate<String> indexNameMatcher,
                              Index fullyQualifiedIndex,
                              BooleanSupplier allowExpensiveQueries,
                              ValuesSourceRegistry valuesSourceRegistry) {
        super(xContentRegistry, namedWriteableRegistry, client, nowInMillis);
        this.shardId = shardId;
        this.similarityService = similarityService;
        this.mapperService = mapperService;
        this.bigArrays = bigArrays;
        this.bitsetFilterCache = bitsetFilterCache;
        this.indexFieldDataService = indexFieldDataLookup;
        this.allowUnmappedFields = indexSettings.isDefaultAllowUnmappedFields();
        this.nestedScope = new NestedScope();
        this.scriptService = scriptService;
        this.indexSettings = indexSettings;
        this.searcher = searcher;
        this.indexNameMatcher = indexNameMatcher;
        this.fullyQualifiedIndex = fullyQualifiedIndex;
        this.allowExpensiveQueries = allowExpensiveQueries;
        this.valuesSourceRegistry = valuesSourceRegistry;
    }

    private void reset() {
        allowUnmappedFields = indexSettings.isDefaultAllowUnmappedFields();
        this.lookup = null;
        this.namedQueries.clear();
        this.nestedScope = new NestedScope();
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return mapperService.getIndexAnalyzers();
    }

    public Similarity getSearchSimilarity() {
        return similarityService != null ? similarityService.similarity(mapperService) : null;
    }

    public List<String> defaultFields() {
        return indexSettings.getDefaultFields();
    }

    public boolean queryStringLenient() {
        return indexSettings.isQueryStringLenient();
    }

    public boolean queryStringAnalyzeWildcard() {
        return indexSettings.isQueryStringAnalyzeWildcard();
    }

    public boolean queryStringAllowLeadingWildcard() {
        return indexSettings.isQueryStringAllowLeadingWildcard();
    }

    public BitSetProducer bitsetFilter(Query filter) {
        return bitsetFilterCache.getBitSetProducer(filter);
    }

    public boolean allowExpensiveQueries() {
        return allowExpensiveQueries.getAsBoolean();
    }

    @SuppressWarnings("unchecked")
    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
        return (IFD) indexFieldDataService.apply(fieldType, fullyQualifiedIndex.getName());
    }

    public void addNamedQuery(String name, Query query) {
        if (query != null) {
            namedQueries.put(name, query);
        }
    }

    public Map<String, Query> copyNamedQueries() {
        // This might be a good use case for CopyOnWriteHashMap
        return Map.copyOf(namedQueries);
    }

    /**
     * Returns all the fields that match a given pattern. If prefixed with a
     * type then the fields will be returned with a type prefix.
     */
    public Set<String> simpleMatchToIndexNames(String pattern) {
        return mapperService.simpleMatchToFullName(pattern);
    }

    public MappedFieldType fieldMapper(String name) {
        if (name.equals(TypeFieldMapper.NAME)) {
            deprecationLogger.deprecatedAndMaybeLog("query_with_types", TYPES_DEPRECATION_MESSAGE);
        }
        return failIfFieldMappingNotFound(name, mapperService.fieldType(name));
    }

    public ObjectMapper getObjectMapper(String name) {
        return mapperService.getObjectMapper(name);
    }

    /**
     * Gets the search analyzer for the given field, or the default if there is none present for the field
     * TODO: remove this by moving defaults into mappers themselves
     */
    public Analyzer getSearchAnalyzer(MappedFieldType fieldType) {
        if (fieldType.searchAnalyzer() != null) {
            return fieldType.searchAnalyzer();
        }
        return getMapperService().searchAnalyzer();
    }

    /**
     * Gets the search quote analyzer for the given field, or the default if there is none present for the field
     * TODO: remove this by moving defaults into mappers themselves
     */
    public Analyzer getSearchQuoteAnalyzer(MappedFieldType fieldType) {
        if (fieldType.searchQuoteAnalyzer() != null) {
            return fieldType.searchQuoteAnalyzer();
        }
        return getMapperService().searchQuoteAnalyzer();
    }

    public ValuesSourceRegistry getValuesSourceRegistry() {
        return valuesSourceRegistry;
    }

    public void setAllowUnmappedFields(boolean allowUnmappedFields) {
        this.allowUnmappedFields = allowUnmappedFields;
    }

    public void setMapUnmappedFieldAsString(boolean mapUnmappedFieldAsString) {
        this.mapUnmappedFieldAsString = mapUnmappedFieldAsString;
    }

    MappedFieldType failIfFieldMappingNotFound(String name, MappedFieldType fieldMapping) {
        if (fieldMapping != null || allowUnmappedFields) {
            return fieldMapping;
        } else if (mapUnmappedFieldAsString) {
            TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name);
            return builder.build(new Mapper.BuilderContext(indexSettings.getSettings(), new ContentPath(1))).fieldType();
        } else {
            throw new QueryShardException(this, "No field mapping can be found for the field with name [{}]", name);
        }
    }

    private SearchLookup lookup = null;

    public SearchLookup lookup() {
        if (lookup == null) {
            lookup = new SearchLookup(getMapperService(),
                    mappedFieldType -> indexFieldDataService.apply(mappedFieldType, fullyQualifiedIndex.getName()));
        }
        return lookup;
    }

    public NestedScope nestedScope() {
        return nestedScope;
    }

    public Version indexVersionCreated() {
        return indexSettings.getIndexVersionCreated();
    }

    /**
     *  Given an index pattern, checks whether it matches against the current shard. The pattern
     *  may represent a fully qualified index name if the search targets remote shards.
     */
    public boolean indexMatches(String pattern) {
        return indexNameMatcher.test(pattern);
    }

    public boolean indexSortedOnField(String field) {
        IndexSortConfig indexSortConfig = indexSettings.getIndexSortConfig();
        return indexSortConfig.hasPrimarySortOnField(field);
    }

    public ParsedQuery toQuery(QueryBuilder queryBuilder) {
        return toQuery(queryBuilder, q -> {
            Query query = q.toQuery(this);
            if (query == null) {
                query = Queries.newMatchNoDocsQuery("No query left after rewrite.");
            }
            return query;
        });
    }

    private ParsedQuery toQuery(QueryBuilder queryBuilder, CheckedFunction<QueryBuilder, Query, IOException> filterOrQuery) {
        reset();
        try {
            QueryBuilder rewriteQuery = Rewriteable.rewrite(queryBuilder, this, true);
            return new ParsedQuery(filterOrQuery.apply(rewriteQuery), copyNamedQueries());
        } catch(QueryShardException | ParsingException e) {
            throw e;
        } catch(Exception e) {
            throw new QueryShardException(this, "failed to create query: {}", e, e.getMessage());
        } finally {
            reset();
        }
    }

    public Index index() {
        return indexSettings.getIndex();
    }

    /** Compile script using script service */
    public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
        FactoryType factory = scriptService.compile(script, context);
        if (factory instanceof ScriptFactory && ((ScriptFactory) factory).isResultDeterministic() == false) {
            failIfFrozen();
        }
        return factory;
    }

    /**
     * if this method is called the query context will throw exception if methods are accessed
     * that could yield different results across executions like {@link #getClient()}
     */
    public final void freezeContext() {
        this.frozen.set(Boolean.TRUE);
    }

    /**
     * This method fails if {@link #freezeContext()} is called before on this
     * context. This is used to <i>seal</i>.
     *
     * This methods and all methods that call it should be final to ensure that
     * setting the request as not cacheable and the freezing behaviour of this
     * class cannot be bypassed. This is important so we can trust when this
     * class says a request can be cached.
     */
    protected final void failIfFrozen() {
        this.cacheable = false;
        if (frozen.get() == Boolean.TRUE) {
            throw new IllegalArgumentException("features that prevent cachability are disabled on this context");
        } else {
            assert frozen.get() == null : frozen.get();
        }
    }

    @Override
    public void registerAsyncAction(BiConsumer<Client, ActionListener<?>> asyncAction) {
        failIfFrozen();
        super.registerAsyncAction(asyncAction);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void executeAsyncActions(ActionListener listener) {
        failIfFrozen();
        super.executeAsyncActions(listener);
    }

    /**
     * Returns <code>true</code> iff the result of the processed search request is cacheable. Otherwise <code>false</code>
     */
    public final boolean isCacheable() {
        return cacheable;
    }

    /**
     * Returns the shard ID this context was created for.
     */
    public int getShardId() {
        return shardId;
    }

    @Override
    public final long nowInMillis() {
        failIfFrozen();
        return super.nowInMillis();
    }

    public Client getClient() {
        failIfFrozen(); // we somebody uses a terms filter with lookup for instance can't be cached...
        return client;
    }

    public QueryBuilder parseInnerQueryBuilder(XContentParser parser) throws IOException {
        return AbstractQueryBuilder.parseInnerQueryBuilder(parser);
    }

    @Override
    public final QueryShardContext convertToShardContext() {
        return this;
    }

    /**
     * Returns the index settings for this context. This might return null if the
     * context has not index scope.
     */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Return the MapperService.
     */
    public MapperService getMapperService() {
        return mapperService;
    }

    /** Return the current {@link IndexReader}, or {@code null} if no index reader is available,
     *  for instance if this rewrite context is used to index queries (percolation). */
    public IndexReader getIndexReader() {
        return searcher == null ? null : searcher.getIndexReader();
    }

    /** Return the current {@link IndexSearcher}, or {@code null} if no index reader is available,
     *  for instance if this rewrite context is used to index queries (percolation). */
    public IndexSearcher searcher() {
        return searcher;
    }

    /**
     * Returns the fully qualified index including a remote cluster alias if applicable, and the index uuid
     */
    public Index getFullyQualifiedIndex() {
        return fullyQualifiedIndex;
    }

    /**
     * Return the {@link BigArrays} instance for this node.
     */
    public BigArrays bigArrays() {
        return bigArrays;
    }

    public SimilarityService getSimilarityService() {
        return similarityService;
    }

    public BitsetFilterCache getBitsetFilterCache() {
        return bitsetFilterCache;
    }

    public AggregationUsageService getUsageService() {
        return valuesSourceRegistry.getUsageService();
    }
}
