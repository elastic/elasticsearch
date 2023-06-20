/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.FielddataOperation;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.lookup.LeafFieldLookupProvider;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static org.elasticsearch.index.IndexService.parseRuntimeMappings;

/**
 * The context used to execute a search request on a shard. It provides access
 * to required information like mapping definitions and document data.
 *
 * This context is used in several components of search execution, including
 * building queries and fetching hits.
 */
public class SearchExecutionContext extends QueryRewriteContext {

    private final SimilarityService similarityService;
    private final BitsetFilterCache bitsetFilterCache;
    private final BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> indexFieldDataLookup;
    private SearchLookup lookup = null;

    private final int shardId;
    private final int shardRequestIndex;
    private final IndexSearcher searcher;
    private boolean cacheable = true;
    private final SetOnce<Boolean> frozen = new SetOnce<>();
    private Set<String> fieldsInIndex = null;

    private final Map<String, Query> namedQueries = new HashMap<>();
    private NestedScope nestedScope;

    /**
     * Build a {@linkplain SearchExecutionContext}.
     */
    public SearchExecutionContext(
        int shardId,
        int shardRequestIndex,
        IndexSettings indexSettings,
        BitsetFilterCache bitsetFilterCache,
        BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> indexFieldDataLookup,
        MapperService mapperService,
        MappingLookup mappingLookup,
        SimilarityService similarityService,
        ScriptService scriptService,
        XContentParserConfiguration parserConfiguration,
        NamedWriteableRegistry namedWriteableRegistry,
        Client client,
        IndexSearcher searcher,
        LongSupplier nowInMillis,
        String clusterAlias,
        Predicate<String> indexNameMatcher,
        BooleanSupplier allowExpensiveQueries,
        ValuesSourceRegistry valuesSourceRegistry,
        Map<String, Object> runtimeMappings
    ) {
        this(
            shardId,
            shardRequestIndex,
            indexSettings,
            bitsetFilterCache,
            indexFieldDataLookup,
            mapperService,
            mappingLookup,
            similarityService,
            scriptService,
            parserConfiguration,
            namedWriteableRegistry,
            client,
            searcher,
            nowInMillis,
            indexNameMatcher,
            new Index(
                RemoteClusterAware.buildRemoteIndexName(clusterAlias, indexSettings.getIndex().getName()),
                indexSettings.getIndex().getUUID()
            ),
            allowExpensiveQueries,
            valuesSourceRegistry,
            parseRuntimeMappings(runtimeMappings, mapperService, indexSettings, mappingLookup),
            null
        );
    }

    public SearchExecutionContext(SearchExecutionContext source) {
        this(
            source.shardId,
            source.shardRequestIndex,
            source.indexSettings,
            source.bitsetFilterCache,
            source.indexFieldDataLookup,
            source.mapperService,
            source.mappingLookup,
            source.similarityService,
            source.scriptService,
            source.getParserConfig(),
            source.getWriteableRegistry(),
            source.client,
            source.searcher,
            source.nowInMillis,
            source.indexNameMatcher,
            source.getFullyQualifiedIndex(),
            source.allowExpensiveQueries,
            source.getValuesSourceRegistry(),
            source.runtimeMappings,
            source.allowedFields
        );
    }

    private SearchExecutionContext(
        int shardId,
        int shardRequestIndex,
        IndexSettings indexSettings,
        BitsetFilterCache bitsetFilterCache,
        BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> indexFieldDataLookup,
        MapperService mapperService,
        MappingLookup mappingLookup,
        SimilarityService similarityService,
        ScriptService scriptService,
        XContentParserConfiguration parserConfig,
        NamedWriteableRegistry namedWriteableRegistry,
        Client client,
        IndexSearcher searcher,
        LongSupplier nowInMillis,
        Predicate<String> indexNameMatcher,
        Index fullyQualifiedIndex,
        BooleanSupplier allowExpensiveQueries,
        ValuesSourceRegistry valuesSourceRegistry,
        Map<String, MappedFieldType> runtimeMappings,
        Predicate<String> allowedFields
    ) {
        super(
            parserConfig,
            client,
            nowInMillis,
            mapperService,
            mappingLookup,
            runtimeMappings,
            allowedFields,
            indexSettings,
            fullyQualifiedIndex,
            indexNameMatcher,
            namedWriteableRegistry,
            valuesSourceRegistry,
            allowExpensiveQueries,
            scriptService
        );
        this.shardId = shardId;
        this.shardRequestIndex = shardRequestIndex;
        this.similarityService = similarityService;
        this.bitsetFilterCache = bitsetFilterCache;
        this.indexFieldDataLookup = indexFieldDataLookup;
        this.nestedScope = new NestedScope();
        this.searcher = searcher;
    }

    private void reset() {
        setAllowUnmappedFields(indexSettings.isDefaultAllowUnmappedFields());
        this.lookup = null;
        this.namedQueries.clear();
        this.nestedScope = new NestedScope();
    }

    /**
     * The similarity to use in searches, which takes into account per-field configuration.
     */
    public Similarity getSearchSimilarity() {
        return similarityService != null ? similarityService.similarity(this::fieldType) : null;
    }

    /**
     * The default similarity configured in the index settings.
     */
    public Similarity getDefaultSimilarity() {
        return similarityService != null ? similarityService.getDefaultSimilarity() : null;
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

    @SuppressWarnings("unchecked")
    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType, FielddataOperation fielddataOperation) {
        return (IFD) indexFieldDataLookup.apply(
            fieldType,
            new FieldDataContext(
                getFullyQualifiedIndex().getName(),
                () -> this.lookup().forkAndTrackFieldReferences(fieldType.name()),
                this::sourcePath,
                fielddataOperation
            )
        );
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
     * Parse a document with current mapping.
     */
    public ParsedDocument parseDocument(SourceToParse source) throws DocumentParsingException {
        return mapperService.documentParser().parseDocument(source, mappingLookup);
    }

    public NestedLookup nestedLookup() {
        return mappingLookup.nestedLookup();
    }

    public boolean hasMappings() {
        return mappingLookup.hasMappings();
    }

    /**
     * Returns the names of all mapped fields that match a given pattern
     *
     * All names returned by this method are guaranteed to resolve to a
     * MappedFieldType if passed to {@link #getFieldType(String)}
     *
     * @param pattern the field name pattern
     */
    public Set<String> getMatchingFieldNames(String pattern) {
        if (runtimeMappings.isEmpty()) {
            return mappingLookup.getMatchingFieldNames(pattern);
        }
        Set<String> matches = new HashSet<>(mappingLookup.getMatchingFieldNames(pattern));
        if ("*".equals(pattern)) {
            matches.addAll(runtimeMappings.keySet());
        } else if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcard
            if (runtimeMappings.containsKey(pattern)) {
                matches.add(pattern);
            }
        } else {
            for (String name : runtimeMappings.keySet()) {
                if (Regex.simpleMatch(pattern, name)) {
                    matches.add(name);
                }
            }
        }
        return matches;
    }

    /**
     * Returns true if the field identified by the provided name is mapped, false otherwise
     */
    public boolean isFieldMapped(String name) {
        return fieldType(name) != null;
    }

    public boolean isMetadataField(String field) {
        return mapperService.isMetadataField(field);
    }

    public boolean isMultiField(String field) {
        if (runtimeMappings.containsKey(field)) {
            return false;
        }
        return mapperService.isMultiField(field);
    }

    public Set<String> sourcePath(String fullName) {
        return mappingLookup.sourcePaths(fullName);
    }

    /**
     * Will there be {@code _source}.
     */
    public boolean isSourceEnabled() {
        return mappingLookup.isSourceEnabled();
    }

    /**
     * Does the source need to be rebuilt on the fly?
     */
    public boolean isSourceSynthetic() {
        return mappingLookup.isSourceSynthetic();
    }

    /**
     * Build something to load source {@code _source}.
     */
    public SourceLoader newSourceLoader(boolean forceSyntheticSource) {
        if (forceSyntheticSource) {
            return new SourceLoader.Synthetic(mappingLookup.getMapping());
        }
        return mappingLookup.newSourceLoader();
    }

    /**
     * Given a type (eg. long, string, ...), returns an anonymous field type that can be used for search operations.
     * Generally used to handle unmapped fields in the context of sorting.
     */
    public MappedFieldType buildAnonymousFieldType(String type) {
        MappingParserContext parserContext = mapperService.parserContext();
        Mapper.TypeParser typeParser = parserContext.typeParser(type);
        if (typeParser == null) {
            throw new IllegalArgumentException("No mapper found for type [" + type + "]");
        }
        Mapper.Builder builder = typeParser.parse("__anonymous_", Collections.emptyMap(), parserContext);
        Mapper mapper = builder.build(MapperBuilderContext.root(false));
        if (mapper instanceof FieldMapper) {
            return ((FieldMapper) mapper).fieldType();
        }
        throw new IllegalArgumentException("Mapper for type [" + type + "] must be a leaf field");
    }

    /**
     * Return the index-time analyzer for the current index
     * @param unindexedFieldAnalyzer    a function that builds an analyzer for unindexed fields
     */
    public Analyzer getIndexAnalyzer(Function<String, NamedAnalyzer> unindexedFieldAnalyzer) {
        return new DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
            @Override
            protected Analyzer getWrappedAnalyzer(String fieldName) {
                return mappingLookup.indexAnalyzer(fieldName, unindexedFieldAnalyzer);
            }
        };
    }

    public void setAllowedFields(Predicate<String> allowedFields) {
        this.allowedFields = allowedFields;
    }

    /**
     * Does the index analyzer for this field have token filters that may produce
     * backwards offsets in term vectors
     */
    public boolean containsBrokenAnalysis(String field) {
        NamedAnalyzer a = mappingLookup.indexAnalyzer(field, f -> null);
        return a == null ? false : a.containsBrokenAnalysis();
    }

    /**
     * Get the lookup to use during the search.
     */
    public SearchLookup lookup() {
        if (this.lookup == null) {
            SourceProvider sourceProvider = isSourceSynthetic() ? (ctx, doc) -> {
                throw new IllegalArgumentException("Cannot access source from scripts in synthetic mode");
            } : SourceProvider.fromStoredFields();
            setLookupProviders(sourceProvider, LeafFieldLookupProvider.fromStoredFields());
        }
        return this.lookup;
    }

    /**
     * Replace the standard source provider and field lookup provider on the SearchLookup
     *
     * Note that this will replace the current SearchLookup with a new one, but will not update
     * the source provider on previously build lookups. This method should only be called before
     * IndexReader access by the current context
     */
    public void setLookupProviders(
        SourceProvider sourceProvider,
        Function<LeafReaderContext, LeafFieldLookupProvider> fieldLookupProvider
    ) {
        // TODO can we assert that this is only called during FetchPhase?
        this.lookup = new SearchLookup(
            this::getFieldType,
            (fieldType, searchLookup, fielddataOperation) -> indexFieldDataLookup.apply(
                fieldType,
                new FieldDataContext(getFullyQualifiedIndex().getName(), searchLookup, this::sourcePath, fielddataOperation)
            ),
            sourceProvider,
            fieldLookupProvider
        );
    }

    public NestedScope nestedScope() {
        return nestedScope;
    }

    public IndexVersion indexVersionCreated() {
        return indexSettings.getIndexVersionCreated();
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
        } catch (QueryShardException | ParsingException e) {
            throw e;
        } catch (Exception e) {
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
        assert scriptService != null;
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
     * Marks this context as not cacheable.
     * This method fails if {@link #freezeContext()} is called before on this context.
     */
    public void disableCache() {
        failIfFrozen();
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

    /**
     * Returns the shard request ordinal that is used by the main search request
     * to reference this shard.
     */
    public int getShardRequestIndex() {
        return shardRequestIndex;
    }

    /**
     * Returns the time in milliseconds that is shared across all resources involved. Even across shards and nodes.
     */
    public long nowInMillis() {
        failIfFrozen();
        return nowInMillis.getAsLong();
    }

    public Client getClient() {
        failIfFrozen(); // we somebody uses a terms filter with lookup for instance can't be cached...
        return client;
    }

    @Override
    public final SearchExecutionContext convertToSearchExecutionContext() {
        return this;
    }

    /** Return the current {@link IndexReader}, or {@code null} if no index reader is available,
     *  for instance if this rewrite context is used to index queries (percolation).
     */
    public IndexReader getIndexReader() {
        return searcher == null ? null : searcher.getIndexReader();
    }

    /** Return the current {@link IndexSearcher}, or {@code null} if no index reader is available, which happens
     * if this rewrite context is used to index queries (percolation).
     */
    public IndexSearcher searcher() {
        return searcher;
    }

    /**
     * Is this field present in the underlying lucene index for the current shard?
     */
    public boolean fieldExistsInIndex(String fieldname) {
        if (searcher == null) {
            return false;
        }
        if (fieldsInIndex == null) {
            fieldsInIndex = new HashSet<>();
            for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                FieldInfos fis = ctx.reader().getFieldInfos();
                for (FieldInfo fi : fis) {
                    fieldsInIndex.add(fi.name);
                }
            }
        }
        return fieldsInIndex.contains(fieldname);
    }

    /**
     * Cache key for current mapping.
     */
    public MappingLookup.CacheKey mappingCacheKey() {
        return mappingLookup.cacheKey();
    }

    public NestedDocuments getNestedDocuments() {
        return new NestedDocuments(mappingLookup, bitsetFilterCache::getBitSetProducer, indexVersionCreated());
    }
}
