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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.percolator.PercolatorQueryCache;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsSubSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Context object used to create lucene queries on the shard level.
 */
public class QueryShardContext extends QueryRewriteContext {

    private final MapperService mapperService;
    private final SimilarityService similarityService;
    private final BitsetFilterCache bitsetFilterCache;
    private final IndexFieldDataService indexFieldDataService;
    private final IndexSettings indexSettings;
    private String[] types = Strings.EMPTY_ARRAY;

    public void setTypes(String... types) {
        this.types = types;
    }

    public String[] getTypes() {
        return types;
    }

    private final Map<String, Query> namedQueries = new HashMap<>();
    private final MapperQueryParser queryParser = new MapperQueryParser(this);
    private final IndicesQueriesRegistry indicesQueriesRegistry;
    private final PercolatorQueryCache percolatorQueryCache;
    private boolean allowUnmappedFields;
    private boolean mapUnmappedFieldAsString;
    private NestedScope nestedScope;
    boolean isFilter; // pkg private for testing

    public QueryShardContext(IndexSettings indexSettings, BitsetFilterCache bitsetFilterCache, IndexFieldDataService indexFieldDataService, MapperService mapperService, SimilarityService similarityService, ScriptService scriptService,
                             final IndicesQueriesRegistry indicesQueriesRegistry, PercolatorQueryCache percolatorQueryCache) {
        super(indexSettings, scriptService, indicesQueriesRegistry);
        this.indexSettings = indexSettings;
        this.similarityService = similarityService;
        this.mapperService = mapperService;
        this.bitsetFilterCache = bitsetFilterCache;
        this.indexFieldDataService = indexFieldDataService;
        this.allowUnmappedFields = indexSettings.isDefaultAllowUnmappedFields();
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        this.percolatorQueryCache = percolatorQueryCache;
        this.nestedScope = new NestedScope();
    }

    public QueryShardContext(QueryShardContext source) {
        this(source.indexSettings, source.bitsetFilterCache, source.indexFieldDataService, source.mapperService, source.similarityService, source.scriptService, source.indicesQueriesRegistry, source.percolatorQueryCache);
        this.types = source.getTypes();
    }


    @Override
    public QueryShardContext clone() {
        return new QueryShardContext(indexSettings, bitsetFilterCache, indexFieldDataService, mapperService, similarityService, scriptService, indicesQueriesRegistry, percolatorQueryCache);
    }

    public void parseFieldMatcher(ParseFieldMatcher parseFieldMatcher) {
        this.parseContext.parseFieldMatcher(parseFieldMatcher);
    }

    public ParseFieldMatcher parseFieldMatcher() {
        return parseContext.parseFieldMatcher();
    }

    public void reset() {
        allowUnmappedFields = indexSettings.isDefaultAllowUnmappedFields();
        this.parseFieldMatcher(ParseFieldMatcher.EMPTY);
        this.lookup = null;
        this.namedQueries.clear();
        this.nestedScope = new NestedScope();
        this.isFilter = false;
    }

    public void reset(XContentParser jp) {
        this.reset();
        this.parseContext.reset(jp);
    }

    public InnerHitsSubSearchContext getInnerHitsContext(XContentParser parser) throws IOException {
        return InnerHitsQueryParserHelper.parse(parser);
    }

    public AnalysisService getAnalysisService() {
        return mapperService.analysisService();
    }

    public MapperService getMapperService() {
        return mapperService;
    }

    public PercolatorQueryCache getPercolatorQueryCache() {
        return percolatorQueryCache;
    }

    public Similarity getSearchSimilarity() {
        return similarityService != null ? similarityService.similarity(mapperService) : null;
    }

    public String defaultField() {
        return indexSettings.getDefaultField();
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

    public MapperQueryParser queryParser(QueryParserSettings settings) {
        queryParser.reset(settings);
        return queryParser;
    }

    public BitSetProducer bitsetFilter(Query filter) {
        return bitsetFilterCache.getBitSetProducer(filter);
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType mapper) {
        return indexFieldDataService.getForField(mapper);
    }

    public void addNamedQuery(String name, Query query) {
        if (query != null) {
            namedQueries.put(name, query);
        }
    }

    public Map<String, Query> copyNamedQueries() {
        // This might be a good use case for CopyOnWriteHashMap
        return unmodifiableMap(new HashMap<>(namedQueries));
    }

    /**
     * Return whether we are currently parsing a filter or a query.
     */
    public boolean isFilter() {
        return isFilter;
    }

    public void addInnerHits(String name, InnerHitsContext.BaseInnerHits context) {
        SearchContext sc = SearchContext.current();
        if (sc == null) {
            throw new QueryShardException(this, "inner_hits unsupported");
        }

        InnerHitsContext innerHitsContext = sc.innerHits();
        innerHitsContext.addInnerHitDefinition(name, context);
    }

    public Collection<String> simpleMatchToIndexNames(String pattern) {
        return mapperService.simpleMatchToIndexNames(pattern);
    }

    public MappedFieldType fieldMapper(String name) {
        return failIfFieldMappingNotFound(name, mapperService.fullName(name));
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

    /**
     * Returns the narrowed down explicit types, or, if not set, all types.
     */
    public Collection<String> queryTypes() {
        String[] types = getTypes();
        if (types == null || types.length == 0) {
            return getMapperService().types();
        }
        if (types.length == 1 && types[0].equals("_all")) {
            return getMapperService().types();
        }
        return Arrays.asList(types);
    }

    private SearchLookup lookup = null;

    public SearchLookup lookup() {
        SearchContext current = SearchContext.current();
        if (current != null) {
            return current.lookup();
        }
        if (lookup == null) {
            lookup = new SearchLookup(getMapperService(), indexFieldDataService, null);
        }
        return lookup;
    }

    public long nowInMillis() {
        SearchContext current = SearchContext.current();
        if (current != null) {
            return current.nowInMillis();
        }
        return System.currentTimeMillis();
    }

    public NestedScope nestedScope() {
        return nestedScope;
    }

    public Version indexVersionCreated() {
        return indexSettings.getIndexVersionCreated();
    }

    public QueryParseContext parseContext() {
        return this.parseContext;
    }

    public boolean matchesIndices(String... indices) {
        for (String index : indices) {
            if (indexSettings.matchesIndexName(index)) {
                return true;
            }
        }
        return false;
    }

    public ParsedQuery parse(BytesReference source) {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            return innerParse(parser);
        } catch (ParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new ParsingException(parser == null ? null : parser.getTokenLocation(), "Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public ParsedQuery parse(XContentParser parser) {
        try {
            return innerParse(parser);
        } catch(IOException e) {
            throw new ParsingException(parser.getTokenLocation(), "Failed to parse", e);
        }
    }

    /**
     * Parses an inner filter, returning null if the filter should be ignored.
     */
    @Nullable
    public ParsedQuery parseInnerFilter(XContentParser parser) throws IOException {
        reset(parser);
        try {
            parseFieldMatcher(indexSettings.getParseFieldMatcher());
            Query filter = QueryBuilder.rewriteQuery(parseContext().parseInnerQueryBuilder(), this).toFilter(this);
            if (filter == null) {
                return null;
            }
            return new ParsedQuery(filter, copyNamedQueries());
        } finally {
            reset(null);
        }
    }


    private ParsedQuery innerParse(XContentParser parser) throws IOException, QueryShardException {
        reset(parser);
        try {
            parseFieldMatcher(indexSettings.getParseFieldMatcher());
            Query query = parseInnerQuery();
            return new ParsedQuery(query, copyNamedQueries());
        } finally {
            reset(null);
        }
    }

    public Query parseInnerQuery() throws IOException {
        return toQuery(this.parseContext().parseInnerQueryBuilder(), this);
    }

    public ParsedQuery toQuery(QueryBuilder<?> queryBuilder) {
        reset();
        parseFieldMatcher(indexSettings.getParseFieldMatcher());
        try {
            Query query = toQuery(queryBuilder, this);
            return new ParsedQuery(query, copyNamedQueries());
        } catch(QueryShardException | ParsingException e ) {
            throw e;
        } catch(Exception e) {
            throw new QueryShardException(this, "failed to create query: {}", e, queryBuilder);
        } finally {
            this.reset();
        }
    }

    private static Query toQuery(final QueryBuilder<?> queryBuilder, final QueryShardContext context) throws IOException {
        final Query query = QueryBuilder.rewriteQuery(queryBuilder, context).toQuery(context);
        if (query == null) {
            return Queries.newMatchNoDocsQuery();
        }
        return query;
    }

    public final Index index() {
        return indexSettings.getIndex();
    }

}
