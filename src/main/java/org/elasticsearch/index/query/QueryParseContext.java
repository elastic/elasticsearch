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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.lucene.search.NoCacheQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.ResolvableFilter;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.query.parser.QueryParserCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.search.child.CustomQueryWrappingFilter;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class QueryParseContext {

    private static ThreadLocal<String[]> typesContext = new ThreadLocal<>();

    public static void setTypes(String[] types) {
        typesContext.set(types);
    }

    public static String[] getTypes() {
        return typesContext.get();
    }

    public static String[] setTypesWithPrevious(String[] types) {
        String[] old = typesContext.get();
        setTypes(types);
        return old;
    }

    public static void removeTypes() {
        typesContext.remove();
    }

    private final Index index;

    private boolean propagateNoCache = false;

    private boolean requireCustomQueryWrappingFilter = false;

    private final IndexQueryParserService indexQueryParser;

    private final Map<String, Filter> namedFilters = Maps.newHashMap();

    private final MapperQueryParser queryParser = new MapperQueryParser(this);

    private XContentParser parser;

    private EnumSet<ParseField.Flag> parseFlags = ParseField.EMPTY_FLAGS;

    private final boolean disableFilterCaching;

    private boolean allowUnmappedFields;

    private boolean mapUnmappedFieldAsString;

    private NestedScope nestedScope;

    public QueryParseContext(Index index, IndexQueryParserService indexQueryParser) {
        this(index, indexQueryParser, false);
    }

    public QueryParseContext(Index index, IndexQueryParserService indexQueryParser, boolean disableFilterCaching) {
        this.index = index;
        this.indexQueryParser = indexQueryParser;
        this.propagateNoCache = disableFilterCaching;
        this.disableFilterCaching = disableFilterCaching;
    }

    public void parseFlags(EnumSet<ParseField.Flag> parseFlags) {
        this.parseFlags = parseFlags == null ? ParseField.EMPTY_FLAGS : parseFlags;
    }

    public EnumSet<ParseField.Flag> parseFlags() {
        return parseFlags;
    }

    public void reset(XContentParser jp) {
        allowUnmappedFields = indexQueryParser.defaultAllowUnmappedFields();
        this.parseFlags = ParseField.EMPTY_FLAGS;
        this.lookup = null;
        this.parser = jp;
        this.namedFilters.clear();
        this.requireCustomQueryWrappingFilter = false;
        this.propagateNoCache = false;
        this.nestedScope = new NestedScope();
    }

    public Index index() {
        return this.index;
    }

    public void parser(XContentParser parser) {
        this.parser = parser;
    }

    public XContentParser parser() {
        return parser;
    }
    
    public IndexQueryParserService indexQueryParserService() {
        return indexQueryParser;
    }

    public AnalysisService analysisService() {
        return indexQueryParser.analysisService;
    }

    public ScriptService scriptService() {
        return indexQueryParser.scriptService;
    }

    public MapperService mapperService() {
        return indexQueryParser.mapperService;
    }

    @Nullable
    public SimilarityService similarityService() {
        return indexQueryParser.similarityService;
    }

    public Similarity searchSimilarity() {
        return indexQueryParser.similarityService != null ? indexQueryParser.similarityService.similarity() : null;
    }

    public QueryParserCache queryParserCache() {
        return indexQueryParser.indexCache.queryParserCache();
    }

    public String defaultField() {
        return indexQueryParser.defaultField();
    }

    public QueryCachingPolicy autoFilterCachePolicy() {
        return indexQueryParser.autoFilterCachePolicy();
    }

    public QueryCachingPolicy parseFilterCachePolicy() throws IOException {
        final String text = parser.textOrNull();
        if (text == null || text.equals("auto")) {
            return autoFilterCachePolicy();
        } else if (parser.booleanValue()) {
            // cache without conditions on how many times the filter has been
            // used or what the produced DocIdSet looks like, but ONLY on large
            // segments to not pollute the cache
            return QueryCachingPolicy.CacheOnLargeSegments.DEFAULT;
        } else {
            return null;
        }
    }

    public boolean queryStringLenient() {
        return indexQueryParser.queryStringLenient();
    }

    public MapperQueryParser queryParser(QueryParserSettings settings) {
        queryParser.reset(settings);
        return queryParser;
    }

    public BitDocIdSetFilter bitsetFilter(Filter filter) {
        return indexQueryParser.bitsetFilterCache.getBitDocIdSetFilter(filter);
    }

    public Filter cacheFilter(Filter filter, final @Nullable HashedBytesRef cacheKey, final QueryCachingPolicy cachePolicy) {
        if (filter == null) {
            return null;
        }
        if (this.disableFilterCaching || this.propagateNoCache || filter instanceof NoCacheFilter) {
            return filter;
        }
        if (filter instanceof ResolvableFilter) {
            final ResolvableFilter resolvableFilter = (ResolvableFilter) filter;
            // We need to wrap it another filter, because this method is invoked at query parse time, which
            // may not be during search execution time. (for example index alias filter and percolator)
            return new Filter() {
                @Override
                public DocIdSet getDocIdSet(LeafReaderContext atomicReaderContext, Bits bits) throws IOException {
                    Filter filter = resolvableFilter.resolve();
                    if (filter == null) {
                        return null;
                    }
                    filter = indexQueryParser.indexCache.filter().cache(filter, cacheKey, cachePolicy);
                    return filter.getDocIdSet(atomicReaderContext, bits);
                }

                @Override
                public String toString(String field) {
                    return "AnonymousResolvableFilter"; // TODO: not sure what is going on here
                }
            };
        } else {
            return indexQueryParser.indexCache.filter().cache(filter, cacheKey, cachePolicy);
        }
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(FieldMapper<?> mapper) {
        return indexQueryParser.fieldDataService.getForField(mapper);
    }

    public void addNamedFilter(String name, Filter filter) {
        namedFilters.put(name, filter);
    }

    public void addNamedQuery(String name, Query query) {
        namedFilters.put(name, Queries.wrap(query, this));
    }

    public ImmutableMap<String, Filter> copyNamedFilters() {
        return ImmutableMap.copyOf(namedFilters);
    }

    public void combineNamedFilters(QueryParseContext context) {
        namedFilters.putAll(context.namedFilters);
    }

    public void addInnerHits(String name, InnerHitsContext.BaseInnerHits context) {
        SearchContext sc = SearchContext.current();
        InnerHitsContext innerHitsContext;
        if (sc.innerHits() == null) {
            innerHitsContext = new InnerHitsContext(new HashMap<String, InnerHitsContext.BaseInnerHits>());
            sc.innerHits(innerHitsContext);
        } else {
            innerHitsContext = sc.innerHits();
        }
        innerHitsContext.addInnerHitDefinition(name, context);
    }

    @Nullable
    public Query parseInnerQuery() throws IOException, QueryParsingException {
        // move to START object
        XContentParser.Token token;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new QueryParsingException(index, "[_na] query malformed, must start with start_object");
            }
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(index, "[_na] query malformed, no field after start_object");
        }
        String queryName = parser.currentName();
        // move to the next START_OBJECT
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT && token != XContentParser.Token.START_ARRAY) {
            throw new QueryParsingException(index, "[_na] query malformed, no field after start_object");
        }

        QueryParser queryParser = indexQueryParser.queryParser(queryName);
        if (queryParser == null) {
            throw new QueryParsingException(index, "No query registered for [" + queryName + "]");
        }
        Query result = queryParser.parse(this);
        if (parser.currentToken() == XContentParser.Token.END_OBJECT || parser.currentToken() == XContentParser.Token.END_ARRAY) {
            // if we are at END_OBJECT, move to the next one...
            parser.nextToken();
        }
        if (result instanceof NoCacheQuery) {
            propagateNoCache = true;
        }
        if (CustomQueryWrappingFilter.shouldUseCustomQueryWrappingFilter(result)) {
            requireCustomQueryWrappingFilter = true;
            // If later on, either directly or indirectly this query gets wrapped in a query filter it must never
            // get cached even if a filter higher up the chain is configured to do this. This will happen, because
            // the result filter will be instance of NoCacheFilter (CustomQueryWrappingFilter) which will in
            // #executeFilterParser() set propagateNoCache to true.
        }
        return result;
    }

    @Nullable
    public Filter parseInnerFilter() throws IOException, QueryParsingException {
        // move to START object
        XContentParser.Token token;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new QueryParsingException(index, "[_na] filter malformed, must start with start_object");
            }
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            // empty filter
            if (token == XContentParser.Token.END_OBJECT || token == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            throw new QueryParsingException(index, "[_na] filter malformed, no field after start_object");
        }
        String filterName = parser.currentName();
        // move to the next START_OBJECT or START_ARRAY
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT && token != XContentParser.Token.START_ARRAY) {
            throw new QueryParsingException(index, "[_na] filter malformed, no field after start_object");
        }

        FilterParser filterParser = indexQueryParser.filterParser(filterName);
        if (filterParser == null) {
            throw new QueryParsingException(index, "No filter registered for [" + filterName + "]");
        }
        Filter result = executeFilterParser(filterParser);
        if (parser.currentToken() == XContentParser.Token.END_OBJECT || parser.currentToken() == XContentParser.Token.END_ARRAY) {
            // if we are at END_OBJECT, move to the next one...
            parser.nextToken();
        }
        return result;
    }

    public Filter parseInnerFilter(String filterName) throws IOException, QueryParsingException {
        FilterParser filterParser = indexQueryParser.filterParser(filterName);
        if (filterParser == null) {
            throw new QueryParsingException(index, "No filter registered for [" + filterName + "]");
        }
        return executeFilterParser(filterParser);
    }

    private Filter executeFilterParser(FilterParser filterParser) throws IOException {
        final boolean propagateNoCache = this.propagateNoCache; // first safe the state that we need to restore
        this.propagateNoCache = false; // parse the subfilter with caching, that's fine
        Filter result = filterParser.parse(this);
        // now make sure we set propagateNoCache to true if it is true already or if the result is
        // an instance of NoCacheFilter or if we used to be true! all filters above will
        // be not cached ie. wrappers of this filter!
        this.propagateNoCache |= (result instanceof NoCacheFilter) || propagateNoCache;
        return result;
    }

    public FieldMapper fieldMapper(String name) {
        FieldMappers fieldMappers = indexQueryParser.mapperService.smartNameFieldMappers(name, getTypes());
        if (fieldMappers == null) {
            return null;
        }
        return fieldMappers.mapper();
    }

    public String indexName(String name) {
        FieldMapper smartMapper = fieldMapper(name);
        if (smartMapper == null) {
            return name;
        }
        return smartMapper.names().indexName();
    }

    public List<String> simpleMatchToIndexNames(String pattern) {
        return indexQueryParser.mapperService.simpleMatchToIndexNames(pattern, getTypes());
    }

    public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
        return failIfFieldMappingNotFound(name, indexQueryParser.mapperService.smartName(name, getTypes()));
    }

    public MapperService.SmartNameObjectMapper smartObjectMapper(String name) {
        return indexQueryParser.mapperService.smartNameObjectMapper(name, getTypes());
    }

    public void setAllowUnmappedFields(boolean allowUnmappedFields) {
        this.allowUnmappedFields = allowUnmappedFields;
    }

    public void setMapUnmappedFieldAsString(boolean mapUnmappedFieldAsString) {
        this.mapUnmappedFieldAsString = mapUnmappedFieldAsString;
    }

    private MapperService.SmartNameFieldMappers failIfFieldMappingNotFound(String name, MapperService.SmartNameFieldMappers fieldMapping) {
        if (allowUnmappedFields) {
            return fieldMapping;
        } else if (mapUnmappedFieldAsString){
            StringFieldMapper.Builder builder = MapperBuilders.stringField(name);
            // it would be better to pass the real index settings, but they are not easily accessible from here...
            Settings settings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, indexQueryParser.getIndexCreatedVersion()).build();
            StringFieldMapper stringFieldMapper = builder.build(new Mapper.BuilderContext(settings, new ContentPath(1)));
            return new MapperService.SmartNameFieldMappers(mapperService(), new FieldMappers(stringFieldMapper), null, false);
        } else {
            Version indexCreatedVersion = indexQueryParser.getIndexCreatedVersion();
            if (fieldMapping == null && indexCreatedVersion.onOrAfter(Version.V_1_4_0_Beta1)) {
                throw new QueryParsingException(index, "Strict field resolution and no field mapping can be found for the field with name [" + name + "]");
            } else {
                return fieldMapping;
            }
        }
    }

    /**
     * Returns the narrowed down explicit types, or, if not set, all types.
     */
    public Collection<String> queryTypes() {
        String[] types = getTypes();
        if (types == null || types.length == 0) {
            return mapperService().types();
        }
        if (types.length == 1 && types[0].equals("_all")) {
            return mapperService().types();
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
            lookup = new SearchLookup(mapperService(), indexQueryParser.fieldDataService, null);
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

    public boolean requireCustomQueryWrappingFilter() {
        return requireCustomQueryWrappingFilter;
    }

    public NestedScope nestedScope() {
        return nestedScope;
    }
}
