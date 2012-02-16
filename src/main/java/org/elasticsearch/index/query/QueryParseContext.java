/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.queryParser.MapperQueryParser;
import org.apache.lucene.queryParser.MultiFieldMapperQueryParser;
import org.apache.lucene.queryParser.MultiFieldQueryParserSettings;
import org.apache.lucene.queryParser.QueryParserSettings;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class QueryParseContext {

    private static ThreadLocal<String[]> typesContext = new ThreadLocal<String[]>();

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

    IndexQueryParserService indexQueryParser;

    private final Map<String, Filter> namedFilters = Maps.newHashMap();

    private final MapperQueryParser queryParser = new MapperQueryParser(this);

    private final MultiFieldMapperQueryParser multiFieldQueryParser = new MultiFieldMapperQueryParser(this);

    private XContentParser parser;

    public QueryParseContext(Index index, IndexQueryParserService indexQueryParser) {
        this.index = index;
        this.indexQueryParser = indexQueryParser;
    }

    public void reset(XContentParser jp) {
        this.lookup = null;
        this.parser = jp;
        this.namedFilters.clear();
    }

    public Index index() {
        return this.index;
    }

    public XContentParser parser() {
        return parser;
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

    public IndexEngine indexEngine() {
        return indexQueryParser.indexEngine;
    }

    @Nullable
    public SimilarityService similarityService() {
        return indexQueryParser.similarityService;
    }

    public Similarity searchSimilarity() {
        return indexQueryParser.similarityService != null ? indexQueryParser.similarityService.defaultSearchSimilarity() : null;
    }

    public IndexCache indexCache() {
        return indexQueryParser.indexCache;
    }

    public String defaultField() {
        return indexQueryParser.defaultField();
    }

    public MapperQueryParser singleQueryParser(QueryParserSettings settings) {
        queryParser.reset(settings);
        return queryParser;
    }

    public MultiFieldMapperQueryParser multiQueryParser(MultiFieldQueryParserSettings settings) {
        multiFieldQueryParser.reset(settings);
        return multiFieldQueryParser;
    }

    public Filter cacheFilter(Filter filter, @Nullable CacheKeyFilter.Key cacheKey) {
        if (cacheKey != null) {
            filter = new CacheKeyFilter.Wrapper(filter, cacheKey);
        }
        return indexQueryParser.indexCache.filter().cache(filter);
    }

    public void addNamedFilter(String name, Filter filter) {
        namedFilters.put(name, filter);
    }

    public ImmutableMap<String, Filter> copyNamedFilters() {
        if (namedFilters.isEmpty()) {
            return ImmutableMap.of();
        }
        return ImmutableMap.copyOf(namedFilters);
    }

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
        return result;
    }

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
        Filter result = filterParser.parse(this);
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
        Filter result = filterParser.parse(this);
        // don't move to the nextToken in this case...
//        if (parser.currentToken() == XContentParser.Token.END_OBJECT || parser.currentToken() == XContentParser.Token.END_ARRAY) {
//            // if we are at END_OBJECT, move to the next one...
//            parser.nextToken();
//        }
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

    public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
        return indexQueryParser.mapperService.smartName(name, getTypes());
    }

    public MapperService.SmartNameObjectMapper smartObjectMapper(String name) {
        return indexQueryParser.mapperService.smartNameObjectMapper(name, getTypes());
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
            lookup = new SearchLookup(mapperService(), indexCache().fieldData());
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
}
