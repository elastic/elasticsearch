/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.queryParser.MapperQueryParser;
import org.apache.lucene.queryParser.MultiFieldMapperQueryParser;
import org.apache.lucene.queryParser.MultiFieldQueryParserSettings;
import org.apache.lucene.queryParser.QueryParserSettings;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.ScopePhase;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class QueryParseContext {

    private final Index index;

    XContentIndexQueryParser indexQueryParser;

    private final Map<String, Filter> namedFilters = Maps.newHashMap();

    private final List<ScopePhase> scopePhases = Lists.newArrayList();

    private final MapperQueryParser queryParser = new MapperQueryParser(this);

    private final MultiFieldMapperQueryParser multiFieldQueryParser = new MultiFieldMapperQueryParser(this);

    private XContentParser parser;

    public QueryParseContext(Index index, XContentIndexQueryParser indexQueryParser) {
        this.index = index;
        this.indexQueryParser = indexQueryParser;
    }

    public void reset(XContentParser jp) {
        this.parser = jp;
        this.namedFilters.clear();
        this.scopePhases.clear();
    }

    public XContentParser parser() {
        return parser;
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

    @Nullable public SimilarityService similarityService() {
        return indexQueryParser.similarityService;
    }

    public Similarity searchSimilarity() {
        return indexQueryParser.similarityService != null ? indexQueryParser.similarityService.defaultSearchSimilarity() : null;
    }

    public IndexCache indexCache() {
        return indexQueryParser.indexCache;
    }

    public MapperQueryParser queryParser(QueryParserSettings settings) {
        queryParser.reset(settings);
        return queryParser;
    }

    public MultiFieldMapperQueryParser queryParser(MultiFieldQueryParserSettings settings) {
        multiFieldQueryParser.reset(settings);
        return multiFieldQueryParser;
    }

    public Filter cacheFilter(Filter filter) {
        return indexQueryParser.indexCache.filter().cache(filter);
    }

    public Filter cacheWeakFilter(Filter filter) {
        return indexQueryParser.indexCache.filter().weakCache(filter);
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

    public void addScopePhase(ScopePhase scopePhase) {
        scopePhases.add(scopePhase);
    }

    private static ScopePhase[] EMPTY_SCOPE_PHASES = new ScopePhase[0];

    public ScopePhase[] copyScopePhases() {
        if (scopePhases.isEmpty()) {
            return EMPTY_SCOPE_PHASES;
        }
        return scopePhases.toArray(new ScopePhase[scopePhases.size()]);
    }

    public Query parseInnerQuery() throws IOException, QueryParsingException {
        // move to START object
        XContentParser.Token token;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            assert token == XContentParser.Token.START_OBJECT;
        }
        token = parser.nextToken();
        assert token == XContentParser.Token.FIELD_NAME;
        String queryName = parser.currentName();
        // move to the next START_OBJECT
        token = parser.nextToken();
        assert token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY;

        XContentQueryParser queryParser = indexQueryParser.queryParser(queryName);
        if (queryParser == null) {
            throw new QueryParsingException(index, "No query parser registered for [" + queryName + "]");
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
            assert token == XContentParser.Token.START_OBJECT;
        }
        token = parser.nextToken();
        assert token == XContentParser.Token.FIELD_NAME;
        String queryName = parser.currentName();
        // move to the next START_OBJECT or START_ARRAY
        token = parser.nextToken();
        assert token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY;

        XContentFilterParser filterParser = indexQueryParser.filterParser(queryName);
        if (filterParser == null) {
            throw new QueryParsingException(index, "No query parser registered for [" + queryName + "]");
        }
        Filter result = filterParser.parse(this);
        if (parser.currentToken() == XContentParser.Token.END_OBJECT || parser.currentToken() == XContentParser.Token.END_ARRAY) {
            // if we are at END_OBJECT, move to the next one...
            parser.nextToken();
        }
        return result;
    }

    public FieldMapper fieldMapper(String name) {
        FieldMappers fieldMappers = indexQueryParser.mapperService.smartNameFieldMappers(name);
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
        return indexQueryParser.mapperService.smartName(name);
    }
}
