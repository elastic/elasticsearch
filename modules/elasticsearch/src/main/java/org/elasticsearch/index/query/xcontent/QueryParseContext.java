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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
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

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class QueryParseContext {

    private final Index index;

    private final ScriptService scriptService;

    private final MapperService mapperService;

    private final SimilarityService similarityService;

    private final IndexCache indexCache;

    private final IndexEngine indexEngine;

    private final XContentQueryParserRegistry queryParserRegistry;

    private XContentParser parser;

    public QueryParseContext(Index index, XContentQueryParserRegistry queryParserRegistry,
                             ScriptService scriptService,
                             MapperService mapperService, SimilarityService similarityService,
                             IndexCache indexCache, IndexEngine indexEngine) {
        this.index = index;
        this.queryParserRegistry = queryParserRegistry;
        this.scriptService = scriptService;
        this.mapperService = mapperService;
        this.similarityService = similarityService;
        this.indexCache = indexCache;
        this.indexEngine = indexEngine;
    }

    public void reset(XContentParser jp) {
        this.parser = jp;
    }

    public XContentParser parser() {
        return parser;
    }

    public ScriptService scriptService() {
        return scriptService;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public IndexEngine indexEngine() {
        return this.indexEngine;
    }

    @Nullable public SimilarityService similarityService() {
        return this.similarityService;
    }

    public Similarity searchSimilarity() {
        return similarityService != null ? similarityService.defaultSearchSimilarity() : null;
    }

    public IndexCache indexCache() {
        return indexCache;
    }

    public Filter cacheFilterIfPossible(Filter filter) {
        return indexCache.filter().cache(filter);
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
        assert token == XContentParser.Token.START_OBJECT;

        XContentQueryParser queryParser = queryParserRegistry.queryParser(queryName);
        if (queryParser == null) {
            throw new QueryParsingException(index, "No query parser registered for [" + queryName + "]");
        }
        Query result = queryParser.parse(this);
        if (parser.currentToken() == XContentParser.Token.END_OBJECT) {
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
        // move to the next START_OBJECT
        token = parser.nextToken();
        assert token == XContentParser.Token.START_OBJECT;

        XContentFilterParser filterParser = queryParserRegistry.filterParser(queryName);
        if (filterParser == null) {
            throw new QueryParsingException(index, "No query parser registered for [" + queryName + "]");
        }
        Filter result = filterParser.parse(this);
        if (parser.currentToken() == XContentParser.Token.END_OBJECT) {
            // if we are at END_OBJECT, move to the next one...
            parser.nextToken();
        }
        return result;
    }

    public FieldMapper fieldMapper(String name) {
        FieldMappers fieldMappers = mapperService.smartNameFieldMappers(name);
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
        return mapperService.smartName(name);
    }
}
