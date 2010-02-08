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

package org.elasticsearch.index.query.json;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonQueryParseContext {

    private final Index index;

    private final MapperService mapperService;

    private final FilterCache filterCache;

    private final JsonQueryParserRegistry queryParserRegistry;

    private JsonParser jp;

    public JsonQueryParseContext(Index index, JsonQueryParserRegistry queryParserRegistry,
                                 MapperService mapperService, FilterCache filterCache) {
        this.index = index;
        this.queryParserRegistry = queryParserRegistry;
        this.mapperService = mapperService;
        this.filterCache = filterCache;
    }

    public void reset(JsonParser jp) {
        this.jp = jp;
    }

    public JsonParser jp() {
        return jp;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public FilterCache filterCache() {
        return filterCache;
    }

    public Filter cacheFilterIfPossible(Filter filter) {
        if (filterCache == null) {
            return filter;
        }
        return filterCache.cache(filter);
    }

    public Query parseInnerQuery() throws IOException, QueryParsingException {
        // move to START object
        JsonToken token;
        if (jp.getCurrentToken() != JsonToken.START_OBJECT) {
            token = jp.nextToken();
            assert token == JsonToken.START_OBJECT;
        }
        token = jp.nextToken();
        assert token == JsonToken.FIELD_NAME;
        String queryName = jp.getCurrentName();
        // move to the next START_OBJECT
        token = jp.nextToken();
        assert token == JsonToken.START_OBJECT;

        JsonQueryParser queryParser = queryParserRegistry.queryParser(queryName);
        if (queryParser == null) {
            throw new QueryParsingException(index, "No json query parser registered for [" + queryName + "]");
        }
        Query result = queryParser.parse(this);
        if (jp.getCurrentToken() == JsonToken.END_OBJECT) {
            // if we are at END_OBJECT, move to the next one...
            jp.nextToken();
        }
        return result;
    }

    public Filter parseInnerFilter() throws IOException, QueryParsingException {
        // move to START object
        JsonToken token;
        if (jp.getCurrentToken() != JsonToken.START_OBJECT) {
            token = jp.nextToken();
            assert token == JsonToken.START_OBJECT;
        }
        token = jp.nextToken();
        assert token == JsonToken.FIELD_NAME;
        String queryName = jp.getCurrentName();
        // move to the next START_OBJECT
        token = jp.nextToken();
        assert token == JsonToken.START_OBJECT;

        JsonFilterParser filterParser = queryParserRegistry.filterParser(queryName);
        if (filterParser == null) {
            throw new QueryParsingException(index, "No json query parser registered for [" + queryName + "]");
        }
        Filter result = filterParser.parse(this);
        if (jp.getCurrentToken() == JsonToken.END_OBJECT) {
            // if we are at END_OBJECT, move to the next one...
            jp.nextToken();
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
        MapperService.SmartNameFieldMappers smartMapper = smartFieldMappers(name);
        if (smartMapper == null) {
            return name;
        }
        if (smartMapper.fieldMappers().mapper() != null) {
            return smartMapper.fieldMappers().mapper().indexName();
        }
        return name;
    }

    public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
        return mapperService.smartName(name);
    }
}
