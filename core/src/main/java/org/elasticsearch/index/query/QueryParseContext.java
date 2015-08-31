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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;

import java.io.IOException;

public class QueryParseContext {

    private static final ParseField CACHE = new ParseField("_cache").withAllDeprecated("Elasticsearch makes its own caching decisions");
    private static final ParseField CACHE_KEY = new ParseField("_cache_key").withAllDeprecated("Filters are always used as cache keys");

    private XContentParser parser;
    private final Index index;
    //norelease this flag is also used in the QueryShardContext, we need to make sure we set it there correctly in doToQuery()
    private ParseFieldMatcher parseFieldMatcher;

    //norelease this can eventually be deleted when context() method goes away
    private final QueryShardContext shardContext;
    private IndicesQueriesRegistry indicesQueriesRegistry;

    public QueryParseContext(Index index, IndicesQueriesRegistry registry) {
        this.index = index;
        this.indicesQueriesRegistry = registry;
        this.shardContext = null;
    }

    QueryParseContext(QueryShardContext context) {
        this.shardContext = context;
        this.index = context.index();
        this.indicesQueriesRegistry = context.indexQueryParserService().indicesQueriesRegistry();
    }

    public void reset(XContentParser jp) {
        this.parseFieldMatcher = ParseFieldMatcher.EMPTY;
        this.parser = jp;
    }

    //norelease this is still used in BaseQueryParserTemp and FunctionScoreQueryParser, remove if not needed there anymore
    @Deprecated
    public QueryShardContext shardContext() {
        return this.shardContext;
    }

    public XContentParser parser() {
        return this.parser;
    }

    public void parseFieldMatcher(ParseFieldMatcher parseFieldMatcher) {
        this.parseFieldMatcher = parseFieldMatcher;
    }

    public boolean isDeprecatedSetting(String setting) {
        return parseFieldMatcher.match(setting, CACHE) || parseFieldMatcher.match(setting, CACHE_KEY);
    }

    public Index index() {
        return this.index;
    }

    /**
     * @deprecated replaced by calls to parseInnerFilterToQueryBuilder() for the resulting queries
     */
    @Nullable
    @Deprecated
    //norelease should be possible to remove after refactoring all queries
    public Query parseInnerFilter() throws QueryShardException, IOException {
        assert this.shardContext != null;
        QueryBuilder builder = parseInnerFilterToQueryBuilder();
        Query result = null;
        if (builder != null) {
            result = builder.toQuery(this.shardContext);
        }
        return result;
    }

    /**
     * @deprecated replaced by calls to parseInnerQueryBuilder() for the resulting queries
     */
    @Nullable
    @Deprecated
    //norelease this method will be removed once all queries are refactored
    public Query parseInnerQuery() throws IOException, QueryShardException {
        QueryBuilder builder = parseInnerQueryBuilder();
        Query result = null;
        if (builder != null) {
            result = builder.toQuery(this.shardContext);
        }
        return result;
    }

    /**
     * @return a new QueryBuilder based on the current state of the parser
     * @throws IOException
     */
    public QueryBuilder parseInnerQueryBuilder() throws IOException {
        // move to START object
        XContentParser.Token token;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new QueryParsingException(this, "[_na] query malformed, must start with start_object");
            }
        }
        token = parser.nextToken();
        if (token == XContentParser.Token.END_OBJECT) {
            // empty query
            return EmptyQueryBuilder.PROTOTYPE;
        }
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(this, "[_na] query malformed, no field after start_object");
        }
        String queryName = parser.currentName();
        // move to the next START_OBJECT
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT && token != XContentParser.Token.START_ARRAY) {
            throw new QueryParsingException(this, "[_na] query malformed, no field after start_object");
        }

        QueryParser queryParser = queryParser(queryName);
        if (queryParser == null) {
            throw new QueryParsingException(this, "No query registered for [" + queryName + "]");
        }
        QueryBuilder result = queryParser.fromXContent(this);
        if (parser.currentToken() == XContentParser.Token.END_OBJECT || parser.currentToken() == XContentParser.Token.END_ARRAY) {
            // if we are at END_OBJECT, move to the next one...
            parser.nextToken();
        }
        return result;
    }

    /**
     * @return a new QueryBuilder based on the current state of the parser, but does so that the inner query
     * is parsed to a filter
     * @throws IOException
     */
    //norelease setting and checking the isFilter Flag should completely be moved to toQuery/toFilter after query refactoring
    public QueryBuilder parseInnerFilterToQueryBuilder() throws IOException {
        final boolean originalIsFilter = this.shardContext.isFilter;
        try {
            this.shardContext.isFilter = true;
            return parseInnerQueryBuilder();
        } finally {
            this.shardContext.isFilter = originalIsFilter;
        }
    }

    //norelease setting and checking the isFilter Flag should completely be moved to toQuery/toFilter after query refactoring
    public QueryBuilder parseInnerFilterToQueryBuilder(String queryName) throws IOException, QueryParsingException {
        final boolean originalIsFilter = this.shardContext.isFilter;
        try {
            this.shardContext.isFilter = true;
            QueryParser queryParser = queryParser(queryName);
            if (queryParser == null) {
                throw new QueryParsingException(this, "No query registered for [" + queryName + "]");
            }
            return queryParser.fromXContent(this);
        } finally {
            this.shardContext.isFilter = originalIsFilter;
        }
    }

    public ParseFieldMatcher parseFieldMatcher() {
        return parseFieldMatcher;
    }

    public void parser(XContentParser innerParser) {
        this.parser = innerParser;
    }

    QueryParser queryParser(String name) {
        return indicesQueriesRegistry.queryParsers().get(name);
    }
}
