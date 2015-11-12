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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;

import java.io.IOException;

public class QueryParseContext {

    private static final ParseField CACHE = new ParseField("_cache").withAllDeprecated("Elasticsearch makes its own caching decisions");
    private static final ParseField CACHE_KEY = new ParseField("_cache_key").withAllDeprecated("Filters are always used as cache keys");

    private XContentParser parser;
    private ParseFieldMatcher parseFieldMatcher = ParseFieldMatcher.EMPTY;

    private IndicesQueriesRegistry indicesQueriesRegistry;

    public QueryParseContext(IndicesQueriesRegistry registry) {
        this.indicesQueriesRegistry = registry;
    }

    public void reset(XContentParser jp) {
        this.parseFieldMatcher = ParseFieldMatcher.EMPTY;
        this.parser = jp;
        if (parser != null) {
            this.parser.setParseFieldMatcher(parseFieldMatcher);
        }
    }

    public XContentParser parser() {
        return this.parser;
    }

    public void parseFieldMatcher(ParseFieldMatcher parseFieldMatcher) {
        if (parseFieldMatcher == null) {
            throw new IllegalArgumentException("parseFieldMatcher must not be null");
        }
        this.parseFieldMatcher = parseFieldMatcher;
    }

    public boolean isDeprecatedSetting(String setting) {
        return parseFieldMatcher.match(setting, CACHE) || parseFieldMatcher.match(setting, CACHE_KEY);
    }

    /**
     * Parses a top level query including the query element that wraps it
     */
    public QueryBuilder<?> parseTopLevelQueryBuilder() {
        try {
            QueryBuilder<?> queryBuilder = null;
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    if ("query".equals(fieldName)) {
                        queryBuilder = parseInnerQueryBuilder();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "request does not support [" + parser.currentName() + "]");
                    }
                }
            }
            if (queryBuilder == null) {
                throw new ParsingException(parser.getTokenLocation(), "Required query is missing");
            }
            return queryBuilder;
        } catch (ParsingException e) {
            throw e;
        } catch (Throwable e) {
            throw new ParsingException(parser == null ? null : parser.getTokenLocation(), "Failed to parse", e);
        }
    }

    /**
     * Parses a query excluding the query element that wraps it
     */
    public QueryBuilder<?> parseInnerQueryBuilder() throws IOException {
        // move to START object
        XContentParser.Token token;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, must start with start_object");
            }
        }
        token = parser.nextToken();
        if (token == XContentParser.Token.END_OBJECT) {
            // empty query
            return EmptyQueryBuilder.PROTOTYPE;
        }
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, no field after start_object");
        }
        String queryName = parser.currentName();
        // move to the next START_OBJECT
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT && token != XContentParser.Token.START_ARRAY) {
            throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, no field after start_object");
        }

        QueryParser queryParser = queryParser(queryName);
        if (queryParser == null) {
            throw new ParsingException(parser.getTokenLocation(), "No query registered for [" + queryName + "]");
        }

        QueryBuilder result = queryParser.fromXContent(this);
        if (parser.currentToken() == XContentParser.Token.END_OBJECT || parser.currentToken() == XContentParser.Token.END_ARRAY) {
            // if we are at END_OBJECT, move to the next one...
            parser.nextToken();
        }
        return result;
    }

    public ParseFieldMatcher parseFieldMatcher() {
        return parseFieldMatcher;
    }

    public void parser(XContentParser innerParser) {
        this.parser = innerParser;
    }

    /**
     * Get the query parser for a specific type of query registered under its name
     * @param name the name of the parser to retrieve
     * @return the query parser
     */
    private QueryParser queryParser(String name) {
        return indicesQueriesRegistry.queryParsers().get(name);
    }
}
