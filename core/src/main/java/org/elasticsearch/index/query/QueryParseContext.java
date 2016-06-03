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
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class QueryParseContext implements ParseFieldMatcherSupplier {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(QueryParseContext.class));

    private static final ParseField CACHE = new ParseField("_cache").withAllDeprecated("Elasticsearch makes its own caching decisions");
    private static final ParseField CACHE_KEY = new ParseField("_cache_key").withAllDeprecated("Filters are always used as cache keys");

    private final XContentParser parser;
    private final IndicesQueriesRegistry indicesQueriesRegistry;
    private final ParseFieldMatcher parseFieldMatcher;

    public QueryParseContext(IndicesQueriesRegistry registry, XContentParser parser, ParseFieldMatcher parseFieldMatcher) {
        this.indicesQueriesRegistry = Objects.requireNonNull(registry, "indices queries registry cannot be null");
        this.parser = Objects.requireNonNull(parser, "parser cannot be null");
        this.parseFieldMatcher = Objects.requireNonNull(parseFieldMatcher, "parse field matcher cannot be null");
    }

    public XContentParser parser() {
        return this.parser;
    }

    public boolean isDeprecatedSetting(String setting) {
        return this.parseFieldMatcher.match(setting, CACHE) || this.parseFieldMatcher.match(setting, CACHE_KEY);
    }

    /**
     * Parses a top level query including the query element that wraps it
     */
    public QueryBuilder parseTopLevelQueryBuilder() {
        try {
            QueryBuilder queryBuilder = null;
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    if ("query".equals(fieldName)) {
                        queryBuilder = parseInnerQueryBuilder().orElse(null);
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
    public Optional<QueryBuilder> parseInnerQueryBuilder() throws IOException {
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
            // we encountered '{}' for a query clause
            String msg = "query malformed, empty clause found at [" + parser.getTokenLocation() +"]";
            DEPRECATION_LOGGER.deprecated(msg);
            if (parseFieldMatcher.isStrict()) {
                throw new IllegalArgumentException(msg);
            }
            return Optional.empty();
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
        @SuppressWarnings("unchecked")
        Optional<QueryBuilder> result = (Optional<QueryBuilder>) indicesQueriesRegistry.lookup(queryName, parseFieldMatcher,
                parser.getTokenLocation()).fromXContent(this);
        if (parser.currentToken() == XContentParser.Token.END_OBJECT || parser.currentToken() == XContentParser.Token.END_ARRAY) {
            // if we are at END_OBJECT, move to the next one...
            parser.nextToken();
        }
        return result;
    }

    @Override
    public ParseFieldMatcher getParseFieldMatcher() {
        return parseFieldMatcher;
    }
}
