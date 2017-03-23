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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry.UnknownNamedObjectException;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class QueryParseContext {

    private static final ParseField CACHE = new ParseField("_cache").withAllDeprecated("Elasticsearch makes its own caching decisions");
    private static final ParseField CACHE_KEY = new ParseField("_cache_key").withAllDeprecated("Filters are always used as cache keys");

    private final XContentParser parser;

    public QueryParseContext(XContentParser parser) {
        this.parser = Objects.requireNonNull(parser, "parser cannot be null");
    }

    public XContentParser parser() {
        return this.parser;
    }

    public boolean isDeprecatedSetting(String setting) {
        return CACHE.match(setting) || CACHE_KEY.match(setting);
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
                        queryBuilder = parseInnerQueryBuilder();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "request does not support [" + parser.currentName() + "]");
                    }
                }
            }
            return queryBuilder;
        } catch (ParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new ParsingException(parser == null ? null : parser.getTokenLocation(), "Failed to parse", e);
        }
    }

    /**
     * Parses a query excluding the query element that wraps it
     */
    public QueryBuilder parseInnerQueryBuilder() throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, must start with start_object");
            }
        }
        if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
            // we encountered '{}' for a query clause, it used to be supported, deprecated in 5.0 and removed in 6.0
            throw new IllegalArgumentException("query malformed, empty clause found at [" + parser.getTokenLocation() +"]");
        }
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, no field after start_object");
        }
        String queryName = parser.currentName();
        // move to the next START_OBJECT
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + queryName + "] query malformed, no start_object after query name");
        }
        QueryBuilder result;
        try {
            result = parser.namedObject(QueryBuilder.class, queryName, this);
        } catch (UnknownNamedObjectException e) {
            // Preserve the error message from 5.0 until we have a compellingly better message so we don't break BWC.
            // This intentionally doesn't include the causing exception because that'd change the "root_cause" of any unknown query errors
            throw new ParsingException(new XContentLocation(e.getLineNumber(), e.getColumnNumber()),
                    "no [query] registered for [" + e.getName() + "]");
        }
        //end_object of the specific query (e.g. match, multi_match etc.) element
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                    "[" + queryName + "] malformed query, expected [END_OBJECT] but found [" + parser.currentToken() + "]");
        }
        //end_object of the query object
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                    "[" + queryName + "] malformed query, expected [END_OBJECT] but found [" + parser.currentToken() + "]");
        }
        return result;
    }
}
