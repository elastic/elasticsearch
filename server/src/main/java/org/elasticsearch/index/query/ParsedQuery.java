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
import org.elasticsearch.common.lucene.search.Queries;

import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * The result of parsing a query.
 */
public class ParsedQuery {
    private final Query query;
    private final Map<String, Query> namedFilters;

    /**
     * Store the query and filters.
     *
     * @param query
     *            the query
     * @param namedFilters
     *            an immutable Map containing the named filters. Good callers
     *            use emptyMap or unmodifiableMap and copy the source to make
     *            sure this is immutable.
     */
    public ParsedQuery(Query query, Map<String, Query> namedFilters) {
        this.query = query;
        this.namedFilters = namedFilters;
    }

    public ParsedQuery(Query query, ParsedQuery parsedQuery) {
        this.query = query;
        this.namedFilters = parsedQuery.namedFilters;
    }

    public ParsedQuery(Query query) {
        this.query = query;
        this.namedFilters = emptyMap();
    }

    /**
     * The query parsed.
     */
    public Query query() {
        return this.query;
    }

    public Map<String, Query> namedFilters() {
        return namedFilters;
    }

    public static ParsedQuery parsedMatchAllQuery() {
        return new ParsedQuery(Queries.newMatchAllQuery(), emptyMap());
    }
}
