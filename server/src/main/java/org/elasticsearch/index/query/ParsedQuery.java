/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
