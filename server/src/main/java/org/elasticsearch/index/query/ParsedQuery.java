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

/**
 * The result of parsing a query.
 */
public class ParsedQuery {
    private final Query query;
    private final boolean matchNamedQueries;

    /**
     * Store the query and filters.
     *  @param query
     *            the query
     * @param matchNamedQueries
     *            {@code true} if the search should generate Matches
     */
    public ParsedQuery(Query query, boolean matchNamedQueries) {
        this.query = query;
        this.matchNamedQueries = matchNamedQueries;
    }

    public ParsedQuery(Query query) {
        this(query, false);
    }

    /**
     * The query parsed.
     */
    public Query query() {
        return this.query;
    }

    public boolean matchNamedQueries() {
        return this.matchNamedQueries;
    }

    public static ParsedQuery parsedMatchAllQuery() {
        return new ParsedQuery(Queries.newMatchAllQuery(), false);
    }
}
