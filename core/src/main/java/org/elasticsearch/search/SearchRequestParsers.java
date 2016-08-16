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

package org.elasticsearch.search;

import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;

/**
 * A container for all parsers used to parse
 * {@link org.elasticsearch.action.search.SearchRequest} objects from a rest request.
 */
public class SearchRequestParsers {
    // TODO: this class should be renamed to SearchRequestParser, and all the parse
    // methods split across RestSearchAction and SearchSourceBuilder should be moved here
    // TODO: make all members private once parsing functions are moved here

    // TODO: IndicesQueriesRegistry should be removed and just have the map of query parsers here
    /**
     * Query parsers that may be used in search requests.
     * @see org.elasticsearch.index.query.QueryParseContext
     * @see org.elasticsearch.search.builder.SearchSourceBuilder#fromXContent(QueryParseContext, AggregatorParsers, Suggesters)
     */
    public final IndicesQueriesRegistry queryParsers;

    // TODO: AggregatorParsers should be removed and the underlying maps of agg
    // and pipeline agg parsers should be here
    /**
     * Agg and pipeline agg parsers that may be used in search requests.
     * @see org.elasticsearch.search.builder.SearchSourceBuilder#fromXContent(QueryParseContext, AggregatorParsers, Suggesters)
     */
    public final AggregatorParsers aggParsers;

    // TODO: Suggesters should be removed and the underlying map moved here
    /**
     * Suggesters that may be used in search requests.
     * @see org.elasticsearch.search.builder.SearchSourceBuilder#fromXContent(QueryParseContext, AggregatorParsers, Suggesters)
     */
    public final Suggesters suggesters;

    public SearchRequestParsers(IndicesQueriesRegistry queryParsers, AggregatorParsers aggParsers, Suggesters suggesters) {
        this.queryParsers = queryParsers;
        this.aggParsers = aggParsers;
        this.suggesters = suggesters;
    }
}
