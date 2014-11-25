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

import org.elasticsearch.action.deletebyquery.TransportShardDeleteByQueryAction;
import org.elasticsearch.search.internal.SearchContext;

/**
 */
public final class QueryParserUtils {

    private QueryParserUtils() {
    }

    /**
     * Ensures that the query parsing wasn't invoked via the delete by query api.
     */
    public static void ensureNotDeleteByQuery(String name, QueryParseContext parseContext) {
        SearchContext context = SearchContext.current();
        if (context == null) {
            // We can't do the api check, because there is no search context.
            // Because the delete by query shard transport action sets the search context this isn't an issue.
            return;
        }

        if (TransportShardDeleteByQueryAction.DELETE_BY_QUERY_API.equals(context.source())) {
            throw new QueryParsingException(parseContext.index(), "[" + name + "] query and filter unsupported in delete_by_query api");
        }
    }

}
