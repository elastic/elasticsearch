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

package org.elasticsearch.search.query;

import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.transport.TransportResponse;

public abstract class QuerySearchResultProvider extends TransportResponse implements SearchPhaseResult {

    /**
     * Returns the query result iff it's included in this response otherwise <code>null</code>
     */
    public QuerySearchResult queryResult() {
        return  null;
    }

    /**
     * Returns the fetch result iff it's included in this response otherwise <code>null</code>
     */
    public FetchSearchResult fetchResult() {
        return null;
    }
}
