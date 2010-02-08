/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.fetch;

import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.search.fetch.FetchSearchResult.*;
import static org.elasticsearch.search.query.QuerySearchResult.*;

/**
 * @author kimchy (Shay Banon)
 */
public class QueryFetchSearchResult implements Streamable, QuerySearchResultProvider, FetchSearchResultProvider {

    private QuerySearchResult queryResult;

    private FetchSearchResult fetchResult;

    public QueryFetchSearchResult() {

    }

    public QueryFetchSearchResult(QuerySearchResult queryResult, FetchSearchResult fetchResult) {
        this.queryResult = queryResult;
        this.fetchResult = fetchResult;
    }

    public long id() {
        return queryResult.id();
    }

    public SearchShardTarget shardTarget() {
        return queryResult.shardTarget();
    }

    @Override public boolean includeFetch() {
        return true;
    }

    public QuerySearchResult queryResult() {
        return queryResult;
    }

    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    public static QueryFetchSearchResult readQueryFetchSearchResult(DataInput in) throws IOException, ClassNotFoundException {
        QueryFetchSearchResult result = new QueryFetchSearchResult();
        result.readFrom(in);
        return result;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        queryResult = readQuerySearchResult(in);
        fetchResult = readFetchSearchResult(in);
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        queryResult.writeTo(out);
        fetchResult.writeTo(out);
    }
}
