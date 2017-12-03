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

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.query.QuerySearchResult;

import java.io.IOException;

public final class FetchSearchResult extends SearchPhaseResult {

    private SearchHits hits;
    // client side counter
    private transient int counter;

    public FetchSearchResult() {
    }

    public FetchSearchResult(long id, SearchShardTarget shardTarget) {
        this.requestId = id;
        setSearchShardTarget(shardTarget);
    }

    @Override
    public QuerySearchResult queryResult() {
        return null;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return this;
    }

    public void hits(SearchHits hits) {
        assert assertNoSearchTarget(hits);
        this.hits = hits;
    }

    private boolean assertNoSearchTarget(SearchHits hits) {
        for (SearchHit hit : hits.getHits()) {
            assert hit.getShard() == null : "expected null but got: " + hit.getShard();
        }
        return true;
    }

    public SearchHits hits() {
        return hits;
    }

    public FetchSearchResult initCounter() {
        counter = 0;
        return this;
    }

    public int counterGetAndIncrement() {
        return counter++;
    }

    public static FetchSearchResult readFetchSearchResult(StreamInput in) throws IOException {
        FetchSearchResult result = new FetchSearchResult();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        requestId = in.readLong();
        hits = SearchHits.readSearchHits(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(requestId);
        hits.writeTo(out);
    }
}
