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

import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

import static org.elasticsearch.common.lucene.Lucene.readTopDocs;
import static org.elasticsearch.common.lucene.Lucene.writeTopDocs;

/**
 *
 */
public class QuerySearchResult extends TransportResponse implements QuerySearchResultProvider {

    private long id;
    private SearchShardTarget shardTarget;
    private int from;
    private int size;
    private TopDocs topDocs;
    private InternalFacets facets;
    private InternalAggregations aggregations;
    private Suggest suggest;
    private boolean searchTimedOut;

    public QuerySearchResult() {

    }

    public QuerySearchResult(long id, SearchShardTarget shardTarget) {
        this.id = id;
        this.shardTarget = shardTarget;
    }

    @Override
    public boolean includeFetch() {
        return false;
    }

    @Override
    public QuerySearchResult queryResult() {
        return this;
    }

    public long id() {
        return this.id;
    }

    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void shardTarget(SearchShardTarget shardTarget) {
        this.shardTarget = shardTarget;
    }

    public void searchTimedOut(boolean searchTimedOut) {
        this.searchTimedOut = searchTimedOut;
    }

    public boolean searchTimedOut() {
        return searchTimedOut;
    }

    public TopDocs topDocs() {
        return topDocs;
    }

    public void topDocs(TopDocs topDocs) {
        this.topDocs = topDocs;
    }

    public Facets facets() {
        return facets;
    }

    public void facets(InternalFacets facets) {
        this.facets = facets;
    }

    public Aggregations aggregations() {
        return aggregations;
    }

    public void aggregations(InternalAggregations aggregations) {
        this.aggregations = aggregations;
    }

    public Suggest suggest() {
        return suggest;
    }

    public void suggest(Suggest suggest) {
        this.suggest = suggest;
    }

    public int from() {
        return from;
    }

    public QuerySearchResult from(int from) {
        this.from = from;
        return this;
    }

    public int size() {
        return size;
    }

    public QuerySearchResult size(int size) {
        this.size = size;
        return this;
    }

    public static QuerySearchResult readQuerySearchResult(StreamInput in) throws IOException {
        QuerySearchResult result = new QuerySearchResult();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readLong();
//        shardTarget = readSearchShardTarget(in);
        from = in.readVInt();
        size = in.readVInt();
        topDocs = readTopDocs(in);
        if (in.readBoolean()) {
            facets = InternalFacets.readFacets(in);
        }
        if (in.readBoolean()) {
            aggregations = InternalAggregations.readAggregations(in);
        }
        if (in.readBoolean()) {
            suggest = Suggest.readSuggest(Suggest.Fields.SUGGEST, in);
        }
        searchTimedOut = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
//        shardTarget.writeTo(out);
        out.writeVInt(from);
        out.writeVInt(size);
        writeTopDocs(out, topDocs, 0);
        if (facets == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            facets.writeTo(out);
        }
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(searchTimedOut);
    }
}
