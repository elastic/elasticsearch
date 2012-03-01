/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.query;

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.InternalFacets;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.lucene.Lucene.readTopDocs;
import static org.elasticsearch.common.lucene.Lucene.writeTopDocs;

/**
 *
 */
public class QuerySearchResult implements Streamable, QuerySearchResultProvider {

    private long id;

    private SearchShardTarget shardTarget;

    private int from;

    private int size;

    private TopDocs topDocs;

    private TopGroups<String> topGroups;

    private final Set<Integer> documentGrouped = new HashSet<Integer>();

    private InternalFacets facets;

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

    public Set<Integer> documentGrouped() {
        return documentGrouped;
    }

    public TopGroups<String> topGroups() {
        return topGroups;
    }

    public void topGroups(TopGroups<String> topGroups) {
        this.topGroups = topGroups;
    }

    public Facets facets() {
        return facets;
    }

    public void facets(InternalFacets facets) {
        this.facets = facets;
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
        id = in.readLong();
//        shardTarget = readSearchShardTarget(in);
        from = in.readVInt();
        size = in.readVInt();
        topDocs = readTopDocs(in);
        if (in.readBoolean()) {
            facets = InternalFacets.readFacets(in);
        }
        searchTimedOut = in.readBoolean();
        int documentsGroupedSize = in.readVInt();
        for (int i = 0; i < documentsGroupedSize; i++) {
            documentGrouped.add(in.readVInt());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        out.writeBoolean(searchTimedOut);
        out.writeVInt(documentGrouped.size());
        for (Integer docId : documentGrouped) {
            out.writeVInt(docId);
        }
    }
}
