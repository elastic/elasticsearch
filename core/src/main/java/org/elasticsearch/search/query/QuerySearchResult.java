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

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.lucene.Lucene.readTopDocs;
import static org.elasticsearch.common.lucene.Lucene.writeTopDocs;

/**
 *
 */
public class QuerySearchResult extends QuerySearchResultProvider {

    private long id;
    private SearchShardTarget shardTarget;
    private int from;
    private int size;
    private TopDocs topDocs;
    private DocValueFormat[] sortValueFormats;
    private InternalAggregations aggregations;
    private List<SiblingPipelineAggregator> pipelineAggregators;
    private Suggest suggest;
    private boolean searchTimedOut;
    private Boolean terminatedEarly = null;
    private ProfileShardResult profileShardResults;

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

    @Override
    public long id() {
        return this.id;
    }

    @Override
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

    public void terminatedEarly(boolean terminatedEarly) {
        this.terminatedEarly = terminatedEarly;
    }

    public Boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    public TopDocs topDocs() {
        return topDocs;
    }

    public void topDocs(TopDocs topDocs, DocValueFormat[] sortValueFormats) {
        this.topDocs = topDocs;
        if (topDocs.scoreDocs.length > 0 && topDocs.scoreDocs[0] instanceof FieldDoc) {
            int numFields = ((FieldDoc) topDocs.scoreDocs[0]).fields.length;
            if (numFields != sortValueFormats.length) {
                throw new IllegalArgumentException("The number of sort fields does not match: "
                        + numFields + " != " + sortValueFormats.length);
            }
        }
        this.sortValueFormats = sortValueFormats;
    }

    public DocValueFormat[] sortValueFormats() {
        return sortValueFormats;
    }

    public Aggregations aggregations() {
        return aggregations;
    }

    public void aggregations(InternalAggregations aggregations) {
        this.aggregations = aggregations;
    }

    /**
     * Returns the profiled results for this search, or potentially null if result was empty
     * @return The profiled results, or null
     */
    public @Nullable ProfileShardResult profileResults() {
        return profileShardResults;
    }

    /**
     * Sets the finalized profiling results for this query
     * @param shardResults The finalized profile
     */
    public void profileResults(ProfileShardResult shardResults) {
        this.profileShardResults = shardResults;
    }

    public List<SiblingPipelineAggregator> pipelineAggregators() {
        return pipelineAggregators;
    }

    public void pipelineAggregators(List<SiblingPipelineAggregator> pipelineAggregators) {
        this.pipelineAggregators = pipelineAggregators;
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
        long id = in.readLong();
        readFromWithId(id, in);
    }

    public void readFromWithId(long id, StreamInput in) throws IOException {
        this.id = id;
//        shardTarget = readSearchShardTarget(in);
        from = in.readVInt();
        size = in.readVInt();
        int numSortFieldsPlus1 = in.readVInt();
        if (numSortFieldsPlus1 == 0) {
            sortValueFormats = null;
        } else {
            sortValueFormats = new DocValueFormat[numSortFieldsPlus1 - 1];
            for (int i = 0; i < sortValueFormats.length; ++i) {
                sortValueFormats[i] = in.readNamedWriteable(DocValueFormat.class);
            }
        }
        topDocs = readTopDocs(in);
        if (in.readBoolean()) {
            aggregations = InternalAggregations.readAggregations(in);
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            List<SiblingPipelineAggregator> pipelineAggregators = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                BytesReference type = in.readBytesReference();
                PipelineAggregator pipelineAggregator = PipelineAggregatorStreams.stream(type).readResult(in);
                pipelineAggregators.add((SiblingPipelineAggregator) pipelineAggregator);
            }
            this.pipelineAggregators = pipelineAggregators;
        }
        if (in.readBoolean()) {
            suggest = Suggest.readSuggest(in);
        }
        searchTimedOut = in.readBoolean();
        terminatedEarly = in.readOptionalBoolean();

        if (in.getVersion().onOrAfter(Version.V_2_2_0) && in.readBoolean()) {
            profileShardResults = new ProfileShardResult(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
        writeToNoId(out);
    }

    public void writeToNoId(StreamOutput out) throws IOException {
//        shardTarget.writeTo(out);
        out.writeVInt(from);
        out.writeVInt(size);
        if (sortValueFormats == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(1 + sortValueFormats.length);
            for (int i = 0; i < sortValueFormats.length; ++i) {
                out.writeNamedWriteable(sortValueFormats[i]);
            }
        }
        writeTopDocs(out, topDocs);
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
        if (pipelineAggregators == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(pipelineAggregators.size());
            for (PipelineAggregator pipelineAggregator : pipelineAggregators) {
                out.writeBytesReference(pipelineAggregator.type().stream());
                pipelineAggregator.writeTo(out);
            }
        }
        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(searchTimedOut);
        out.writeOptionalBoolean(terminatedEarly);

        if (out.getVersion().onOrAfter(Version.V_2_2_0)) {
            if (profileShardResults == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                profileShardResults.writeTo(out);
            }
        }
    }
}
