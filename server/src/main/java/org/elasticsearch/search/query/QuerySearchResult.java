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
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.SearchContextId;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;

import static org.elasticsearch.common.lucene.Lucene.readTopDocs;
import static org.elasticsearch.common.lucene.Lucene.writeTopDocs;

public final class QuerySearchResult extends SearchPhaseResult {

    private int from;
    private int size;
    private TopDocsAndMaxScore topDocsAndMaxScore;
    private boolean hasScoreDocs;
    private TotalHits totalHits;
    private float maxScore = Float.NaN;
    private DocValueFormat[] sortValueFormats;
    /**
     * Aggregation results. We wrap them in
     * {@linkplain DelayableWriteable} because
     * {@link InternalAggregation} is usually made up of many small objects
     * which have a fairly high overhead in the JVM. So we delay deserializing
     * them until just before we need them.
     */
    private DelayableWriteable<InternalAggregations> aggregations;
    private boolean hasAggs;
    private Suggest suggest;
    private boolean searchTimedOut;
    private Boolean terminatedEarly = null;
    private ProfileShardResult profileShardResults;
    private boolean hasProfileResults;
    private long serviceTimeEWMA = -1;
    private int nodeQueueSize = -1;

    private final boolean isNull;

    public QuerySearchResult() {
        this(false);
    }

    public QuerySearchResult(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            isNull = in.readBoolean();
        } else {
            isNull = false;
        }
        if (isNull == false) {
            SearchContextId id = new SearchContextId(in);
            readFromWithId(id, in);
        }
    }

    public QuerySearchResult(SearchContextId id, SearchShardTarget shardTarget) {
        this.contextId = id;
        setSearchShardTarget(shardTarget);
        isNull = false;
    }

    private QuerySearchResult(boolean isNull) {
        this.isNull = isNull;
    }

    /**
     * Returns an instance that contains no response.
     */
    public static QuerySearchResult nullInstance() {
        return new QuerySearchResult(true);
    }

    /**
     * Returns true if the result doesn't contain any useful information.
     * It is used by the search action to avoid creating an empty response on
     * shard request that rewrites to match_no_docs.
     *
     * TODO: Currently we need the concrete aggregators to build empty responses. This means that we cannot
     *       build an empty response in the coordinating node so we rely on this hack to ensure that at least one shard
     *       returns a valid empty response. We should move the ability to create empty responses to aggregation builders
     *       in order to allow building empty responses directly from the coordinating node.
     */
    public boolean isNull() {
        return isNull;
    }

    @Override
    public QuerySearchResult queryResult() {
        return this;
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

    public TopDocsAndMaxScore topDocs() {
        if (topDocsAndMaxScore == null) {
            throw new IllegalStateException("topDocs already consumed");
        }
        return topDocsAndMaxScore;
    }

    /**
     * Returns <code>true</code> iff the top docs have already been consumed.
     */
    public boolean hasConsumedTopDocs() {
        return topDocsAndMaxScore == null;
    }

    /**
     * Returns and nulls out the top docs for this search results. This allows to free up memory once the top docs are consumed.
     * @throws IllegalStateException if the top docs have already been consumed.
     */
    public TopDocsAndMaxScore consumeTopDocs() {
        TopDocsAndMaxScore topDocsAndMaxScore = this.topDocsAndMaxScore;
        if (topDocsAndMaxScore == null) {
            throw new IllegalStateException("topDocs already consumed");
        }
        this.topDocsAndMaxScore = null;
        return topDocsAndMaxScore;
    }

    public void topDocs(TopDocsAndMaxScore topDocs, DocValueFormat[] sortValueFormats) {
        setTopDocs(topDocs);
        if (topDocs.topDocs.scoreDocs.length > 0 && topDocs.topDocs.scoreDocs[0] instanceof FieldDoc) {
            int numFields = ((FieldDoc) topDocs.topDocs.scoreDocs[0]).fields.length;
            if (numFields != sortValueFormats.length) {
                throw new IllegalArgumentException("The number of sort fields does not match: "
                        + numFields + " != " + sortValueFormats.length);
            }
        }
        this.sortValueFormats = sortValueFormats;
    }

    private void setTopDocs(TopDocsAndMaxScore topDocsAndMaxScore) {
        this.topDocsAndMaxScore = topDocsAndMaxScore;
        this.totalHits = topDocsAndMaxScore.topDocs.totalHits;
        this.maxScore = topDocsAndMaxScore.maxScore;
        this.hasScoreDocs = topDocsAndMaxScore.topDocs.scoreDocs.length > 0;
    }

    public DocValueFormat[] sortValueFormats() {
        return sortValueFormats;
    }

    /**
     * Returns <code>true</code> if this query result has unconsumed aggregations
     */
    public boolean hasAggs() {
        return hasAggs;
    }

    /**
     * Returns and nulls out the aggregation for this search results. This allows to free up memory once the aggregation is consumed.
     * @throws IllegalStateException if the aggregations have already been consumed.
     */
    public DelayableWriteable<InternalAggregations> consumeAggs() {
        if (aggregations == null) {
            throw new IllegalStateException("aggs already consumed");
        }
        DelayableWriteable<InternalAggregations> aggs = aggregations;
        aggregations = null;
        return aggs;
    }

    public void aggregations(InternalAggregations aggregations) {
        this.aggregations = aggregations == null ? null : DelayableWriteable.referencing(aggregations);
        hasAggs = aggregations != null;
    }

    public DelayableWriteable<InternalAggregations> aggregations() {
        return aggregations;
    }

    /**
     * Returns and nulls out the profiled results for this search, or potentially null if result was empty.
     * This allows to free up memory once the profiled result is consumed.
     * @throws IllegalStateException if the profiled result has already been consumed.
     */
    public ProfileShardResult consumeProfileResult() {
        if (profileShardResults == null) {
            throw new IllegalStateException("profile results already consumed");
        }
        ProfileShardResult result = profileShardResults;
        profileShardResults = null;
        return result;
    }

    public boolean hasProfileResults() {
        return hasProfileResults;
    }

    /**
     * Sets the finalized profiling results for this query
     * @param shardResults The finalized profile
     */
    public void profileResults(ProfileShardResult shardResults) {
        this.profileShardResults = shardResults;
        hasProfileResults = shardResults != null;
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

    /**
     * Returns the maximum size of this results top docs.
     */
    public int size() {
        return size;
    }

    public QuerySearchResult size(int size) {
        this.size = size;
        return this;
    }

    public long serviceTimeEWMA() {
        return this.serviceTimeEWMA;
    }

    public QuerySearchResult serviceTimeEWMA(long serviceTimeEWMA) {
        this.serviceTimeEWMA = serviceTimeEWMA;
        return this;
    }

    public int nodeQueueSize() {
        return this.nodeQueueSize;
    }

    public QuerySearchResult nodeQueueSize(int nodeQueueSize) {
        this.nodeQueueSize = nodeQueueSize;
        return this;
    }

    /**
     * Returns <code>true</code> if this result has any suggest score docs
     */
    public boolean hasSuggestHits() {
      return (suggest != null && suggest.hasScoreDocs());
    }

    public boolean hasSearchContext() {
        return hasScoreDocs || hasSuggestHits();
    }

    public void readFromWithId(SearchContextId id, StreamInput in) throws IOException {
        this.contextId = id;
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
        setTopDocs(readTopDocs(in));
        if (hasAggs = in.readBoolean()) {
            aggregations = DelayableWriteable.delayed(InternalAggregations::new, in);
        }
        if (in.readBoolean()) {
            suggest = new Suggest(in);
        }
        searchTimedOut = in.readBoolean();
        terminatedEarly = in.readOptionalBoolean();
        profileShardResults = in.readOptionalWriteable(ProfileShardResult::new);
        hasProfileResults = profileShardResults != null;
        serviceTimeEWMA = in.readZLong();
        nodeQueueSize = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeBoolean(isNull);
        }
        if (isNull == false) {
            contextId.writeTo(out);
            writeToNoId(out);
        }
    }

    public void writeToNoId(StreamOutput out) throws IOException {
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
        writeTopDocs(out, topDocsAndMaxScore);
        out.writeOptionalWriteable(aggregations);
        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(searchTimedOut);
        out.writeOptionalBoolean(terminatedEarly);
        out.writeOptionalWriteable(profileShardResults);
        out.writeZLong(serviceTimeEWMA);
        out.writeInt(nodeQueueSize);
    }

    public TotalHits getTotalHits() {
        return totalHits;
    }

    public float getMaxScore() {
        return maxScore;
    }
}
