/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.lucene.Lucene.readTopDocs;
import static org.elasticsearch.common.lucene.Lucene.writeTopDocs;

public final class QuerySearchResult extends SearchPhaseResult {

    public final static class SingleQueryResult {

        private int from;
        private int size;
        private TopDocsAndMaxScore topDocsAndMaxScore;
        private boolean hasScoreDocs;
        private float maxScore = Float.NaN;
        private DocValueFormat[] sortValueFormats;
        private RescoreDocIds rescoreDocIds = RescoreDocIds.EMPTY;

        private long serviceTimeEWMA = -1;
        private int nodeQueueSize = -1;

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

        public SingleQueryResult topDocs(TopDocsAndMaxScore topDocs, DocValueFormat[] sortValueFormats) {
            setTopDocs(topDocs);
            if (topDocs.topDocs.scoreDocs.length > 0 && topDocs.topDocs.scoreDocs[0] instanceof FieldDoc) {
                int numFields = ((FieldDoc) topDocs.topDocs.scoreDocs[0]).fields.length;
                if (numFields != sortValueFormats.length) {
                    throw new IllegalArgumentException(
                        "The number of sort fields does not match: " + numFields + " != " + sortValueFormats.length
                    );
                }
            }
            this.sortValueFormats = sortValueFormats;
            return this;
        }

        private void setTopDocs(TopDocsAndMaxScore topDocsAndMaxScore) {
            this.topDocsAndMaxScore = topDocsAndMaxScore;
            this.maxScore = topDocsAndMaxScore.maxScore;
            this.hasScoreDocs = topDocsAndMaxScore.topDocs.scoreDocs.length > 0;
        }

        public DocValueFormat[] sortValueFormats() {
            return sortValueFormats;
        }

        public int from() {
            return from;
        }

        public SingleQueryResult from(int from) {
            this.from = from;
            return this;
        }

        /**
         * Returns the maximum size of this results top docs.
         */
        public int size() {
            return size;
        }

        public SingleQueryResult size(int size) {
            this.size = size;
            return this;
        }

        public long serviceTimeEWMA() {
            return this.serviceTimeEWMA;
        }

        public SingleQueryResult serviceTimeEWMA(long serviceTimeEWMA) {
            this.serviceTimeEWMA = serviceTimeEWMA;
            return this;
        }

        public int nodeQueueSize() {
            return this.nodeQueueSize;
        }

        public SingleQueryResult nodeQueueSize(int nodeQueueSize) {
            this.nodeQueueSize = nodeQueueSize;
            return this;
        }

        public float getMaxScore() {
            return maxScore;
        }

        public RescoreDocIds getRescoreDocIds() {
            return rescoreDocIds;
        }

        public SingleQueryResult setRescoreDocIds(RescoreDocIds rescoreDocIds) {
            this.rescoreDocIds = rescoreDocIds;
            return this;
        }
    }

    List<SingleQueryResult> singleQueryResults = new ArrayList<>();
    /**
     * Aggregation results. We wrap them in
     * {@linkplain DelayableWriteable} because
     * {@link InternalAggregation} is usually made up of many small objects
     * which have a fairly high overhead in the JVM. So we delay deserializing
     * them until just before we need them.
     */
    private DelayableWriteable<InternalAggregations> aggregations;
    private boolean hasAggs;
    private TotalHits totalHits;
    private Suggest suggest;
    private boolean searchTimedOut;
    private Boolean terminatedEarly = null;

    private SearchProfileQueryPhaseResult profileShardResults;
    private boolean hasProfileResults;

    private final boolean isNull;

    public QuerySearchResult() {
        this(false);
    }

    public QuerySearchResult(StreamInput in) throws IOException {
        this(in, false);
    }

    /**
     * Read the object, but using a delayed aggregations field when delayedAggregations=true. Using this, the caller must ensure that
     * either `consumeAggs` or `releaseAggs` is called if `hasAggs() == true`.
     * @param delayedAggregations whether to use delayed aggregations or not
     */
    public QuerySearchResult(StreamInput in, boolean delayedAggregations) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_7_0)) {
            isNull = in.readBoolean();
        } else {
            isNull = false;
        }
        if (isNull == false) {
            ShardSearchContextId id = new ShardSearchContextId(in);
            readFromWithId(id, in, delayedAggregations);
        }
    }

    public QuerySearchResult(ShardSearchContextId contextId, SearchShardTarget shardTarget, ShardSearchRequest shardSearchRequest) {
        this.contextId = contextId;
        setSearchShardTarget(shardTarget);
        isNull = false;
        setShardSearchRequest(shardSearchRequest);
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

    public List<SingleQueryResult> getSingleQueryResults() {
        return singleQueryResults;
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
    public InternalAggregations consumeAggs() {
        if (aggregations == null) {
            throw new IllegalStateException("aggs already consumed");
        }
        try {
            return aggregations.expand();
        } finally {
            aggregations.close();
            aggregations = null;
        }
    }

    public void releaseAggs() {
        if (aggregations != null) {
            aggregations.close();
            aggregations = null;
        }
    }

    public void aggregations(InternalAggregations aggregations) {
        assert this.aggregations == null : "aggregations already set to [" + this.aggregations + "]";
        this.aggregations = aggregations == null ? null : DelayableWriteable.referencing(aggregations);
        hasAggs = aggregations != null;
    }

    public DelayableWriteable<InternalAggregations> aggregations() {
        return aggregations;
    }

    public void setSearchProfileDfsPhaseResult(SearchProfileDfsPhaseResult searchProfileDfsPhaseResult) {
        if (profileShardResults == null) {
            return;
        }
        profileShardResults.setSearchProfileDfsPhaseResult(searchProfileDfsPhaseResult);
    }

    /**
     * Returns and nulls out the profiled results for this search, or potentially null if result was empty.
     * This allows to free up memory once the profiled result is consumed.
     * @throws IllegalStateException if the profiled result has already been consumed.
     */
    public SearchProfileQueryPhaseResult consumeProfileResult() {
        if (profileShardResults == null) {
            throw new IllegalStateException("profile results already consumed");
        }
        SearchProfileQueryPhaseResult result = profileShardResults;
        profileShardResults = null;
        return result;
    }

    public boolean hasProfileResults() {
        return hasProfileResults;
    }

    public void consumeAll() {
        if (hasProfileResults()) {
            consumeProfileResult();
        }
        for (SingleQueryResult singleQueryResult : singleQueryResults) {
            if (singleQueryResult.hasConsumedTopDocs() == false) {
                singleQueryResult.consumeTopDocs();
            }
        }
        releaseAggs();
    }

    /**
     * Sets the finalized profiling results for this query
     * @param shardResults The finalized profile
     */
    public void profileResults(SearchProfileQueryPhaseResult shardResults) {
        this.profileShardResults = shardResults;
        hasProfileResults = shardResults != null;
    }

    public Suggest suggest() {
        return suggest;
    }

    public void suggest(Suggest suggest) {
        this.suggest = suggest;
    }

    /**
     * Returns <code>true</code> if this result has any suggest score docs
     */
    public boolean hasSuggestHits() {
        return (suggest != null && suggest.hasScoreDocs());
    }

    public boolean hasSearchContext() {
        return singleQueryResults.stream().anyMatch(sqr -> sqr.hasScoreDocs) || hasSuggestHits();
    }

    public void readFromWithId(ShardSearchContextId id, StreamInput in) throws IOException {
        readFromWithId(id, in, false);
    }

    private void readFromWithId(ShardSearchContextId id, StreamInput in, boolean delayedAggregations) throws IOException {
        boolean success = false;
        try {
            this.contextId = id;
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                int singleQueryResultsSize = in.readVInt();
                for (int singleQueryResultIndex = 0; singleQueryResultIndex < singleQueryResultsSize; ++singleQueryResultIndex) {
                    SingleQueryResult singleQueryResult = new SingleQueryResult();
                    singleQueryResult.from = in.readVInt();
                    singleQueryResult.size = in.readVInt();
                    int numSortFieldsPlus1 = in.readVInt();
                    if (numSortFieldsPlus1 == 0) {
                        singleQueryResult.sortValueFormats = null;
                    } else {
                        singleQueryResult.sortValueFormats = new DocValueFormat[numSortFieldsPlus1 - 1];
                        for (int i = 0; i < singleQueryResult.sortValueFormats.length; ++i) {
                            singleQueryResult.sortValueFormats[i] = in.readNamedWriteable(DocValueFormat.class);
                        }
                    }
                    singleQueryResult.setTopDocs(readTopDocs(in));
                    singleQueryResult.serviceTimeEWMA = in.readZLong();
                    singleQueryResult.nodeQueueSize = in.readInt();
                    setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
                    singleQueryResult.setRescoreDocIds(new RescoreDocIds(in));
                    singleQueryResults.add(singleQueryResult);
                }
                hasAggs = in.readBoolean();
                if (hasAggs) {
                    if (delayedAggregations) {
                        aggregations = DelayableWriteable.delayed(InternalAggregations::readFrom, in);
                    } else {
                        aggregations = DelayableWriteable.referencing(InternalAggregations::readFrom, in);
                    }
                }
                if (in.readBoolean()) {
                    suggest = new Suggest(in);
                }
                searchTimedOut = in.readBoolean();
                terminatedEarly = in.readOptionalBoolean();
                profileShardResults = in.readOptionalWriteable(SearchProfileQueryPhaseResult::new);
                hasProfileResults = profileShardResults != null;
            } else {
                SingleQueryResult singleQueryResult = new SingleQueryResult();
                singleQueryResult.from = in.readVInt();
                singleQueryResult.size = in.readVInt();
                int numSortFieldsPlus1 = in.readVInt();
                if (numSortFieldsPlus1 == 0) {
                    singleQueryResult.sortValueFormats = null;
                } else {
                    singleQueryResult.sortValueFormats = new DocValueFormat[numSortFieldsPlus1 - 1];
                    for (int i = 0; i < singleQueryResult.sortValueFormats.length; ++i) {
                        singleQueryResult.sortValueFormats[i] = in.readNamedWriteable(DocValueFormat.class);
                    }
                }
                singleQueryResult.setTopDocs(readTopDocs(in));
                hasAggs = in.readBoolean();
                if (hasAggs) {
                    if (delayedAggregations) {
                        aggregations = DelayableWriteable.delayed(InternalAggregations::readFrom, in);
                    } else {
                        aggregations = DelayableWriteable.referencing(InternalAggregations::readFrom, in);
                    }
                }
                if (in.readBoolean()) {
                    suggest = new Suggest(in);
                }
                searchTimedOut = in.readBoolean();
                terminatedEarly = in.readOptionalBoolean();
                profileShardResults = in.readOptionalWriteable(SearchProfileQueryPhaseResult::new);
                hasProfileResults = profileShardResults != null;
                singleQueryResult.serviceTimeEWMA = in.readZLong();
                singleQueryResult.nodeQueueSize = in.readInt();
                if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
                    setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
                    singleQueryResult.setRescoreDocIds(new RescoreDocIds(in));
                }
            }
            success = true;
        } finally {
            if (success == false) {
                // in case we were not able to deserialize the full message we must release the aggregation buffer
                Releasables.close(aggregations);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // we do not know that it is being sent over transport, but this at least protects all writes from happening, including sending.
        if (aggregations != null && aggregations.isSerialized()) {
            throw new IllegalStateException("cannot send serialized version since it will leak");
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_7_0)) {
            out.writeBoolean(isNull);
        }
        if (isNull == false) {
            contextId.writeTo(out);
            writeToNoId(out);
        }
    }

    public void writeToNoId(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeVInt(singleQueryResults.size());
            for (SingleQueryResult singleQueryResult : singleQueryResults) {
                out.writeVInt(singleQueryResult.from);
                out.writeVInt(singleQueryResult.size);
                if (singleQueryResult.sortValueFormats == null) {
                    out.writeVInt(0);
                } else {
                    out.writeVInt(1 + singleQueryResult.sortValueFormats.length);
                    for (int i = 0; i < singleQueryResult.sortValueFormats.length; ++i) {
                        out.writeNamedWriteable(singleQueryResult.sortValueFormats[i]);
                    }
                }
                writeTopDocs(out, singleQueryResult.topDocsAndMaxScore);
                out.writeZLong(singleQueryResult.serviceTimeEWMA);
                out.writeInt(singleQueryResult.nodeQueueSize);
                out.writeOptionalWriteable(getShardSearchRequest());
                singleQueryResult.getRescoreDocIds().writeTo(out);
            }
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
        } else {
            SingleQueryResult singleQueryResult = singleQueryResults.get(0);
            out.writeVInt(singleQueryResult.from);
            out.writeVInt(singleQueryResult.size);
            if (singleQueryResult.sortValueFormats == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(1 + singleQueryResult.sortValueFormats.length);
                for (int i = 0; i < singleQueryResult.sortValueFormats.length; ++i) {
                    out.writeNamedWriteable(singleQueryResult.sortValueFormats[i]);
                }
            }
            writeTopDocs(out, singleQueryResult.topDocsAndMaxScore);
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
            out.writeZLong(singleQueryResult.serviceTimeEWMA);
            out.writeInt(singleQueryResult.nodeQueueSize);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
                out.writeOptionalWriteable(getShardSearchRequest());
                singleQueryResult.getRescoreDocIds().writeTo(out);
            }
        }
    }

    public QuerySearchResult setTotalHits(TotalHits totalHits) {
        this.totalHits = totalHits;
        return this;
    }

    public TotalHits getTotalHits() {
        return totalHits;
    }
}
