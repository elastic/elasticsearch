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

    public static class SingleSearchResult {

        private int from;
        private int size;
        private TopDocsAndMaxScore topDocsAndMaxScore;
        private boolean hasScoreDocs;
        private TotalHits totalHits;
        private float maxScore = Float.NaN;
        private DocValueFormat[] sortValueFormats;
        private RescoreDocIds rescoreDocIds = RescoreDocIds.EMPTY;

        private Boolean terminatedEarly = null;
        private long serviceTimeEWMA = -1;
        private int nodeQueueSize = -1;

        public int from() {
            return from;
        }

        public void from(int from) {
            this.from = from;
        }

        /**
         * Returns the maximum size of this results top docs.
         */
        public int size() {
            return size;
        }

        public void size(int size) {
            this.size = size;
        }

        public long serviceTimeEWMA() {
            return this.serviceTimeEWMA;
        }

        public void serviceTimeEWMA(long serviceTimeEWMA) {
            this.serviceTimeEWMA = serviceTimeEWMA;
        }

        public int nodeQueueSize() {
            return this.nodeQueueSize;
        }

        public void nodeQueueSize(int nodeQueueSize) {
            this.nodeQueueSize = nodeQueueSize;
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
                    throw new IllegalArgumentException(
                        "The number of sort fields does not match: " + numFields + " != " + sortValueFormats.length
                    );
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

        public TotalHits getTotalHits() {
            return totalHits;
        }

        public float getMaxScore() {
            return maxScore;
        }

        public RescoreDocIds getRescoreDocIds() {
            return rescoreDocIds;
        }

        public void setRescoreDocIds(RescoreDocIds rescoreDocIds) {
            this.rescoreDocIds = rescoreDocIds;
        }
    }

    private final SingleSearchResult primarySearchResult = new SingleSearchResult();
    private final List<SingleSearchResult> secondarySearchResults = new ArrayList<>();

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

    public List<SingleSearchResult> getSecondarySearchResults() {
        return secondarySearchResults;
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
        primarySearchResult.terminatedEarly(terminatedEarly);
    }

    public Boolean terminatedEarly() {
        return primarySearchResult.terminatedEarly();
    }

    public TopDocsAndMaxScore topDocs() {
        return primarySearchResult.topDocs();
    }

    /**
     * Returns <code>true</code> iff the primary search results' top docs have already been consumed.
     */
    public boolean hasConsumedTopDocs() {
        return primarySearchResult.hasConsumedTopDocs();
    }

    /**
     * Returns and nulls out the top docs for the primary search results. This allows to free up memory once the top docs are consumed.
     * @throws IllegalStateException if the top docs have already been consumed.
     */
    public TopDocsAndMaxScore consumeTopDocs() {
        return primarySearchResult.consumeTopDocs();
    }

    public void topDocs(TopDocsAndMaxScore topDocs, DocValueFormat[] sortValueFormats) {
        primarySearchResult.topDocs(topDocs, sortValueFormats);
    }

    public DocValueFormat[] sortValueFormats() {
        return primarySearchResult.sortValueFormats;
    }

    /**
     * Returns <code>true</code> if this query result has unconsumed aggregations
     */
    public boolean hasAggs() {
        return hasAggs;
    }

    /**
     * Returns and nulls out the aggregation for the search result. This allows to free up memory once the aggregation is consumed.
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
     * Returns and nulls out the profiled results for the primary search result, or potentially null if result was empty.
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
        if (hasConsumedTopDocs() == false) {
            consumeTopDocs();
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

    public int from() {
        return primarySearchResult.from();
    }

    public QuerySearchResult from(int from) {
        primarySearchResult.size();
        return this;
    }

    /**
     * Returns the maximum size of the primary search results top docs.
     */
    public int size() {
        return primarySearchResult.size();
    }

    public QuerySearchResult size(int size) {
        primarySearchResult.size(size);
        return this;
    }

    public long serviceTimeEWMA() {
        return primarySearchResult.serviceTimeEWMA();
    }

    public QuerySearchResult serviceTimeEWMA(long serviceTimeEWMA) {
        primarySearchResult.serviceTimeEWMA(serviceTimeEWMA);
        return this;
    }

    public int nodeQueueSize() {
        return primarySearchResult.nodeQueueSize();
    }

    public QuerySearchResult nodeQueueSize(int nodeQueueSize) {
        primarySearchResult.nodeQueueSize(nodeQueueSize);
        return this;
    }

    /**
     * Returns <code>true</code> if this result has any suggest score docs
     */
    public boolean hasSuggestHits() {
        return (suggest != null && suggest.hasScoreDocs());
    }

    public boolean hasSearchContext() {
        return primarySearchResult.hasScoreDocs || secondarySearchResults.stream().anyMatch(ssr -> ssr.hasScoreDocs) || hasSuggestHits();
    }

    public void readFromWithId(ShardSearchContextId id, StreamInput in) throws IOException {
        readFromWithId(id, in, false);
    }

    private void readFromWithId(ShardSearchContextId id, StreamInput in, boolean delayedAggregations) throws IOException {
        this.contextId = id;
        boolean success = false;
        try {
            primarySearchResult.from = in.readVInt();
            primarySearchResult.size = in.readVInt();
            int numSortFieldsPlus1 = in.readVInt();
            if (numSortFieldsPlus1 == 0) {
                primarySearchResult.sortValueFormats = null;
            } else {
                primarySearchResult.sortValueFormats = new DocValueFormat[numSortFieldsPlus1 - 1];
                for (int i = 0; i < primarySearchResult.sortValueFormats.length; ++i) {
                    primarySearchResult.sortValueFormats[i] = in.readNamedWriteable(DocValueFormat.class);
                }
            }
            primarySearchResult.setTopDocs(readTopDocs(in));
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
            primarySearchResult.terminatedEarly = in.readOptionalBoolean();
            profileShardResults = in.readOptionalWriteable(SearchProfileQueryPhaseResult::new);
            hasProfileResults = profileShardResults != null;
            primarySearchResult.serviceTimeEWMA = in.readZLong();
            primarySearchResult.nodeQueueSize = in.readInt();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
                setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
                primarySearchResult.setRescoreDocIds(new RescoreDocIds(in));
            }
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                int ssrSize = in.readVInt();
                for (int ssrIndex = 0; ssrIndex < ssrSize; ++ssrSize) {
                    SingleSearchResult secondarySearchResult = new SingleSearchResult();
                    secondarySearchResult.from = in.readVInt();
                    secondarySearchResult.size = in.readVInt();
                    numSortFieldsPlus1 = in.readVInt();
                    if (numSortFieldsPlus1 == 0) {
                        secondarySearchResult.sortValueFormats = null;
                    } else {
                        secondarySearchResult.sortValueFormats = new DocValueFormat[numSortFieldsPlus1 - 1];
                        for (int i = 0; i < secondarySearchResult.sortValueFormats.length; ++i) {
                            secondarySearchResult.sortValueFormats[i] = in.readNamedWriteable(DocValueFormat.class);
                        }
                    }
                    secondarySearchResult.setTopDocs(readTopDocs(in));
                    secondarySearchResult.terminatedEarly = in.readOptionalBoolean();
                    secondarySearchResult.serviceTimeEWMA = in.readZLong();
                    secondarySearchResult.nodeQueueSize = in.readInt();
                    secondarySearchResult.setRescoreDocIds(new RescoreDocIds(in));
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
        out.writeVInt(primarySearchResult.from);
        out.writeVInt(primarySearchResult.size);
        if (primarySearchResult.sortValueFormats == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(1 + primarySearchResult.sortValueFormats.length);
            for (int i = 0; i < primarySearchResult.sortValueFormats.length; ++i) {
                out.writeNamedWriteable(primarySearchResult.sortValueFormats[i]);
            }
        }
        writeTopDocs(out, primarySearchResult.topDocsAndMaxScore);
        out.writeOptionalWriteable(aggregations);
        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(searchTimedOut);
        out.writeOptionalBoolean(primarySearchResult.terminatedEarly);
        out.writeOptionalWriteable(profileShardResults);
        out.writeZLong(primarySearchResult.serviceTimeEWMA);
        out.writeInt(primarySearchResult.nodeQueueSize);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
            out.writeOptionalWriteable(getShardSearchRequest());
            primarySearchResult.getRescoreDocIds().writeTo(out);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeVInt(secondarySearchResults.size());
            for (SingleSearchResult secondarySearchResult : secondarySearchResults) {
                out.writeVInt(secondarySearchResult.from);
                out.writeVInt(secondarySearchResult.size);
                if (secondarySearchResult.sortValueFormats == null) {
                    out.writeVInt(0);
                } else {
                    out.writeVInt(1 + secondarySearchResult.sortValueFormats.length);
                    for (int i = 0; i < secondarySearchResult.sortValueFormats.length; ++i) {
                        out.writeNamedWriteable(secondarySearchResult.sortValueFormats[i]);
                    }
                }
                writeTopDocs(out, secondarySearchResult.topDocsAndMaxScore);
                out.writeOptionalBoolean(secondarySearchResult.terminatedEarly);
                out.writeZLong(secondarySearchResult.serviceTimeEWMA);
                out.writeInt(secondarySearchResult.nodeQueueSize);
                secondarySearchResult.getRescoreDocIds().writeTo(out);
            }
        }
    }

    public TotalHits getTotalHits() {
        return primarySearchResult.getTotalHits();
    }

    public float getMaxScore() {
        return primarySearchResult.getMaxScore();
    }

    public RescoreDocIds getRescoreDocIds() {
        return primarySearchResult.getRescoreDocIds();
    }

    public void setRescoreDocIds(RescoreDocIds rescoreDocIds) {
        primarySearchResult.setRescoreDocIds(rescoreDocIds);
    }
}
