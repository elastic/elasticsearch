/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.transport.LeakTracker;

import java.io.IOException;

import static org.elasticsearch.common.lucene.Lucene.readTopDocs;
import static org.elasticsearch.common.lucene.Lucene.writeTopDocs;

public final class QuerySearchResult extends SearchPhaseResult {
    private int from;
    private int size;
    private TopDocsAndMaxScore topDocsAndMaxScore;
    private boolean hasScoreDocs;
    private RankShardResult rankShardResult;
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
    private SearchProfileQueryPhaseResult profileShardResults;
    private boolean hasProfileResults;
    private long serviceTimeEWMA = -1;
    private int nodeQueueSize = -1;

    private boolean reduced;

    private final boolean isNull;

    private final RefCounted refCounted;

    private final SubscribableListener<Void> aggsContextReleased;

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
        isNull = in.readBoolean();
        if (isNull == false) {
            ShardSearchContextId id = versionSupportsBatchedExecution(in.getTransportVersion())
                ? in.readOptionalWriteable(ShardSearchContextId::new)
                : new ShardSearchContextId(in);
            readFromWithId(id, in, delayedAggregations);
        }
        refCounted = null;
        aggsContextReleased = null;
    }

    public QuerySearchResult(ShardSearchContextId contextId, SearchShardTarget shardTarget, ShardSearchRequest shardSearchRequest) {
        this.contextId = contextId;
        setSearchShardTarget(shardTarget);
        isNull = false;
        setShardSearchRequest(shardSearchRequest);
        this.refCounted = LeakTracker.wrap(new SimpleRefCounted());
        this.aggsContextReleased = new SubscribableListener<>();
    }

    private QuerySearchResult(boolean isNull) {
        this.isNull = isNull;
        this.refCounted = null;
        aggsContextReleased = null;
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

    /**
     * @return true if this result was already partially reduced on the data node that it originated on so that the coordinating node
     * will skip trying to merge aggregations and top-hits from this instance on the final reduce pass
     */
    public boolean isPartiallyReduced() {
        return reduced;
    }

    /**
     * See {@link #isPartiallyReduced()}, calling this method marks this hit as having undergone partial reduction on the data node.
     */
    public void markAsPartiallyReduced() {
        assert (hasConsumedTopDocs() || topDocsAndMaxScore.topDocs.scoreDocs.length == 0) && aggregations == null
            : "result not yet partially reduced [" + topDocsAndMaxScore + "][" + aggregations + "]";
        this.reduced = true;
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

    @Nullable
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

    public void setRankShardResult(RankShardResult rankShardResult) {
        this.rankShardResult = rankShardResult;
    }

    @Nullable
    public RankShardResult getRankShardResult() {
        return rankShardResult;
    }

    @Nullable
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
     * Returns the aggregation as a {@link DelayableWriteable} object. Callers are free to expand them whenever they wat
     * but they should call {@link #releaseAggs()} in order to free memory,
     * @throws IllegalStateException if {@link #releaseAggs()} has already being called.
     */
    public DelayableWriteable<InternalAggregations> getAggs() {
        if (aggregations == null) {
            throw new IllegalStateException("aggs already released");
        }
        return aggregations;
    }

    public DelayableWriteable<InternalAggregations> consumeAggs() {
        if (aggregations == null) {
            throw new IllegalStateException("aggs already released");
        }
        var res = aggregations;
        aggregations = null;
        return res;
    }

    /**
     * Release the memory hold by the {@link DelayableWriteable} aggregations
     * @throws IllegalStateException if {@link #releaseAggs()} has already being called.
     */
    public void releaseAggs() {
        if (aggregations != null) {
            aggregations.close();
            aggregations = null;
        }
        releaseAggsContext();
    }

    public void addAggregationContext(AggregationContext aggsContext) {
        aggsContextReleased.addListener(ActionListener.releasing(aggsContext));
    }

    public void aggregations(InternalAggregations aggregations) {
        assert this.aggregations == null : "aggregations already set to [" + this.aggregations + "]";
        this.aggregations = aggregations == null ? null : DelayableWriteable.referencing(aggregations);
        hasAggs = aggregations != null;
        releaseAggsContext();
    }

    private void releaseAggsContext() {
        if (aggsContextReleased != null) {
            aggsContextReleased.onResponse(null);
        }
    }

    @Nullable
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
        return hasScoreDocs || hasSuggestHits() || rankShardResult != null;
    }

    public void readFromWithId(ShardSearchContextId id, StreamInput in) throws IOException {
        readFromWithId(id, in, false);
    }

    private void readFromWithId(ShardSearchContextId id, StreamInput in, boolean delayedAggregations) throws IOException {
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
        if (versionSupportsBatchedExecution(in.getTransportVersion())) {
            if (in.readBoolean()) {
                setTopDocs(readTopDocs(in));
            }
        } else {
            setTopDocs(readTopDocs(in));
        }
        hasAggs = in.readBoolean();
        boolean success = false;
        try {
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
            serviceTimeEWMA = in.readZLong();
            nodeQueueSize = in.readInt();
            setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
            setRescoreDocIds(new RescoreDocIds(in));
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                rankShardResult = in.readOptionalNamedWriteable(RankShardResult.class);
                if (versionSupportsBatchedExecution(in.getTransportVersion())) {
                    reduced = in.readBoolean();
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
        out.writeBoolean(isNull);
        if (isNull == false) {
            if (versionSupportsBatchedExecution(out.getTransportVersion())) {
                out.writeOptionalWriteable(contextId);
            } else {
                contextId.writeTo(out);
            }
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
        if (versionSupportsBatchedExecution(out.getTransportVersion())) {
            if (topDocsAndMaxScore != null) {
                out.writeBoolean(true);
                writeTopDocs(out, topDocsAndMaxScore);
            } else {
                assert isPartiallyReduced();
                out.writeBoolean(false);
            }
        } else {
            writeTopDocs(out, topDocsAndMaxScore);
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
        out.writeZLong(serviceTimeEWMA);
        out.writeInt(nodeQueueSize);
        out.writeOptionalWriteable(getShardSearchRequest());
        getRescoreDocIds().writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeOptionalNamedWriteable(rankShardResult);
        } else if (rankShardResult != null) {
            throw new IllegalArgumentException("cannot serialize [rank] to version [" + out.getTransportVersion().toReleaseVersion() + "]");
        }
        if (versionSupportsBatchedExecution(out.getTransportVersion())) {
            out.writeBoolean(reduced);
        }
    }

    @Nullable
    public TotalHits getTotalHits() {
        return totalHits;
    }

    public float getMaxScore() {
        return maxScore;
    }

    @Override
    public void incRef() {
        if (refCounted != null) {
            refCounted.incRef();
        } else {
            super.incRef();
        }
    }

    @Override
    public boolean tryIncRef() {
        if (refCounted != null) {
            return refCounted.tryIncRef();
        }
        return super.tryIncRef();
    }

    @Override
    public boolean decRef() {
        if (refCounted != null) {
            if (refCounted.decRef()) {
                aggsContextReleased.onResponse(null);
                return true;
            }
            return false;
        }
        return super.decRef();
    }

    @Override
    public boolean hasReferences() {
        if (refCounted != null) {
            return refCounted.hasReferences();
        }
        return super.hasReferences();
    }

    private static boolean versionSupportsBatchedExecution(TransportVersion transportVersion) {
        return transportVersion.onOrAfter(TransportVersions.BATCHED_QUERY_PHASE_VERSION)
            || transportVersion.isPatchFrom(TransportVersions.BATCHED_QUERY_PHASE_VERSION_BACKPORT_8_X);
    }
}
