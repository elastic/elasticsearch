/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;

/**
 * Wraps a {@link QuerySearchResult} together with an adapter that can be used to
 * translate calls to pre 7.10 nodes when using the "fields" option and to translate
 * the resulting response back
 */
final class WrappedQuerySearchResult extends QuerySearchResult {

    private final FieldsOptionSourceAdapter adapter;
    private final QuerySearchResult original;

    WrappedQuerySearchResult(QuerySearchResult original, FieldsOptionSourceAdapter adapter) {
        this.original = original;
        this.adapter = adapter;
    }

    /**
     * @return the adapter to use or <code>null</code> if no adaption is necessary
     */
    FieldsOptionSourceAdapter getAdapter() {
        return this.adapter;
    }

    @Override
    public void remoteAddress(TransportAddress remoteAddress) {
        original.remoteAddress(remoteAddress);
    }

    @Override
    public TransportAddress remoteAddress() {
        return original.remoteAddress();
    }

    @Override
    public void incRef() {
        original.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return original.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return original.decRef();
    }

    @Override
    public int hashCode() {
        return original.hashCode();
    }

    @Override
    public ShardSearchContextId getContextId() {
        return original.getContextId();
    }

    @Override
    public int getShardIndex() {
        return original.getShardIndex();
    }

    @Override
    public SearchShardTarget getSearchShardTarget() {
        return original.getSearchShardTarget();
    }

    @Override
    public void setSearchShardTarget(SearchShardTarget shardTarget) {
        original.setSearchShardTarget(shardTarget);
    }

    @Override
    public void setShardIndex(int shardIndex) {
        original.setShardIndex(shardIndex);
    }

    @Override
    public FetchSearchResult fetchResult() {
        return original.fetchResult();
    }

    @Override
    public ShardSearchRequest getShardSearchRequest() {
        return original.getShardSearchRequest();
    }

    @Override
    public void setShardSearchRequest(ShardSearchRequest shardSearchRequest) {
        original.setShardSearchRequest(shardSearchRequest);
    }

    @Override
    public RescoreDocIds getRescoreDocIds() {
        return original.getRescoreDocIds();
    }

    @Override
    public boolean equals(Object obj) {
        return original.equals(obj);
    }

    @Override
    public void setRescoreDocIds(RescoreDocIds rescoreDocIds) {
        original.setRescoreDocIds(rescoreDocIds);
    }

    @Override
    public boolean isNull() {
        return original.isNull();
    }

    @Override
    public QuerySearchResult queryResult() {
        return this;
    }

    @Override
    public void searchTimedOut(boolean searchTimedOut) {
        original.searchTimedOut(searchTimedOut);
    }

    @Override
    public boolean searchTimedOut() {
        return original.searchTimedOut();
    }

    @Override
    public void terminatedEarly(boolean terminatedEarly) {
        original.terminatedEarly(terminatedEarly);
    }

    @Override
    public Boolean terminatedEarly() {
        return original.terminatedEarly();
    }

    @Override
    public TopDocsAndMaxScore topDocs() {
        return original.topDocs();
    }

    @Override
    public boolean hasConsumedTopDocs() {
        return original.hasConsumedTopDocs();
    }

    @Override
    public TopDocsAndMaxScore consumeTopDocs() {
        return original.consumeTopDocs();
    }

    @Override
    public void topDocs(TopDocsAndMaxScore topDocs, DocValueFormat[] sortValueFormats) {
        original.topDocs(topDocs, sortValueFormats);
    }

    @Override
    public DocValueFormat[] sortValueFormats() {
        return original.sortValueFormats();
    }

    @Override
    public boolean hasAggs() {
        return original.hasAggs();
    }

    @Override
    public InternalAggregations consumeAggs() {
        return original.consumeAggs();
    }

    @Override
    public void releaseAggs() {
        original.releaseAggs();
    }

    @Override
    public void aggregations(InternalAggregations aggregations) {
        original.aggregations(aggregations);
    }

    @Override
    public DelayableWriteable<InternalAggregations> aggregations() {
        return original.aggregations();
    }

    @Override
    public ProfileShardResult consumeProfileResult() {
        return original.consumeProfileResult();
    }

    @Override
    public boolean hasProfileResults() {
        return original.hasProfileResults();
    }

    @Override
    public void consumeAll() {
        original.consumeAll();
    }

    @Override
    public String toString() {
        return original.toString();
    }

    @Override
    public void profileResults(ProfileShardResult shardResults) {
        original.profileResults(shardResults);
    }

    @Override
    public Suggest suggest() {
        return original.suggest();
    }

    @Override
    public void suggest(Suggest suggest) {
        original.suggest(suggest);
    }

    @Override
    public int from() {
        return original.from();
    }

    @Override
    public QuerySearchResult from(int from) {
        return original.from(from);
    }

    @Override
    public int size() {
        return original.size();
    }

    @Override
    public QuerySearchResult size(int size) {
        return original.size(size);
    }

    @Override
    public long serviceTimeEWMA() {
        return original.serviceTimeEWMA();
    }

    @Override
    public QuerySearchResult serviceTimeEWMA(long serviceTimeEWMA) {
        return original.serviceTimeEWMA(serviceTimeEWMA);
    }

    @Override
    public int nodeQueueSize() {
        return original.nodeQueueSize();
    }

    @Override
    public QuerySearchResult nodeQueueSize(int nodeQueueSize) {
        return original.nodeQueueSize(nodeQueueSize);
    }

    @Override
    public boolean hasSuggestHits() {
        return original.hasSuggestHits();
    }

    @Override
    public boolean hasSearchContext() {
        return original.hasSearchContext();
    }

    @Override
    public void readFromWithId(ShardSearchContextId id, StreamInput in) throws IOException {
        original.readFromWithId(id, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        original.writeTo(out);
    }

    @Override
    public void writeToNoId(StreamOutput out) throws IOException {
        original.writeToNoId(out);
    }

    @Override
    public TotalHits getTotalHits() {
        return original.getTotalHits();
    }

    @Override
    public float getMaxScore() {
        return original.getMaxScore();
    }
}