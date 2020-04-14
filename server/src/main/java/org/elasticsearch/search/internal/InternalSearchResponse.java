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

package org.elasticsearch.search.internal;

import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;

/**
 * {@link SearchResponseSections} subclass that can be serialized over the wire.
 */
public class InternalSearchResponse extends SearchResponseSections implements Writeable, ToXContentFragment {
    public static InternalSearchResponse empty() {
        return empty(true);
    }

    public static InternalSearchResponse empty(boolean withTotalHits) {
        return new InternalSearchResponse(SearchHits.empty(withTotalHits), null, null, null, false, null, 1);
    }

    private final boolean hasAggregations;
    private DelayableWriteable<InternalAggregations> aggregations;

    public InternalSearchResponse(SearchHits hits, DelayableWriteable<InternalAggregations> aggregations, Suggest suggest,
                                  SearchProfileShardResults profileResults, boolean timedOut, Boolean terminatedEarly,
                                  int numReducePhases) {
        super(hits, suggest, timedOut, terminatedEarly, profileResults, numReducePhases);
        this.hasAggregations = aggregations != null;
        this.aggregations = aggregations;
    }

    public static InternalSearchResponse read(StreamInput in) throws IOException {
        SearchHits hits = new SearchHits(in);
        /*
         * We don't delay reading the aggs here because it'd do two things:
         * 1. Delay reading aggs for cross cluster search until all clusters
         *    are ready to be reduced. This is almost certainly a good thing.
         * 2. Delay reading aggs in the any *other* time we serialize the
         *    search response.
         * We haven't done the digging to be sure that the second one is ok yet. 
         */
        DelayableWriteable<InternalAggregations> aggs = in.readOptionalWriteable(
                si -> DelayableWriteable.referencing(new InternalAggregations(si)));
        Suggest suggest = in.readOptionalWriteable(Suggest::new);
        boolean timedOut = in.readBoolean();
        Boolean terminatedEarly = in.readOptionalBoolean();
        SearchProfileShardResults profileResults = in.readOptionalWriteable(SearchProfileShardResults::new);
        int numReducePhases = in.readVInt();
        return new InternalSearchResponse(hits, aggs, suggest, profileResults, timedOut, terminatedEarly, numReducePhases);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        hits.writeTo(out);
        out.writeOptionalWriteable(aggregations == null ? null : aggregations.get());
        out.writeOptionalWriteable(suggest);
        out.writeBoolean(timedOut);
        out.writeOptionalBoolean(terminatedEarly);
        out.writeOptionalWriteable(profileResults);
        out.writeVInt(numReducePhases);
    }

    @Override
    public InternalAggregations aggregations() {
        if (hasAggregations) {
            if (aggregations == null) {
                throw new IllegalStateException("aggregations already consumed");
            }
            return aggregations.get();
        }
        return null;
    }

    /**
     * Get the aggregation results and remove the reference to them so they
     * can be GCed.
     */
    public InternalAggregations consumeAggregations() {
        InternalAggregations aggs = aggregations();
        this.aggregations = null;
        return aggs;
    }
}
