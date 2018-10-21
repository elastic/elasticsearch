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

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Base class that holds the various sections which a search response is
 * composed of (hits, aggs, suggestions etc.) and allows to retrieve them.
 *
 * The reason why this class exists is that the high level REST client uses its own classes
 * to parse aggregations into, which are not serializable. This is the common part that can be
 * shared between core and client.
 */
public class SearchResponseSections implements ToXContentFragment {

    protected final SearchHits hits;
    protected final Aggregations aggregations;
    protected final Suggest suggest;
    protected final SearchProfileShardResults profileResults;
    protected final boolean timedOut;
    protected final Boolean terminatedEarly;
    protected final int numReducePhases;

    public SearchResponseSections(SearchHits hits, Aggregations aggregations, Suggest suggest, boolean timedOut, Boolean terminatedEarly,
                                  SearchProfileShardResults profileResults,  int numReducePhases) {
        this.hits = hits;
        this.aggregations = aggregations;
        this.suggest = suggest;
        this.profileResults = profileResults;
        this.timedOut = timedOut;
        this.terminatedEarly = terminatedEarly;
        this.numReducePhases = numReducePhases;
    }

    public final boolean timedOut() {
        return this.timedOut;
    }

    public final Boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    public final SearchHits hits() {
        return hits;
    }

    public final Aggregations aggregations() {
        return aggregations;
    }

    public final Suggest suggest() {
        return suggest;
    }

    /**
     * Returns the number of reduce phases applied to obtain this search response
     */
    public final int getNumReducePhases() {
        return numReducePhases;
    }

    /**
     * Returns the profile results for this search response (including all shards).
     * An empty map is returned if profiling was not enabled
     *
     * @return Profile results
     */
    public final Map<String, ProfileShardResult> profile() {
        if (profileResults == null) {
            return Collections.emptyMap();
        }
        return profileResults.getShardResults();
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        hits.toXContent(builder, params);
        if (aggregations != null) {
            aggregations.toXContent(builder, params);
        }
        if (suggest != null) {
            suggest.toXContent(builder, params);
        }
        if (profileResults != null) {
            profileResults.toXContent(builder, params);
        }
        return builder;
    }

    protected void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}
