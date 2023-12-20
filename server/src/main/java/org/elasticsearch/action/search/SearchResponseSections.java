/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Base class that holds the various sections which a search response is
 * composed of (hits, aggs, suggestions etc.) and allows to retrieve them.
 *
 * The reason why this class exists is that the high level REST client uses its own classes
 * to parse aggregations into, which are not serializable. This is the common part that can be
 * shared between core and client.
 */
public class SearchResponseSections implements ChunkedToXContent {

    protected final SearchHits hits;
    protected final Aggregations aggregations;
    protected final Suggest suggest;
    protected final SearchProfileResults profileResults;
    protected final boolean timedOut;
    protected final Boolean terminatedEarly;
    protected final int numReducePhases;

    public SearchResponseSections(
        SearchHits hits,
        Aggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileResults profileResults,
        int numReducePhases
    ) {
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
    public final Map<String, SearchProfileShardResult> profile() {
        if (profileResults == null) {
            return Collections.emptyMap();
        }
        return profileResults.getShardResults();
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            Iterators.flatMap(Iterators.single(hits), r -> r.toXContentChunked(params)),
            Iterators.single((ToXContent) (b, p) -> {
                if (aggregations != null) {
                    aggregations.toXContent(b, p);
                }
                return b;
            }),
            Iterators.single((b, p) -> {
                if (suggest != null) {
                    suggest.toXContent(b, p);
                }
                return b;
            }),
            Iterators.single((b, p) -> {
                if (profileResults != null) {
                    profileResults.toXContent(b, p);
                }
                return b;
            })
        );
    }

    protected void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}
