/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.List;

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

    public InternalSearchResponse(SearchHits hits, InternalAggregations aggregations, Suggest suggest,
                                  SearchProfileResults profileResults, boolean timedOut, Boolean terminatedEarly,
                                  int numReducePhases) {
        super(hits, aggregations, suggest, timedOut, terminatedEarly, profileResults, numReducePhases);
    }

    public InternalSearchResponse(StreamInput in) throws IOException {
        super(
            new SearchHits(in),
            in.readBoolean() ? InternalAggregations.readFrom(in) : null,
            in.readBoolean() ? new Suggest(in) : null,
            in.readBoolean(),
            in.readOptionalBoolean(),
            in.getVersion().onOrAfter(Version.V_8_0_0)
                ? in.readOptionalWriteable(SearchProfileResults::new)
                : in.readOptionalWriteable(SearchProfileShardResults::new).merge(List.of()),
            in.readVInt()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        hits.writeTo(out);
        out.writeOptionalWriteable((InternalAggregations)aggregations);
        out.writeOptionalWriteable(suggest);
        out.writeBoolean(timedOut);
        out.writeOptionalBoolean(terminatedEarly);
        out.writeOptionalWriteable(profileResults);
        out.writeVInt(numReducePhases);
    }
}
