/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.aggregation;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A container class to hold the profile results for a single shard in the request.
 * Contains a list of query profiles, a collector tree and a total rewrite tree.
 */
public final class AggregationProfileShardResult implements Writeable, ToXContentFragment {

    public static final String AGGREGATIONS = "aggregations";
    private final List<ProfileResult> aggProfileResults;

    public AggregationProfileShardResult(List<ProfileResult> aggProfileResults) {
        this.aggProfileResults = aggProfileResults;
    }

    /**
     * Read from a stream.
     */
    public AggregationProfileShardResult(StreamInput in) throws IOException {
        int profileSize = in.readVInt();
        aggProfileResults = new ArrayList<>(profileSize);
        for (int j = 0; j < profileSize; j++) {
            aggProfileResults.add(new ProfileResult(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(aggProfileResults);
    }

    public List<ProfileResult> getProfileResults() {
        return Collections.unmodifiableList(aggProfileResults);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(AGGREGATIONS);
        for (ProfileResult p : aggProfileResults) {
            p.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AggregationProfileShardResult other = (AggregationProfileShardResult) obj;
        return aggProfileResults.equals(other.aggProfileResults);
    }

    @Override
    public int hashCode() {
        return aggProfileResults.hashCode();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
