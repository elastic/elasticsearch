/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A container class to hold the profile results for a single shard in the request.
 * Contains a list of query profiles, a collector tree and a total rewrite tree.
 */
public final class QueryProfileShardResult implements Writeable, ToXContentObject {

    public static final String COLLECTOR = "collector";
    public static final String REWRITE_TIME = "rewrite_time";
    public static final String QUERY_ARRAY = "query";

    public static final String VECTOR_OPERATIONS_COUNT = "vector_operations_count";
    public static final String KNN_PROFILE = "knn_profile";

    private final List<ProfileResult> queryProfileResults;

    private final CollectorResult profileCollector;

    private final long rewriteTime;

    private final Long vectorOperationsCount;

    @Nullable
    private final Map<String, Object> knnProfileBreakdown;

    public QueryProfileShardResult(
        List<ProfileResult> queryProfileResults,
        long rewriteTime,
        CollectorResult profileCollector,
        @Nullable Long vectorOperationsCount
    ) {
        this(queryProfileResults, rewriteTime, profileCollector, vectorOperationsCount, null);
    }

    public QueryProfileShardResult(
        List<ProfileResult> queryProfileResults,
        long rewriteTime,
        CollectorResult profileCollector,
        @Nullable Long vectorOperationsCount,
        @Nullable Map<String, Object> knnProfileBreakdown
    ) {
        assert (profileCollector != null);
        this.queryProfileResults = queryProfileResults;
        this.profileCollector = profileCollector;
        this.rewriteTime = rewriteTime;
        this.vectorOperationsCount = vectorOperationsCount;
        this.knnProfileBreakdown = knnProfileBreakdown;
    }

    /**
     * Read from a stream.
     */
    private static final TransportVersion KNN_PROFILE_BREAKDOWN_VERSION = TransportVersion.fromName("knn_profile_breakdown");

    @SuppressWarnings("unchecked")
    public QueryProfileShardResult(StreamInput in) throws IOException {
        int profileSize = in.readVInt();
        queryProfileResults = new ArrayList<>(profileSize);
        for (int j = 0; j < profileSize; j++) {
            queryProfileResults.add(new ProfileResult(in));
        }

        profileCollector = new CollectorResult(in);
        rewriteTime = in.readLong();
        vectorOperationsCount = in.readOptionalLong();
        if (in.getTransportVersion().supports(KNN_PROFILE_BREAKDOWN_VERSION)) {
            knnProfileBreakdown = (Map<String, Object>) in.readGenericValue();
        } else {
            knnProfileBreakdown = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(queryProfileResults.size());
        for (ProfileResult p : queryProfileResults) {
            p.writeTo(out);
        }
        profileCollector.writeTo(out);
        out.writeLong(rewriteTime);
        out.writeOptionalLong(vectorOperationsCount);
        if (out.getTransportVersion().supports(KNN_PROFILE_BREAKDOWN_VERSION)) {
            out.writeGenericValue(knnProfileBreakdown);
        }
    }

    public List<ProfileResult> getQueryResults() {
        return Collections.unmodifiableList(queryProfileResults);
    }

    public long getRewriteTime() {
        return rewriteTime;
    }

    public CollectorResult getCollectorResult() {
        return profileCollector;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (vectorOperationsCount != null) {
            builder.field(VECTOR_OPERATIONS_COUNT, vectorOperationsCount);
        }
        if (knnProfileBreakdown != null && knnProfileBreakdown.isEmpty() == false) {
            builder.field(KNN_PROFILE, knnProfileBreakdown);
        }
        builder.startArray(QUERY_ARRAY);
        for (ProfileResult p : queryProfileResults) {
            p.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(REWRITE_TIME, rewriteTime);
        builder.startArray(COLLECTOR);
        profileCollector.toXContent(builder, params);
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QueryProfileShardResult other = (QueryProfileShardResult) obj;
        return queryProfileResults.equals(other.queryProfileResults)
            && profileCollector.equals(other.profileCollector)
            && rewriteTime == other.rewriteTime
            && Objects.equals(vectorOperationsCount, other.vectorOperationsCount)
            && Objects.equals(knnProfileBreakdown, other.knnProfileBreakdown);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryProfileResults, profileCollector, rewriteTime, vectorOperationsCount, knnProfileBreakdown);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Nullable
    public Map<String, Object> getKnnProfileBreakdown() {
        return knnProfileBreakdown;
    }

    /**
     * Collapses the per-subtree kNN breakdowns a {@link QueryProfiler} accumulated into the single
     * {@code knn_profile} map that is serialized. Returns {@code null} when empty, the single breakdown
     * when there is exactly one, and a {@code {"knn_queries": [...]}} wrapper when a single search carried
     * several kNN queries (e.g. a {@code bool} with multiple {@code knn} clauses).
     */
    @Nullable
    public static Map<String, Object> collapseKnnProfileBreakdowns(@Nullable List<Map<String, Object>> breakdowns) {
        if (breakdowns == null || breakdowns.isEmpty()) {
            return null;
        }
        if (breakdowns.size() == 1) {
            return breakdowns.get(0);
        }
        Map<String, Object> combined = new LinkedHashMap<>();
        combined.put("knn_queries", new ArrayList<>(breakdowns));
        return combined;
    }

    public Long getVectorOperationsCount() {
        return vectorOperationsCount;
    }
}
