/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ParserConstructor;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SearchProfileDfsPhaseResult implements Writeable, ToXContentObject {

    private final ProfileResult dfsShardResult;
    private final QueryProfileShardResult queryProfileShardResult;

    @ParserConstructor
    public SearchProfileDfsPhaseResult(@Nullable ProfileResult dfsShardResult, @Nullable QueryProfileShardResult queryProfileShardResult) {
        this.dfsShardResult = dfsShardResult;
        this.queryProfileShardResult = queryProfileShardResult;
    }

    public SearchProfileDfsPhaseResult(StreamInput in) throws IOException {
        dfsShardResult = in.readOptionalWriteable(ProfileResult::new);
        queryProfileShardResult = in.readOptionalWriteable(QueryProfileShardResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(dfsShardResult);
        out.writeOptionalWriteable(queryProfileShardResult);
    }

    private static final ParseField STATISTICS = new ParseField("statistics");
    private static final ParseField KNN = new ParseField("knn");
    private static final InstantiatingObjectParser<SearchProfileDfsPhaseResult, Void> PARSER;

    static {
        InstantiatingObjectParser.Builder<SearchProfileDfsPhaseResult, Void> parser = InstantiatingObjectParser.builder(
            "search_profile_dfs_phase_result",
            true,
            SearchProfileDfsPhaseResult.class
        );
        parser.declareObject(optionalConstructorArg(), (p, c) -> ProfileResult.fromXContent(p), STATISTICS);
        parser.declareObject(optionalConstructorArg(), (p, c) -> QueryProfileShardResult.fromXContent(p), KNN);
        PARSER = parser.build();
    }

    public static SearchProfileDfsPhaseResult fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (dfsShardResult != null) {
            builder.field(STATISTICS.getPreferredName());
            dfsShardResult.toXContent(builder, params);
        }
        if (queryProfileShardResult != null) {
            builder.field(KNN.getPreferredName());
            queryProfileShardResult.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchProfileDfsPhaseResult that = (SearchProfileDfsPhaseResult) o;
        return Objects.equals(dfsShardResult, that.dfsShardResult) && Objects.equals(queryProfileShardResult, that.queryProfileShardResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dfsShardResult, queryProfileShardResult);
    }

    @Override
    public String toString() {
        return "SearchProfileDfsPhaseResult{"
            + "dfsShardResult="
            + dfsShardResult
            + ", queryProfileShardResult="
            + queryProfileShardResult
            + '}';
    }
}
