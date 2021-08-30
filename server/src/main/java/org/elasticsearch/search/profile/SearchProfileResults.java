package org.elasticsearch.search.profile;

import org.elasticsearch.Version;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Profile results for all shards.
 */
public final class SearchProfileResults implements Writeable, ToXContentFragment {

    private static final String SEARCHES_FIELD = "searches";
    private static final String ID_FIELD = "id";
    private static final String SHARDS_FIELD = "shards";
    public static final String PROFILE_FIELD = "profile";

    private Map<String, SearchProfileShardResult> shardResults;

    public SearchProfileResults(Map<String, SearchProfileShardResult> shardResults) {
        this.shardResults =  Collections.unmodifiableMap(shardResults);
    }

    public SearchProfileResults(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            shardResults = in.readMap(StreamInput::readString, SearchProfileShardResult::new);
        } else {
            // Before 8.0.0 we only send the search profile result
            shardResults = in.readMap(
                StreamInput::readString,
                i -> new SearchProfileShardResult(new SearchProfileQueryPhaseResult(in), null)
            );
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeMap(shardResults, StreamOutput::writeString, (o, r) -> r.writeTo(o));
        } else {
            // Before 8.0.0 we only send the search profile result
            out.writeMap(shardResults, StreamOutput::writeString, (o, r) -> r.getSearch().writeTo(o));
        }
    }

    public Map<String, SearchProfileShardResult> getShardResults() {
        return shardResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PROFILE_FIELD).startArray(SHARDS_FIELD);
        // shardResults is a map, but we print entries in a json array, which is ordered.
        // we sort the keys of the map, so that toXContent always prints out the same array order
        TreeSet<String> sortedKeys = new TreeSet<>(shardResults.keySet());
        for (String key : sortedKeys) {
            builder.startObject();
            builder.field(ID_FIELD, key);
            builder.startArray(SEARCHES_FIELD);
            SearchProfileShardResult profileShardResult = shardResults.get(key);
            for (QueryProfileShardResult result : profileShardResult.getSearch().getQueryProfileResults()) {
                result.toXContent(builder, params);
            }
            builder.endArray();
            profileShardResult.getSearch().getAggregationProfileResults().toXContent(builder, params);
            if (profileShardResult.getFetch() != null) {
                builder.field("fetch");
                profileShardResult.getFetch().toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endArray().endObject();
        return builder;
    }

    public static SearchProfileResults fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        Map<String, SearchProfileShardResult> profileResults = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_ARRAY) {
                if (SHARDS_FIELD.equals(parser.currentName())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseProfileResultsEntry(parser, profileResults);
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.skipChildren();
            }
        }
        return new SearchProfileResults(profileResults);
    }

    private static void parseProfileResultsEntry(XContentParser parser,
            Map<String, SearchProfileShardResult> searchProfileResults) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        List<QueryProfileShardResult> queryProfileResults = new ArrayList<>();
        AggregationProfileShardResult aggProfileShardResult = null;
        ProfileResult fetchResult = null;
        String id = null;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (ID_FIELD.equals(currentFieldName)) {
                    id = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (SEARCHES_FIELD.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        queryProfileResults.add(QueryProfileShardResult.fromXContent(parser));
                    }
                } else if (AggregationProfileShardResult.AGGREGATIONS.equals(currentFieldName)) {
                    aggProfileShardResult = AggregationProfileShardResult.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                fetchResult = ProfileResult.fromXContent(parser);
            } else {
                parser.skipChildren();
            }
        }
        SearchProfileShardResult result = new SearchProfileShardResult(
            new SearchProfileQueryPhaseResult(queryProfileResults, aggProfileShardResult),
            fetchResult
        );
        searchProfileResults.put(id, result);
    }
}
