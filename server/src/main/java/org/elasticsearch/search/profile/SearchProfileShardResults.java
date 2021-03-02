package org.elasticsearch.search.profile;

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
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfiler;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A container class to hold all the profile results across all shards.  Internally
 * holds a map of shard ID -&gt; Profiled results
 */
public final class SearchProfileShardResults implements Writeable, ToXContentFragment {

    private static final String SEARCHES_FIELD = "searches";
    private static final String ID_FIELD = "id";
    private static final String SHARDS_FIELD = "shards";
    public static final String PROFILE_FIELD = "profile";

    private Map<String, ProfileShardResult> shardResults;

    public SearchProfileShardResults(Map<String, ProfileShardResult> shardResults) {
        this.shardResults =  Collections.unmodifiableMap(shardResults);
    }

    public SearchProfileShardResults(StreamInput in) throws IOException {
        int size = in.readInt();
        shardResults = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            String key = in.readString();
            ProfileShardResult shardResult = new ProfileShardResult(in);
            shardResults.put(key, shardResult);
        }
        shardResults = Collections.unmodifiableMap(shardResults);
    }

    public Map<String, ProfileShardResult> getShardResults() {
        return this.shardResults;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardResults.size());
        for (Map.Entry<String, ProfileShardResult> entry : shardResults.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
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
            ProfileShardResult profileShardResult = shardResults.get(key);
            for (QueryProfileShardResult result : profileShardResult.getQueryProfileResults()) {
                result.toXContent(builder, params);
            }
            builder.endArray();
            profileShardResult.getAggregationProfileResults().toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray().endObject();
        return builder;
    }

    public static SearchProfileShardResults fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        Map<String, ProfileShardResult> searchProfileResults = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_ARRAY) {
                if (SHARDS_FIELD.equals(parser.currentName())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseSearchProfileResultsEntry(parser, searchProfileResults);
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.skipChildren();
            }
        }
        return new SearchProfileShardResults(searchProfileResults);
    }

    private static void parseSearchProfileResultsEntry(XContentParser parser,
            Map<String, ProfileShardResult> searchProfileResults) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        List<QueryProfileShardResult> queryProfileResults = new ArrayList<>();
        AggregationProfileShardResult aggProfileShardResult = null;
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
            } else {
                parser.skipChildren();
            }
        }
        searchProfileResults.put(id, new ProfileShardResult(queryProfileResults, aggProfileShardResult));
    }

    /**
     * Helper method to convert Profiler into InternalProfileShardResults, which
     * can be serialized to other nodes, emitted as JSON, etc.
     *
     * @param profilers
     *            The {@link Profilers} to convert into results
     * @return A {@link ProfileShardResult} representing the results for this
     *         shard
     */
    public static ProfileShardResult buildShardResults(Profilers profilers) {
        List<QueryProfiler> queryProfilers = profilers.getQueryProfilers();
        AggregationProfiler aggProfiler = profilers.getAggregationProfiler();
        List<QueryProfileShardResult> queryResults = new ArrayList<>(queryProfilers.size());
        for (QueryProfiler queryProfiler : queryProfilers) {
            QueryProfileShardResult result = new QueryProfileShardResult(queryProfiler.getTree(), queryProfiler.getRewriteTime(),
                    queryProfiler.getCollector());
            queryResults.add(result);
        }
        AggregationProfileShardResult aggResults = new AggregationProfileShardResult(aggProfiler.getTree());
        return new ProfileShardResult(queryResults, aggResults);
    }
}
