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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;
import static org.elasticsearch.search.internal.InternalSearchHits.readSearchHits;

/**
 * A response of a search request.
 */
public class SearchResponse extends ActionResponse implements StatusToXContentObject {

    private InternalSearchResponse internalResponse;

    private String scrollId;

    private int totalShards;

    private int successfulShards;

    private ShardSearchFailure[] shardFailures;

    private long tookInMillis;

    public SearchResponse() {
    }

    public SearchResponse(InternalSearchResponse internalResponse, String scrollId, int totalShards, int successfulShards, long tookInMillis, ShardSearchFailure[] shardFailures) {
        this.internalResponse = internalResponse;
        this.scrollId = scrollId;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.tookInMillis = tookInMillis;
        this.shardFailures = shardFailures;
    }

    @Override
    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }

    /**
     * The search hits.
     */
    public SearchHits getHits() {
        return internalResponse.hits;
    }

    public Aggregations getAggregations() {
        return internalResponse.aggregations;
    }


    public Suggest getSuggest() {
        return internalResponse.suggest;
    }

    /**
     * Has the search operation timed out.
     */
    public boolean isTimedOut() {
        return internalResponse.timedOut;
    }

    /**
     * Has the search operation terminated early due to reaching
     * <code>terminateAfter</code>
     */
    public Boolean isTerminatedEarly() {
        return internalResponse.terminatedEarly;
    }

    /**
     * How long the search took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the search took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * The total number of shards the search was executed on.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        // we don't return totalShards - successfulShards, we don't count "no shards available" as a failed shard, just don't
        // count it in the successful counter
        return shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] getShardFailures() {
        return this.shardFailures;
    }

    /**
     * If scrolling was enabled ({@link SearchRequest#scroll(org.elasticsearch.search.Scroll)}, the
     * scroll id that can be used to continue scrolling.
     */
    public String getScrollId() {
        return scrollId;
    }

    public void scrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    /**
     * If profiling was enabled, this returns an object containing the profile results from
     * each shard.  If profiling was not enabled, this will return null
     *
     * @return The profile results or an empty map
     */
    @Nullable public Map<String, ProfileShardResult> getProfileResults() {
        return internalResponse.profile();
    }

    static final class Fields {
        static final String _SCROLL_ID = "_scroll_id";
        static final String TOOK = "took";
        static final String TIMED_OUT = "timed_out";
        static final String TERMINATED_EARLY = "terminated_early";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (scrollId != null) {
            builder.field(Fields._SCROLL_ID, scrollId);
        }
        builder.field(Fields.TOOK, tookInMillis);
        builder.field(Fields.TIMED_OUT, isTimedOut());
        if (isTerminatedEarly() != null) {
            builder.field(Fields.TERMINATED_EARLY, isTerminatedEarly());
        }
        RestActions.buildBroadcastShardsHeader(builder, params, getTotalShards(), getSuccessfulShards(), getFailedShards(), getShardFailures());
        internalResponse.hits.toXContent(builder, params);
        if (internalResponse.aggregations != null) {
            internalResponse.aggregations.toXContent(builder, params);
        }
        if (internalResponse.suggest != null) {
            internalResponse.suggest.toXContent(builder, params);
        }
        if (internalResponse.profileResults != null) {
            internalResponse.profileResults.toXContent(builder, params);
        }
        return builder;
    }

    public static SearchResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        String currentFieldName = null;
        InternalSearchHits hits = null;
        boolean timedOut = false;
        Boolean terminatedEarly = null;
        long tookInMillis = 0;
        int successfulShards = 0;
        int totalShards = 0;
        String scrollId = null;
        List<ShardSearchFailure> failures = new ArrayList<>();
        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields._SCROLL_ID.equals(currentFieldName)) {
                    scrollId = parser.text();
                } else if (Fields.TOOK.equals(currentFieldName)) {
                    tookInMillis = parser.longValue();
                } else if (Fields.TIMED_OUT.equals(currentFieldName)) {
                    timedOut = parser.booleanValue();
                } else if (Fields.TERMINATED_EARLY.equals(currentFieldName)) {
                    terminatedEarly = parser.booleanValue();
                } else {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (InternalSearchHits.Fields.HITS.equals(currentFieldName)) {
                    hits = InternalSearchHits.fromXContent(parser);
                } else if (InternalAggregations.Fields.AGGREGATIONS.equals(currentFieldName)) {
                    // TODO parse aggregation result correctly
                    parser.skipChildren();
                } else if (Suggest.NAME.equals(currentFieldName)) {
                    // TODO parse suggest section
                    parser.skipChildren();
                } else if (SearchProfileShardResults.PROFILE_NAME.equals(currentFieldName)) {
                    // TODO parse "profile" section
                    parser.skipChildren();
                } else if (RestActions._SHARDS_FIELD.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (RestActions.FAILED_FIELD.equals(currentFieldName)) {
                                parser.intValue(); // we don't need it but need to consume it
                            } else if (RestActions.SUCCESSFUL_FIELD.equals(currentFieldName)) {
                                successfulShards = parser.intValue();
                            } else if (RestActions.TOTAL_FIELD.equals(currentFieldName)) {
                                totalShards = parser.intValue();
                            } else {
                                throwUnknownField(currentFieldName, parser.getTokenLocation());
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            if (RestActions.FAILURES_FIELD.equals(currentFieldName)) {
                                while((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    failures.add(ShardSearchFailure.fromXContent(parser));
                                }
                            } else {
                                throwUnknownField(currentFieldName, parser.getTokenLocation());
                            }
                        } else {
                            throwUnknownToken(token, parser.getTokenLocation());
                        }
                    }
                } else {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            }
        }
        return new SearchResponse(new InternalSearchResponse(hits, null, null, null, timedOut, terminatedEarly), scrollId, totalShards,
                successfulShards, tookInMillis, failures.toArray(new ShardSearchFailure[failures.size()]));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        internalResponse = InternalSearchResponse.readInternalSearchResponse(in);
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        int size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = readShardSearchFailure(in);
            }
        }
        scrollId = in.readOptionalString();
        tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        internalResponse.writeTo(out);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);

        out.writeVInt(shardFailures.length);
        for (ShardSearchFailure shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }

        out.writeOptionalString(scrollId);
        out.writeVLong(tookInMillis);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class InternalSearchResponse {

        public static InternalSearchResponse empty() {
            return new InternalSearchResponse(InternalSearchHits.empty(), null, null, null, false, null);
        }

        InternalSearchHits hits;

        private InternalAggregations aggregations;

        private Suggest suggest;

        private SearchProfileShardResults profileResults;

        private boolean timedOut;

        private Boolean terminatedEarly = null;

        private InternalSearchResponse() {
        }

        public InternalSearchResponse(InternalSearchHits hits, InternalAggregations aggregations, Suggest suggest,
                                      SearchProfileShardResults profileResults, boolean timedOut, Boolean terminatedEarly) {
            this.hits = hits;
            this.aggregations = aggregations;
            this.suggest = suggest;
            this.profileResults = profileResults;
            this.timedOut = timedOut;
            this.terminatedEarly = terminatedEarly;
        }

        /**
         * Returns the profile results for this search response (including all shards).
         * An empty map is returned if profiling was not enabled
         *
         * @return Profile results
         */
        public Map<String, ProfileShardResult> profile() {
            if (profileResults == null) {
                return Collections.emptyMap();
            }
            return profileResults.getShardResults();
        }

        static InternalSearchResponse readInternalSearchResponse(StreamInput in) throws IOException {
            InternalSearchResponse response = new InternalSearchResponse();
            response.readFrom(in);
            return response;
        }

        void readFrom(StreamInput in) throws IOException {
            hits = readSearchHits(in);
            if (in.readBoolean()) {
                aggregations = InternalAggregations.readAggregations(in);
            }
            if (in.readBoolean()) {
                suggest = Suggest.readSuggest(in);
            }
            timedOut = in.readBoolean();
            terminatedEarly = in.readOptionalBoolean();
            profileResults = in.readOptionalWriteable(SearchProfileShardResults::new);
        }

        void writeTo(StreamOutput out) throws IOException {
            hits.writeTo(out);
            if (aggregations == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                aggregations.writeTo(out);
            }
            if (suggest == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                suggest.writeTo(out);
            }
            out.writeBoolean(timedOut);
            out.writeOptionalBoolean(terminatedEarly);
            out.writeOptionalWriteable(profileResults);
        }
    }
}
