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
package org.elasticsearch.client.response.indices.flush;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

public class SyncedFlushResponse extends ActionResponse implements ToXContentFragment {

    // Field name declaration
    public static final String SHARDS = "_shards";
    // End field name declaration

    private ShardCounts totalCounts;
    private Map<String, IndexResult> indexResults;

    SyncedFlushResponse(ShardCounts totalCounts, Map<String, IndexResult> indexResults) {
        this.totalCounts = new ShardCounts(totalCounts.total, totalCounts.successful, totalCounts.failed);
        this.indexResults = Collections.unmodifiableMap(indexResults);
    }

    public int totalShards() {
        return totalCounts.total;
    }

    public int successfulShards() {
        return totalCounts.successful;
    }

    public int failedShards() {
        return totalCounts.failed;
    }

    public Map<String, IndexResult> getIndexResults() {
        return indexResults;
    }

    protected ShardCounts getShardCounts() {
        return totalCounts;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SHARDS);
        totalCounts.toXContent(builder, params);
        builder.endObject();
        for (Map.Entry<String, IndexResult> entry: indexResults.entrySet()) {
            String indexName = entry.getKey();
            IndexResult indexResult = entry.getValue();
            builder.startObject(indexName);
            indexResult.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    public static SyncedFlushResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        ShardCounts totalCounts = null;
        Map<String, IndexResult> indexResults = new HashMap<>();
        XContentLocation startLoc = parser.getTokenLocation();
        while (parser.nextToken().equals(Token.FIELD_NAME)) {
            if (parser.currentName().equals(SHARDS)) {
                ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                totalCounts = ShardCounts.fromXContent(parser);
                parser.nextToken();
            } else {
                String indexName = parser.currentName();
                IndexResult indexResult = IndexResult.fromXContent(parser);
                indexResults.put(indexName, indexResult);
            }
        }
        if (totalCounts != null) {
            return new SyncedFlushResponse(totalCounts, indexResults);
        } else {
            throw new ParsingException(
                startLoc,
                "Unable to reconstruct object. Total counts for shards couldn't be parsed."
            );
        }
    }

    private static int getIntValueFromField(XContentParser parser, String fieldName) throws IOException {
        // Search while there are fields
        while (parser.nextToken().equals(Token.FIELD_NAME)) {
            if (parser.currentName().equals(fieldName)) {
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, parser.nextToken(), parser::getTokenLocation);
                return parser.intValue();
            } else {
                // Move to the value for this unknown field
                parser.nextToken();
                // Then skip it
                parser.skipChildren();
            }
        }
        // All fields were searched and the required field not found.
        throw new ParsingException(
            parser.getTokenLocation(),
            "Unable to find field name " + fieldName + " while parsing SyncedFlushResponse"
        );
    }

    private static String getStringValueFromField(XContentParser parser, String fieldName) throws IOException {
        // Search while there are fields
        while (parser.nextToken().equals(Token.FIELD_NAME)) {
            if (parser.currentName().equals(fieldName)) {
                ensureExpectedToken(Token.VALUE_STRING, parser.nextToken(), parser::getTokenLocation);
                return parser.text();
            } else {
                // Move to the value for this unknown field
                parser.nextToken();
                // Then skip it
                parser.skipChildren();
            }
        }
        // All fields were searched and the required field not found.
        throw new ParsingException(
            parser.getTokenLocation(),
            "Unable to find field name " + fieldName + " while parsing SyncedFlushResponse"
        );
    }

    public static final class ShardCounts implements ToXContentFragment {

        // Field name declaration
        public static final String TOTAL = "total";
        public static final String SUCCESSFUL = "successful";
        public static final String FAILED = "failed";
        // End field name declaration

        private int total;
        private int successful;
        private int failed;


        ShardCounts(int total, int successful, int failed) {
            this.total = total;
            this.successful = successful;
            this.failed = failed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TOTAL, total);
            builder.field(SUCCESSFUL, successful);
            builder.field(FAILED, failed);
            return builder;
        }

        public static ShardCounts fromXContent(XContentParser parser) throws IOException {
            int total = getIntValueFromField(parser, TOTAL);
            int successful = getIntValueFromField(parser, SUCCESSFUL);
            int failed = getIntValueFromField(parser, FAILED);
            return new ShardCounts(total, successful, failed);
        }

        public boolean equals(ShardCounts other) {
            if (other != null) {
                return
                    other.total == this.total &&
                    other.successful == this.successful &&
                    other.failed == this.failed;
            } else {
                return false;
            }
        }

    }

    public static final class IndexResult implements ToXContentFragment {

        // Field name declaration
        public static final String FAILURES = "failures";
        // End field name declaration

        private ShardCounts counts;
        private List<ShardFailure> failures;

        public IndexResult(ShardCounts counts, List<ShardFailure> failures) {
            this.counts = new ShardCounts(counts.total, counts.successful, counts.failed);
            this.failures = Collections.unmodifiableList(failures);
        }

        public int totalShards() {
            return counts.total;
        }

        public int successfulShards() {
            return counts.successful;
        }

        public int failedShards() {
            return counts.failed;
        }

        public List<ShardFailure> failures() {
            return failures;
        }

        protected ShardCounts getShardCounts() {
            return counts;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            counts.toXContent(builder, params);
            if (failures.size() > 0) {
                builder.startArray(FAILURES);
                for (ShardFailure failure : failures) {
                    failure.toXContent(builder, params);
                }
                builder.endArray();
            }
            return builder;
        }

        public static IndexResult fromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ShardCounts counts = ShardCounts.fromXContent(parser);
            List<ShardFailure> failures = new ArrayList<>();
            while (parser.nextToken().equals(Token.FIELD_NAME)) {
                if (parser.currentName().equals(FAILURES)) {
                    ensureExpectedToken(Token.START_ARRAY, parser.nextToken(), parser::getTokenLocation);
                    while (parser.nextToken().equals(Token.START_OBJECT)) {
                        failures.add(ShardFailure.fromXContent(parser));
                    }
                    // We should be at the end of the array by now
                    ensureExpectedToken(Token.END_ARRAY, parser.currentToken(), parser::getTokenLocation);
                }
            }
            // We should be at the end of the index object by now
            ensureExpectedToken(Token.END_OBJECT, parser.currentToken(), parser::getTokenLocation);
            return new IndexResult(counts, failures);
        }
    }

    // Only used as a container for XContent
    public static final class ShardFailure implements ToXContentFragment {

        // Field name declaration
        public static String SHARD_ID = "shard";
        public static String FAILURE_REASON = "reason";
        public static String ROUTING = "routing";
        // End field name declaration

        private int shardId;
        private String failureReason;
        private Map<String, Object> routing;

        ShardFailure(int shardId, String failureReason, Map<String, Object> routing) {
            this.shardId = shardId;
            this.failureReason = failureReason;
            this.routing = Collections.unmodifiableMap(routing);
        }

        public int getShardId() {
            return shardId;
        }

        public String getFailureReason() {
            return failureReason;
        }

        public Map<String, Object> getRouting() {
            return routing;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SHARD_ID, shardId);
            builder.field(FAILURE_REASON, failureReason);
            if (routing.size() > 0) {
                builder.field(ROUTING, routing);
            }
            builder.endObject();
            return builder;
        }

        public static ShardFailure fromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            int shardId = getIntValueFromField(parser, SHARD_ID);
            String failureReason = getStringValueFromField(parser, FAILURE_REASON);
            Map<String, Object> routing = new HashMap<>();
            while (parser.nextToken().equals(Token.FIELD_NAME)) {
                if (parser.currentName().equals(ROUTING)) {
                    ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    routing = parser.map();
                } else {
                    parser.nextToken();
                    parser.skipChildren();
                }
            }
            // we should be at the end of the object by now
            ensureExpectedToken(Token.END_OBJECT, parser.currentToken(), parser::getTokenLocation);
            return new ShardFailure(shardId, failureReason, routing);
        }
    }
}
