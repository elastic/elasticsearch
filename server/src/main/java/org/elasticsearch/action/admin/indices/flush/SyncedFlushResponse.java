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
package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * The result of performing a sync flush operation on all shards of multiple indices
 */
public class SyncedFlushResponse extends ActionResponse implements ToXContentFragment {

    Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex;
    Map<String, ShardCounts> shardCountsPerIndex;
    ShardCounts shardCounts;

    SyncedFlushResponse() {

    }

    public SyncedFlushResponse(Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex) {
        // shardsResultPerIndex is never modified after it is passed to this
        // constructor so this is safe even though shardsResultPerIndex is a
        // ConcurrentHashMap
        this.shardsResultPerIndex = unmodifiableMap(shardsResultPerIndex);
        this.shardCounts = calculateShardCounts(Iterables.flatten(shardsResultPerIndex.values()));
        Map<String, ShardCounts> shardsCountsPerIndex = new HashMap<>();
        for (Map.Entry<String, List<ShardsSyncedFlushResult>> entry: shardsResultPerIndex.entrySet()) {
            shardsCountsPerIndex.put(entry.getKey(), calculateShardCounts(entry.getValue()));
        }
        this.shardCountsPerIndex = unmodifiableMap(shardsCountsPerIndex);
    }

    public SyncedFlushResponse(ShardCounts shardCounts, Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex,
        Map<String, ShardCounts> shardCountsPerIndex) {
        // shardsResultPerIndex is never modified after it is passed to this
        // constructor so this is safe even though shardsResultPerIndex is a
        // ConcurrentHashMap
        this.shardsResultPerIndex = unmodifiableMap(shardsResultPerIndex);
        this.shardCounts = shardCounts;
        this.shardCountsPerIndex = shardCountsPerIndex;
    }

    /**
     * total number shards, including replicas, both assigned and unassigned
     */
    public int totalShards() {
        return shardCounts.total;
    }

    /**
     * total number of shards for which the operation failed
     */
    public int failedShards() {
        return shardCounts.failed;
    }

    /**
     * total number of shards which were successfully sync-flushed
     */
    public int successfulShards() {
        return shardCounts.successful;
    }

    public RestStatus restStatus() {
        return failedShards() == 0 ? RestStatus.OK : RestStatus.CONFLICT;
    }

    public Map<String, List<ShardsSyncedFlushResult>> getShardsResultPerIndex() {
        return shardsResultPerIndex;
    }

    public Map<String, ShardCounts> getShardCountsPerIndex() {
        return shardCountsPerIndex;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields._SHARDS);
        shardCounts.toXContent(builder, params);
        builder.endObject();
        for (Map.Entry<String, List<ShardsSyncedFlushResult>> indexEntry : shardsResultPerIndex.entrySet()) {
            List<ShardsSyncedFlushResult> indexResult = indexEntry.getValue();
            builder.startObject(indexEntry.getKey());
            ShardCounts indexShardCounts = calculateShardCounts(indexResult);
            indexShardCounts.toXContent(builder, params);
            if (indexShardCounts.failed > 0) {
                builder.startArray(Fields.FAILURES);
                for (ShardsSyncedFlushResult shardResults : indexResult) {
                    if (shardResults.failed()) {
                        builder.startObject();
                        builder.field(Fields.SHARD, shardResults.shardId().id());
                        builder.field(Fields.REASON, shardResults.failureReason());
                        builder.field(Fields.TOTAL_COPIES, shardResults.totalShards());
                        builder.field(Fields.SUCCESSFUL_COPIES, shardResults.successfulShards());
                        builder.endObject();
                        continue;
                    }
                    Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> failedShards = shardResults.failedShards();
                    for (Map.Entry<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardEntry : failedShards.entrySet()) {
                        builder.startObject();
                        builder.field(Fields.SHARD, shardResults.shardId().id());
                        builder.field(Fields.REASON, shardEntry.getValue().failureReason());
                        builder.field(Fields.TOTAL_COPIES, shardResults.totalShards());
                        builder.field(Fields.SUCCESSFUL_COPIES, shardResults.successfulShards());
                        builder.field(Fields.ROUTING, shardEntry.getKey());
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();
        }
        return builder;
    }

    public static SyncedFlushResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        parser.nextToken();
        return innerFromXContent(parser);
    }

    private static SyncedFlushResponse innerFromXContent(XContentParser parser) throws IOException {
        ShardCounts totalShardCounts = null;
        Map<String, ShardCounts> shardsCountsPerIndex = new HashMap<>();
        Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex = new HashMap<>();
        // If it is an object we try to parse it for Fields._SHARD or for an index entry
        for (Token curToken = parser.currentToken(); curToken != Token.END_OBJECT; curToken = parser.nextToken()) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            String fieldName = parser.currentName();
            curToken = parser.nextToken();
            Integer totalShards = null;
            Integer successfulShards = null;
            Integer failedShards = null;
            Map<ShardId, List<FailureContainer>> failures = new HashMap<>();
            if (curToken == Token.START_OBJECT) { // Start parsing for _shard or for index
                for (curToken = parser.nextToken(); curToken != Token.END_OBJECT; curToken = parser.nextToken()) {
                    if (curToken == Token.FIELD_NAME) {
                        String level2FieldName = parser.currentName();
                        curToken = parser.nextToken();
                        switch (level2FieldName) {
                            case Fields.TOTAL:
                                ensureExpectedToken(Token.VALUE_NUMBER, curToken, parser::getTokenLocation);
                                totalShards = parser.intValue();
                                break;
                            case Fields.SUCCESSFUL:
                                ensureExpectedToken(Token.VALUE_NUMBER, curToken, parser::getTokenLocation);
                                successfulShards = parser.intValue();
                                break;
                            case Fields.FAILED:
                                ensureExpectedToken(Token.VALUE_NUMBER, curToken, parser::getTokenLocation);
                                failedShards = parser.intValue();
                                break;
                            case Fields.FAILURES:
                                if (!fieldName.equals(Fields._SHARDS)) {
                                    ensureExpectedToken(Token.START_ARRAY, curToken, parser::getTokenLocation);
                                    for (curToken = parser.nextToken(); curToken != Token.END_ARRAY; curToken = parser.nextToken()) {
                                        ensureExpectedToken(Token.START_OBJECT, curToken, parser::getTokenLocation);
                                        ShardRouting routing = null;
                                        String failureReason = null;
                                        Integer totalCopies = null;
                                        Integer successfulCopies = null;
                                        ShardId shardId = null;
                                        XContentLocation startLocation = parser.getTokenLocation();
                                        for (curToken = parser.nextToken(); curToken != Token.END_OBJECT; curToken = parser.nextToken()) {
                                            ensureExpectedToken(Token.FIELD_NAME, curToken, parser::getTokenLocation);
                                            String level3FieldName = parser.currentName();
                                            curToken = parser.nextToken();
                                            switch (level3FieldName) {
                                                case Fields.SHARD:
                                                    ensureExpectedToken(Token.VALUE_NUMBER, curToken, parser::getTokenLocation);
                                                    shardId = new ShardId(
                                                        fieldName,
                                                        IndexMetaData.INDEX_UUID_NA_VALUE,
                                                        parser.intValue()
                                                    );
                                                    break;
                                                case Fields.REASON:
                                                    ensureExpectedToken(Token.VALUE_STRING, curToken, parser::getTokenLocation);
                                                    failureReason = parser.text();
                                                    break;
                                                case Fields.TOTAL_COPIES:
                                                    ensureExpectedToken(Token.VALUE_NUMBER, curToken, parser::getTokenLocation);
                                                    totalCopies = parser.intValue();
                                                    break;
                                                case Fields.SUCCESSFUL_COPIES:
                                                    ensureExpectedToken(Token.VALUE_NUMBER, curToken, parser::getTokenLocation);
                                                    successfulCopies = parser.intValue();
                                                    break;
                                                case Fields.ROUTING:
                                                    routing = ShardRouting.fromXContent(parser);
                                                    break;
                                                default:
                                                    // If something else skip it
                                                    parser.skipChildren();
                                                    break;
                                            }
                                        }
                                        if (failureReason != null &&
                                            shardId != null &&
                                            totalCopies != null &&
                                            successfulCopies != null) {
                                            // This is ugly but there is only one ShardsSyncedFlushResult for each shardId
                                            // so this will work.
                                            if (!failures.containsKey(shardId)) {
                                                failures.put(shardId, new ArrayList<>());
                                            }
                                            failures.get(shardId).add(
                                                new FailureContainer(shardId, failureReason, totalCopies, successfulCopies, routing)
                                            );
                                        } else {
                                            throw new ParsingException(startLocation, "Unable to construct ShardsSyncedFlushResult");
                                        }
                                    }
                                } else {
                                    parser.skipChildren();
                                }
                                break;
                            default:
                                parser.skipChildren();
                                break;
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
                if (totalShards != null &&
                    successfulShards != null &&
                    failedShards != null) {
                    ShardCounts shardCount = new ShardCounts(totalShards, successfulShards, failedShards);
                    if (fieldName.equals(Fields._SHARDS)) {
                        totalShardCounts = shardCount;
                    } else {
                        List<ShardsSyncedFlushResult> results = new ArrayList<>();
                        // All failures in this list belong to the same index
                        for (Map.Entry<ShardId, List<FailureContainer>> entry: failures.entrySet()) {
                            Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardResponses = new HashMap<>();
                            for (FailureContainer container: entry.getValue()) {
                                if (container.shardRouting != null) {
                                    shardResponses.put(container.shardRouting,
                                        new SyncedFlushService.ShardSyncedFlushResponse(container.failureReason)
                                    );
                                }
                            }
                            // Size of entry.getValue() will at least be one
                            FailureContainer container = entry.getValue().get(0);
                            if (!shardResponses.isEmpty()) {
                                results.add(
                                    new ShardsSyncedFlushResult(container.shardId, null, container.totalCopies,
                                        container.successfulCopies, shardResponses)
                                );
                            } else {
                                results.add(
                                    new ShardsSyncedFlushResult(container.shardId, container.totalCopies,
                                        container.successfulCopies, container.failureReason)
                                );
                            }
                        }
                        shardsCountsPerIndex.put(fieldName, shardCount);
                        shardsResultPerIndex.put(fieldName, results);
                    }
                }
            } else { // Else leave this tree alone
                parser.skipChildren();
            }
        }
        return new SyncedFlushResponse(totalShardCounts, shardsResultPerIndex, shardsCountsPerIndex);
    }


    static ShardCounts calculateShardCounts(Iterable<ShardsSyncedFlushResult> results) {
        int total = 0, successful = 0, failed = 0;
        for (ShardsSyncedFlushResult result : results) {
            total += result.totalShards();
            successful += result.successfulShards();
            if (result.failed()) {
                // treat all shard copies as failed
                failed += result.totalShards();
            } else {
                // some shards may have failed during the sync phase
                failed += result.failedShards().size();
            }
        }
        return new ShardCounts(total, successful, failed);
    }

    // Only used as a container for parsing XContent
    static final class FailureContainer {
        ShardId shardId;
        String failureReason;
        ShardRouting shardRouting;
        int totalCopies;
        int successfulCopies;
        FailureContainer(ShardId shardId, String failureReason, int totalCopies, int successfulCopies,
            @Nullable ShardRouting shardRouting) {
            this.shardId = shardId;
            this.failureReason = failureReason;
            this.shardRouting = shardRouting;
            this.totalCopies = totalCopies;
            this.successfulCopies = successfulCopies;
        }
    }

    static final class ShardCounts implements ToXContentFragment, Streamable {

        public int total;
        public int successful;
        public int failed;

        ShardCounts(int total, int successful, int failed) {
            this.total = total;
            this.successful = successful;
            this.failed = failed;
        }

        ShardCounts() {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.TOTAL, total);
            builder.field(Fields.SUCCESSFUL, successful);
            builder.field(Fields.FAILED, failed);
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            total = in.readInt();
            successful = in.readInt();
            failed = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(total);
            out.writeInt(successful);
            out.writeInt(failed);
        }
    }

    static final class Fields {
        static final String _SHARDS = "_shards";
        static final String TOTAL = "total";
        static final String SUCCESSFUL = "successful";
        static final String FAILED = "failed";
        static final String FAILURES = "failures";
        static final String TOTAL_COPIES = "total_copies";
        static final String FAILED_COPIES = "failed_copies";
        static final String SUCCESSFUL_COPIES = "successful_copies";
        static final String SHARD = "shard";
        static final String ROUTING = "routing";
        static final String REASON = "reason";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardCounts = new ShardCounts();
        shardCounts.readFrom(in);
        Map<String, List<ShardsSyncedFlushResult>> tmpShardsResultPerIndex = new HashMap<>();
        int numShardsResults = in.readInt();
        for (int i =0 ; i< numShardsResults; i++) {
            String index = in.readString();
            List<ShardsSyncedFlushResult> shardsSyncedFlushResults = new ArrayList<>();
            int numShards = in.readInt();
            for (int j =0; j< numShards; j++) {
                shardsSyncedFlushResults.add(ShardsSyncedFlushResult.readShardsSyncedFlushResult(in));
            }
            tmpShardsResultPerIndex.put(index, shardsSyncedFlushResults);
        }
        shardsResultPerIndex = Collections.unmodifiableMap(tmpShardsResultPerIndex);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardCounts.writeTo(out);
        out.writeInt(shardsResultPerIndex.size());
        for (Map.Entry<String, List<ShardsSyncedFlushResult>> entry : shardsResultPerIndex.entrySet()) {
            out.writeString(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (ShardsSyncedFlushResult shardsSyncedFlushResult : entry.getValue()) {
                shardsSyncedFlushResult.writeTo(out);
            }
        }
    }
}
