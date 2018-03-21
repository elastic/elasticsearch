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
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
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
    ShardCounts shardCounts;
    Map<String, ShardCounts> shardCountsPerIndex;

    SyncedFlushResponse() {

    }

    public SyncedFlushResponse(Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex) {
        // shardsResultPerIndex is never modified after it is passed to this
        // constructor so this is safe even though shardsResultPerIndex is a
        // ConcurrentHashMap
        this.shardsResultPerIndex = unmodifiableMap(shardsResultPerIndex);
        this.shardCounts = calculateShardCounts(Iterables.flatten(shardsResultPerIndex.values()));
        this.shardCountsPerIndex = new HashMap<>();
        for (Map.Entry<String, List<ShardsSyncedFlushResult>> entry: shardsResultPerIndex.entrySet()) {
            this.shardCountsPerIndex.put(entry.getKey(), calculateShardCounts(entry.getValue()));
        }
    }

    public SyncedFlushResponse(ShardCounts shardCounts,
        Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex,
        Map<String, ShardCounts> shardCountsPerIndex) {
        // shardsResultPerIndex is never modified after it is passed to this
        // constructor so this is safe even though shardsResultPerIndex is a
        // ConcurrentHashMap
        this.shardsResultPerIndex = unmodifiableMap(shardsResultPerIndex);
        this.shardCounts = shardCounts;
        this.shardCountsPerIndex = unmodifiableMap(shardCountsPerIndex);
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

    /**
     * Get the ShardCount for a particular index name.
     * @param index name of the index to be searched
     * @return ShardCounts or {@code null} if index is not present
     */
    public ShardCounts getShardCountsForIndex(String index) {
        return shardCountsPerIndex.get(index);
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
                        builder.field(Fields.TOTAL_COPIES, shardResults.totalShards());
                        builder.field(Fields.SUCCESSFUL_COPIES, shardResults.successfulShards());
                        builder.field(Fields.SHARD, shardResults.shardId().id());
                        builder.field(Fields.REASON, shardResults.failureReason());
                        builder.endObject();
                        continue;
                    }
                    Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> failedShards = shardResults.failedShards();
                    for (Map.Entry<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardEntry : failedShards.entrySet()) {
                        builder.startObject();
                        builder.field(Fields.TOTAL_COPIES, shardResults.totalShards());
                        builder.field(Fields.SUCCESSFUL_COPIES, shardResults.successfulShards());
                        builder.field(Fields.SHARD, shardResults.shardId().id());
                        builder.field(Fields.REASON, shardEntry.getValue().failureReason());
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
        Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex = new HashMap<>();
        Map<String, ShardCounts> shardsCountsPerIndex = new HashMap<>();
        // If it is an object we try to parse it for Fields._SHARD or for an index entry
        for (Token curToken = parser.currentToken(); curToken != Token.END_OBJECT; curToken = parser.nextToken()) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            String fieldName = parser.currentName();
            curToken = parser.nextToken();
            Integer totalShards = null;
            Integer successfulShards = null;
            Integer failedShards = null;
            List<ShardsSyncedFlushResult> listShardsSyncedFlushResult = new ArrayList<>();
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
                                    Map<ShardId, ShardsSyncedFlushResult> shardsSyncedFlushResults = new HashMap<>();
                                    Map<ShardId, Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse>>
                                        failedSharedResponses = new HashMap<>();
                                    for (curToken = parser.nextToken(); curToken != Token.END_ARRAY; curToken = parser.nextToken()) {
                                        ensureExpectedToken(Token.START_OBJECT, curToken, parser::getTokenLocation);
                                        ShardRouting routing = null;
                                        String failureReason = null;
                                        ShardId shardId = null;
                                        Integer totalShardCopies = null;
                                        Integer successfulShardCopies = null;
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
                                                case Fields.ROUTING:
                                                    routing = ShardRouting.fromXContent(parser);
                                                    break;
                                                case Fields.TOTAL_COPIES:
                                                    ensureExpectedToken(Token.VALUE_NUMBER, curToken, parser::getTokenLocation);
                                                    totalShardCopies = parser.intValue();
                                                    break;
                                                case Fields.SUCCESSFUL_COPIES:
                                                    ensureExpectedToken(Token.VALUE_NUMBER, curToken, parser::getTokenLocation);
                                                    successfulShardCopies = parser.intValue();
                                                    break;
                                                default:
                                                    // If something else skip it
                                                    parser.skipChildren();
                                                    break;
                                            }
                                        }
                                        if (failureReason != null &&
                                            shardId != null &&
                                            totalShardCopies != null &&
                                            successfulShardCopies != null) {
                                            // This is ugly but there is only one ShardsSyncedFlushResult for each shardId
                                            // so this will work.
                                            shardsSyncedFlushResults.putIfAbsent (
                                                shardId,
                                                new ShardsSyncedFlushResult(
                                                    shardId,
                                                    totalShardCopies,
                                                    successfulShardCopies,
                                                    routing == null ? failureReason : null
                                                )
                                            );
                                            if (routing != null) {
                                                if (failedSharedResponses.containsKey(shardId)) {
                                                    failedSharedResponses.get(shardId).put(
                                                        routing, new SyncedFlushService.ShardSyncedFlushResponse(failureReason)
                                                    );
                                                } else {
                                                    Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> m =
                                                        new HashMap<>();
                                                    m.put(
                                                        routing,
                                                        new SyncedFlushService.ShardSyncedFlushResponse(failureReason)
                                                    );
                                                    failedSharedResponses.put(shardId, m);
                                                }
                                            }
                                        } else {
                                            throw new ParsingException(startLocation, "Unable to construct ShardsSyncedFlushResult");
                                        }
                                    }
                                    for (Map.Entry<ShardId, ShardsSyncedFlushResult> entry: shardsSyncedFlushResults.entrySet()) {
                                        ShardsSyncedFlushResult result;
                                        if (failedSharedResponses.containsKey(entry.getKey())) {
                                            result = new ShardsSyncedFlushResult(
                                                entry.getValue().shardId(),
                                                null, // syncid is null since this is a failure response
                                                entry.getValue().totalShards(),
                                                entry.getValue().successfulShards(),
                                                failedSharedResponses.get(entry.getKey())
                                            );
                                        } else {
                                            result = entry.getValue();
                                        }
                                        listShardsSyncedFlushResult.add(result);
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
                        shardsCountsPerIndex.put(fieldName, shardCount);
                        shardsResultPerIndex.put(fieldName, listShardsSyncedFlushResult);
                    }
                }
            } else { // Else leave this tree alone
                parser.skipChildren();
            }
        }
        return new SyncedFlushResponse(
            totalShardCounts, shardsResultPerIndex, shardsCountsPerIndex
        );
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
