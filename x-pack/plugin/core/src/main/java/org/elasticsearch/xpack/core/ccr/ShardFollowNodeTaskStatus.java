/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ShardFollowNodeTaskStatus implements Task.Status {

    public static final String STATUS_PARSER_NAME = "shard-follow-node-task-status";

    private static final ParseField LEADER_INDEX = new ParseField("leader_index");
    private static final ParseField SHARD_ID = new ParseField("shard_id");
    private static final ParseField LEADER_GLOBAL_CHECKPOINT_FIELD = new ParseField("leader_global_checkpoint");
    private static final ParseField LEADER_MAX_SEQ_NO_FIELD = new ParseField("leader_max_seq_no");
    private static final ParseField FOLLOWER_GLOBAL_CHECKPOINT_FIELD = new ParseField("follower_global_checkpoint");
    private static final ParseField FOLLOWER_MAX_SEQ_NO_FIELD = new ParseField("follower_max_seq_no");
    private static final ParseField LAST_REQUESTED_SEQ_NO_FIELD = new ParseField("last_requested_seq_no");
    private static final ParseField NUMBER_OF_CONCURRENT_READS_FIELD = new ParseField("number_of_concurrent_reads");
    private static final ParseField NUMBER_OF_CONCURRENT_WRITES_FIELD = new ParseField("number_of_concurrent_writes");
    private static final ParseField NUMBER_OF_QUEUED_WRITES_FIELD = new ParseField("number_of_queued_writes");
    private static final ParseField MAPPING_VERSION_FIELD = new ParseField("mapping_version");
    private static final ParseField TOTAL_FETCH_TIME_MILLIS_FIELD = new ParseField("total_fetch_time_millis");
    private static final ParseField NUMBER_OF_SUCCESSFUL_FETCHES_FIELD = new ParseField("number_of_successful_fetches");
    private static final ParseField NUMBER_OF_FAILED_FETCHES_FIELD = new ParseField("number_of_failed_fetches");
    private static final ParseField OPERATIONS_RECEIVED_FIELD = new ParseField("operations_received");
    private static final ParseField TOTAL_TRANSFERRED_BYTES = new ParseField("total_transferred_bytes");
    private static final ParseField TOTAL_INDEX_TIME_MILLIS_FIELD = new ParseField("total_index_time_millis");
    private static final ParseField NUMBER_OF_SUCCESSFUL_BULK_OPERATIONS_FIELD = new ParseField("number_of_successful_bulk_operations");
    private static final ParseField NUMBER_OF_FAILED_BULK_OPERATIONS_FIELD = new ParseField("number_of_failed_bulk_operations");
    private static final ParseField NUMBER_OF_OPERATIONS_INDEXED_FIELD = new ParseField("number_of_operations_indexed");
    private static final ParseField FETCH_EXCEPTIONS = new ParseField("fetch_exceptions");
    private static final ParseField TIME_SINCE_LAST_FETCH_MILLIS_FIELD = new ParseField("time_since_last_fetch_millis");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ShardFollowNodeTaskStatus, Void> STATUS_PARSER =
            new ConstructingObjectParser<>(
                    STATUS_PARSER_NAME,
                    args -> new ShardFollowNodeTaskStatus(
                            (String) args[0],
                            (int) args[1],
                            (long) args[2],
                            (long) args[3],
                            (long) args[4],
                            (long) args[5],
                            (long) args[6],
                            (int) args[7],
                            (int) args[8],
                            (int) args[9],
                            (long) args[10],
                            (long) args[11],
                            (long) args[12],
                            (long) args[13],
                            (long) args[14],
                            (long) args[15],
                            (long) args[16],
                            (long) args[17],
                            (long) args[18],
                            (long) args[19],
                            new TreeMap<>(
                                    ((List<Map.Entry<Long, ElasticsearchException>>) args[20])
                                            .stream()
                                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
                            (long) args[21]));

    public static final String FETCH_EXCEPTIONS_ENTRY_PARSER_NAME = "shard-follow-node-task-status-fetch-exceptions-entry";

    static final ConstructingObjectParser<Map.Entry<Long, ElasticsearchException>, Void> FETCH_EXCEPTIONS_ENTRY_PARSER =
            new ConstructingObjectParser<>(
                    FETCH_EXCEPTIONS_ENTRY_PARSER_NAME,
                    args -> new AbstractMap.SimpleEntry<>((long) args[0], (ElasticsearchException) args[1]));

    static {
        STATUS_PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_INDEX);
        STATUS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), SHARD_ID);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), LEADER_GLOBAL_CHECKPOINT_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), LEADER_MAX_SEQ_NO_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_GLOBAL_CHECKPOINT_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_MAX_SEQ_NO_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_REQUESTED_SEQ_NO_FIELD);
        STATUS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_CONCURRENT_READS_FIELD);
        STATUS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_CONCURRENT_WRITES_FIELD);
        STATUS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_QUEUED_WRITES_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAPPING_VERSION_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_FETCH_TIME_MILLIS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_SUCCESSFUL_FETCHES_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_FAILED_FETCHES_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), OPERATIONS_RECEIVED_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_TRANSFERRED_BYTES);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_INDEX_TIME_MILLIS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_SUCCESSFUL_BULK_OPERATIONS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_FAILED_BULK_OPERATIONS_FIELD);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_OPERATIONS_INDEXED_FIELD);
        STATUS_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), FETCH_EXCEPTIONS_ENTRY_PARSER, FETCH_EXCEPTIONS);
        STATUS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIME_SINCE_LAST_FETCH_MILLIS_FIELD);
    }

    static final ParseField FETCH_EXCEPTIONS_ENTRY_FROM_SEQ_NO = new ParseField("from_seq_no");
    static final ParseField FETCH_EXCEPTIONS_ENTRY_EXCEPTION = new ParseField("exception");

    static {
        FETCH_EXCEPTIONS_ENTRY_PARSER.declareLong(ConstructingObjectParser.constructorArg(), FETCH_EXCEPTIONS_ENTRY_FROM_SEQ_NO);
        FETCH_EXCEPTIONS_ENTRY_PARSER.declareObject(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> ElasticsearchException.fromXContent(p),
                FETCH_EXCEPTIONS_ENTRY_EXCEPTION);
    }

    private final String leaderIndex;

    public String leaderIndex() {
        return leaderIndex;
    }

    private final int shardId;

    public int getShardId() {
        return shardId;
    }

    private final long leaderGlobalCheckpoint;

    public long leaderGlobalCheckpoint() {
        return leaderGlobalCheckpoint;
    }

    private final long leaderMaxSeqNo;

    public long leaderMaxSeqNo() {
        return leaderMaxSeqNo;
    }

    private final long followerGlobalCheckpoint;

    public long followerGlobalCheckpoint() {
        return followerGlobalCheckpoint;
    }

    private final long followerMaxSeqNo;

    public long followerMaxSeqNo() {
        return followerMaxSeqNo;
    }

    private final long lastRequestedSeqNo;

    public long lastRequestedSeqNo() {
        return lastRequestedSeqNo;
    }

    private final int numberOfConcurrentReads;

    public int numberOfConcurrentReads() {
        return numberOfConcurrentReads;
    }

    private final int numberOfConcurrentWrites;

    public int numberOfConcurrentWrites() {
        return numberOfConcurrentWrites;
    }

    private final int numberOfQueuedWrites;

    public int numberOfQueuedWrites() {
        return numberOfQueuedWrites;
    }

    private final long mappingVersion;

    public long mappingVersion() {
        return mappingVersion;
    }

    private final long totalFetchTimeMillis;

    public long totalFetchTimeMillis() {
        return totalFetchTimeMillis;
    }

    private final long numberOfSuccessfulFetches;

    public long numberOfSuccessfulFetches() {
        return numberOfSuccessfulFetches;
    }

    private final long numberOfFailedFetches;

    public long numberOfFailedFetches() {
        return numberOfFailedFetches;
    }

    private final long operationsReceived;

    public long operationsReceived() {
        return operationsReceived;
    }

    private final long totalTransferredBytes;

    public long totalTransferredBytes() {
        return totalTransferredBytes;
    }

    private final long totalIndexTimeMillis;

    public long totalIndexTimeMillis() {
        return totalIndexTimeMillis;
    }

    private final long numberOfSuccessfulBulkOperations;

    public long numberOfSuccessfulBulkOperations() {
        return numberOfSuccessfulBulkOperations;
    }

    private final long numberOfFailedBulkOperations;

    public long numberOfFailedBulkOperations() {
        return numberOfFailedBulkOperations;
    }

    private final long numberOfOperationsIndexed;

    public long numberOfOperationsIndexed() {
        return numberOfOperationsIndexed;
    }

    private final NavigableMap<Long, ElasticsearchException> fetchExceptions;

    public NavigableMap<Long, ElasticsearchException> fetchExceptions() {
        return fetchExceptions;
    }

    private final long timeSinceLastFetchMillis;

    public long timeSinceLastFetchMillis() {
        return timeSinceLastFetchMillis;
    }

    public ShardFollowNodeTaskStatus(
            final String leaderIndex,
            final int shardId,
            final long leaderGlobalCheckpoint,
            final long leaderMaxSeqNo,
            final long followerGlobalCheckpoint,
            final long followerMaxSeqNo,
            final long lastRequestedSeqNo,
            final int numberOfConcurrentReads,
            final int numberOfConcurrentWrites,
            final int numberOfQueuedWrites,
            final long mappingVersion,
            final long totalFetchTimeMillis,
            final long numberOfSuccessfulFetches,
            final long numberOfFailedFetches,
            final long operationsReceived,
            final long totalTransferredBytes,
            final long totalIndexTimeMillis,
            final long numberOfSuccessfulBulkOperations,
            final long numberOfFailedBulkOperations,
            final long numberOfOperationsIndexed,
            final NavigableMap<Long, ElasticsearchException> fetchExceptions,
            final long timeSinceLastFetchMillis) {
        this.leaderIndex = leaderIndex;
        this.shardId = shardId;
        this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
        this.leaderMaxSeqNo = leaderMaxSeqNo;
        this.followerGlobalCheckpoint = followerGlobalCheckpoint;
        this.followerMaxSeqNo = followerMaxSeqNo;
        this.lastRequestedSeqNo = lastRequestedSeqNo;
        this.numberOfConcurrentReads = numberOfConcurrentReads;
        this.numberOfConcurrentWrites = numberOfConcurrentWrites;
        this.numberOfQueuedWrites = numberOfQueuedWrites;
        this.mappingVersion = mappingVersion;
        this.totalFetchTimeMillis = totalFetchTimeMillis;
        this.numberOfSuccessfulFetches = numberOfSuccessfulFetches;
        this.numberOfFailedFetches = numberOfFailedFetches;
        this.operationsReceived = operationsReceived;
        this.totalTransferredBytes = totalTransferredBytes;
        this.totalIndexTimeMillis = totalIndexTimeMillis;
        this.numberOfSuccessfulBulkOperations = numberOfSuccessfulBulkOperations;
        this.numberOfFailedBulkOperations = numberOfFailedBulkOperations;
        this.numberOfOperationsIndexed = numberOfOperationsIndexed;
        this.fetchExceptions = Objects.requireNonNull(fetchExceptions);
        this.timeSinceLastFetchMillis = timeSinceLastFetchMillis;
    }

    public ShardFollowNodeTaskStatus(final StreamInput in) throws IOException {
        this.leaderIndex = in.readString();
        this.shardId = in.readVInt();
        this.leaderGlobalCheckpoint = in.readZLong();
        this.leaderMaxSeqNo = in.readZLong();
        this.followerGlobalCheckpoint = in.readZLong();
        this.followerMaxSeqNo = in.readZLong();
        this.lastRequestedSeqNo = in.readZLong();
        this.numberOfConcurrentReads = in.readVInt();
        this.numberOfConcurrentWrites = in.readVInt();
        this.numberOfQueuedWrites = in.readVInt();
        this.mappingVersion = in.readVLong();
        this.totalFetchTimeMillis = in.readVLong();
        this.numberOfSuccessfulFetches = in.readVLong();
        this.numberOfFailedFetches = in.readVLong();
        this.operationsReceived = in.readVLong();
        this.totalTransferredBytes = in.readVLong();
        this.totalIndexTimeMillis = in.readVLong();
        this.numberOfSuccessfulBulkOperations = in.readVLong();
        this.numberOfFailedBulkOperations = in.readVLong();
        this.numberOfOperationsIndexed = in.readVLong();
        this.fetchExceptions = new TreeMap<>(in.readMap(StreamInput::readVLong, StreamInput::readException));
        this.timeSinceLastFetchMillis = in.readZLong();
    }

    @Override
    public String getWriteableName() {
        return STATUS_PARSER_NAME;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(leaderIndex);
        out.writeVInt(shardId);
        out.writeZLong(leaderGlobalCheckpoint);
        out.writeZLong(leaderMaxSeqNo);
        out.writeZLong(followerGlobalCheckpoint);
        out.writeZLong(followerMaxSeqNo);
        out.writeZLong(lastRequestedSeqNo);
        out.writeVInt(numberOfConcurrentReads);
        out.writeVInt(numberOfConcurrentWrites);
        out.writeVInt(numberOfQueuedWrites);
        out.writeVLong(mappingVersion);
        out.writeVLong(totalFetchTimeMillis);
        out.writeVLong(numberOfSuccessfulFetches);
        out.writeVLong(numberOfFailedFetches);
        out.writeVLong(operationsReceived);
        out.writeVLong(totalTransferredBytes);
        out.writeVLong(totalIndexTimeMillis);
        out.writeVLong(numberOfSuccessfulBulkOperations);
        out.writeVLong(numberOfFailedBulkOperations);
        out.writeVLong(numberOfOperationsIndexed);
        out.writeMap(fetchExceptions, StreamOutput::writeVLong, StreamOutput::writeException);
        out.writeZLong(timeSinceLastFetchMillis);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(LEADER_INDEX.getPreferredName(), leaderIndex);
            builder.field(SHARD_ID.getPreferredName(), shardId);
            builder.field(LEADER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), leaderGlobalCheckpoint);
            builder.field(LEADER_MAX_SEQ_NO_FIELD.getPreferredName(), leaderMaxSeqNo);
            builder.field(FOLLOWER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), followerGlobalCheckpoint);
            builder.field(FOLLOWER_MAX_SEQ_NO_FIELD.getPreferredName(), followerMaxSeqNo);
            builder.field(LAST_REQUESTED_SEQ_NO_FIELD.getPreferredName(), lastRequestedSeqNo);
            builder.field(NUMBER_OF_CONCURRENT_READS_FIELD.getPreferredName(), numberOfConcurrentReads);
            builder.field(NUMBER_OF_CONCURRENT_WRITES_FIELD.getPreferredName(), numberOfConcurrentWrites);
            builder.field(NUMBER_OF_QUEUED_WRITES_FIELD.getPreferredName(), numberOfQueuedWrites);
            builder.field(MAPPING_VERSION_FIELD.getPreferredName(), mappingVersion);
            builder.humanReadableField(
                    TOTAL_FETCH_TIME_MILLIS_FIELD.getPreferredName(),
                    "total_fetch_time",
                    new TimeValue(totalFetchTimeMillis, TimeUnit.MILLISECONDS));
            builder.field(NUMBER_OF_SUCCESSFUL_FETCHES_FIELD.getPreferredName(), numberOfSuccessfulFetches);
            builder.field(NUMBER_OF_FAILED_FETCHES_FIELD.getPreferredName(), numberOfFailedFetches);
            builder.field(OPERATIONS_RECEIVED_FIELD.getPreferredName(), operationsReceived);
            builder.humanReadableField(
                    TOTAL_TRANSFERRED_BYTES.getPreferredName(),
                    "total_transferred",
                    new ByteSizeValue(totalTransferredBytes, ByteSizeUnit.BYTES));
            builder.humanReadableField(
                    TOTAL_INDEX_TIME_MILLIS_FIELD.getPreferredName(),
                    "total_index_time",
                    new TimeValue(totalIndexTimeMillis, TimeUnit.MILLISECONDS));
            builder.field(NUMBER_OF_SUCCESSFUL_BULK_OPERATIONS_FIELD.getPreferredName(), numberOfSuccessfulBulkOperations);
            builder.field(NUMBER_OF_FAILED_BULK_OPERATIONS_FIELD.getPreferredName(), numberOfFailedBulkOperations);
            builder.field(NUMBER_OF_OPERATIONS_INDEXED_FIELD.getPreferredName(), numberOfOperationsIndexed);
            builder.startArray(FETCH_EXCEPTIONS.getPreferredName());
            {
                for (final Map.Entry<Long, ElasticsearchException> entry : fetchExceptions.entrySet()) {
                    builder.startObject();
                    {
                        builder.field(FETCH_EXCEPTIONS_ENTRY_FROM_SEQ_NO.getPreferredName(), entry.getKey());
                        builder.field(FETCH_EXCEPTIONS_ENTRY_EXCEPTION.getPreferredName());
                        builder.startObject();
                        {
                            ElasticsearchException.generateThrowableXContent(builder, params, entry.getValue());
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.humanReadableField(
                    TIME_SINCE_LAST_FETCH_MILLIS_FIELD.getPreferredName(),
                    "time_since_last_fetch",
                    new TimeValue(timeSinceLastFetchMillis, TimeUnit.MILLISECONDS));
        }
        builder.endObject();
        return builder;
    }

    public static ShardFollowNodeTaskStatus fromXContent(final XContentParser parser) {
        return STATUS_PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ShardFollowNodeTaskStatus that = (ShardFollowNodeTaskStatus) o;
        return leaderIndex.equals(that.leaderIndex) &&
                shardId == that.shardId &&
                leaderGlobalCheckpoint == that.leaderGlobalCheckpoint &&
                leaderMaxSeqNo == that.leaderMaxSeqNo &&
                followerGlobalCheckpoint == that.followerGlobalCheckpoint &&
                followerMaxSeqNo == that.followerMaxSeqNo &&
                lastRequestedSeqNo == that.lastRequestedSeqNo &&
                numberOfConcurrentReads == that.numberOfConcurrentReads &&
                numberOfConcurrentWrites == that.numberOfConcurrentWrites &&
                numberOfQueuedWrites == that.numberOfQueuedWrites &&
                mappingVersion == that.mappingVersion &&
                totalFetchTimeMillis == that.totalFetchTimeMillis &&
                numberOfSuccessfulFetches == that.numberOfSuccessfulFetches &&
                numberOfFailedFetches == that.numberOfFailedFetches &&
                operationsReceived == that.operationsReceived &&
                totalTransferredBytes == that.totalTransferredBytes &&
                numberOfSuccessfulBulkOperations == that.numberOfSuccessfulBulkOperations &&
                numberOfFailedBulkOperations == that.numberOfFailedBulkOperations &&
                numberOfOperationsIndexed == that.numberOfOperationsIndexed &&
                /*
                 * ElasticsearchException does not implement equals so we will assume the fetch exceptions are equal if they are equal
                 * up to the key set and their messages.  Note that we are relying on the fact that the fetch exceptions are ordered by
                 * keys.
                 */
                fetchExceptions.keySet().equals(that.fetchExceptions.keySet()) &&
                getFetchExceptionMessages(this).equals(getFetchExceptionMessages(that)) &&
                timeSinceLastFetchMillis == that.timeSinceLastFetchMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                leaderIndex,
                shardId,
                leaderGlobalCheckpoint,
                leaderMaxSeqNo,
                followerGlobalCheckpoint,
                followerMaxSeqNo,
                lastRequestedSeqNo,
                numberOfConcurrentReads,
                numberOfConcurrentWrites,
                numberOfQueuedWrites,
                mappingVersion,
                totalFetchTimeMillis,
                numberOfSuccessfulFetches,
                numberOfFailedFetches,
                operationsReceived,
                totalTransferredBytes,
                numberOfSuccessfulBulkOperations,
                numberOfFailedBulkOperations,
                numberOfOperationsIndexed,
                /*
                 * ElasticsearchException does not implement hash code so we will compute the hash code based on the key set and the
                 * messages. Note that we are relying on the fact that the fetch exceptions are ordered by keys.
                 */
                fetchExceptions.keySet(),
                getFetchExceptionMessages(this),
                timeSinceLastFetchMillis);
    }

    private static List<String> getFetchExceptionMessages(final ShardFollowNodeTaskStatus status) {
        return status.fetchExceptions().values().stream().map(ElasticsearchException::getMessage).collect(Collectors.toList());
    }

    public String toString() {
        return Strings.toString(this);
    }

}
