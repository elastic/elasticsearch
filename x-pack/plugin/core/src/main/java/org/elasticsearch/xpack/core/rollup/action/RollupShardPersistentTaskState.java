/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * @param rollupShardIndexerStatus An instance of {@link RollupShardIndexerStatus} with the rollupShardIndexerStatus of the rollup task
 * @param tsid The latest successfully processed tsid component of a tuple (tsid, timestamp)
 */
public record RollupShardPersistentTaskState(RollupShardIndexerStatus rollupShardIndexerStatus, BytesRef tsid)
    implements
        PersistentTaskState {

    public static final String NAME = RollupShardTask.TASK_NAME;
    private static final ParseField ROLLUP_SHARD_INDEXER_STATUS = new ParseField("status");
    private static final ParseField TSID = new ParseField("tsid");

    private static final ConstructingObjectParser<RollupShardPersistentTaskState, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new RollupShardPersistentTaskState((RollupShardIndexerStatus) args[0], (BytesRef) args[1])
    );

    public RollupShardPersistentTaskState(final StreamInput in) throws IOException {
        this(RollupShardIndexerStatus.readFromStream(in), in.readBytesRef());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ROLLUP_SHARD_INDEXER_STATUS.getPreferredName(), rollupShardIndexerStatus);
        builder.field(TSID.getPreferredName(), tsid);
        return builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return RollupShardTask.TASK_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        rollupShardIndexerStatus.writeTo(out);
        out.writeBytesRef(tsid);
    }

    @Override
    public RollupShardIndexerStatus rollupShardIndexerStatus() {
        return rollupShardIndexerStatus;
    }

    public boolean done() {
        return RollupShardIndexerStatus.COMPLETED.equals(rollupShardIndexerStatus)
            || RollupShardIndexerStatus.CANCELLED.equals(rollupShardIndexerStatus)
            || RollupShardIndexerStatus.FAILED.equals(rollupShardIndexerStatus);
    }

    public boolean started() {
        return RollupShardIndexerStatus.STARTED.equals(rollupShardIndexerStatus);
    }

    public boolean cancelled() {
        return RollupShardIndexerStatus.CANCELLED.equals(rollupShardIndexerStatus);
    }

    public boolean failed() {
        return RollupShardIndexerStatus.FAILED.equals(rollupShardIndexerStatus);
    }

    public static RollupShardPersistentTaskState readFromStream(final StreamInput in) throws IOException {
        return new RollupShardPersistentTaskState(RollupShardIndexerStatus.readFromStream(in), in.readBytesRef());
    }

    public static RollupShardPersistentTaskState fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
