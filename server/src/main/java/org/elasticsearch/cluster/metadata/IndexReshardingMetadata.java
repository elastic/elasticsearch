/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * IndexReshardingMetadata holds persistent state managing an in-flight index resharding operation
 *
 * Resharding is changing the number of shards that comprise an index.
 * We currently only support splitting an index into an integer multiple of its current shard count,
 * e.g., going from 1 to 3 shards, or 2 to 4. This is due to the fact that we route documents to
 * shards by hash of the document id modulo the shard count. Multiplying the shard count under this
 * scheme lets us move only the fraction of the documents that route to new shards while the rest stay
 * where they were.
 *
 * During a split, we create new shards and then migrate the documents that belong to the new shards
 * according to the routing function to those new shards. While we're moving documents, search requests
 * may be ongoing, or new documents may be indexed. There must not be ambiguity about whether the source
 * shard or the target shards are responsible for documents being indexed or searched while this handoff
 * is occurring, to ensure that we don't lose or double-count documents during the process. We manage this
 * by maintaining the state of the split on the source and target shards, and making an atomic (from the point
 * of view of indexing and search requests) transition from handling requests that route to the target shard
 * on the source shard, to letting the new target shard handle them. This transition state is called SPLIT_HANDOFF:
 * before the handoff, the source shard has the entire document collection for both the source and target, and handles
 * indexing and search requests. After the handoff, documents that route to the target are handled by the target,
 * and the source does not necessarily have a complete view - it will be missing any documents that are indexed
 * to the target shard after SPLIT_HANDOFF. In fact, when the target becomes active, the source filters target documents
 * from its search results, so that they are not counted twice when the target shard is also searched. The handoff
 * is performed at the target by queueing incoming requests prior to entering HANDOFF, waiting for the target to
 * be RUNNING, and then forwarding requests for the target shard to the target. Similarly, when the target first
 * becomes active it must filter out search results containing documents owned by the source shard, which may be
 * present if the target was created by copying the source shard's Lucene files.
 *
 * To ensure that we always route requests to the correct shard, even in the case of failure of either the source or
 * target during split, we preserve the transition point in persistent state until the split is complete, so that
 * when the source or target recovers, it can resync and route correctly based on that state. This class holds the persistent
 * state required to recover correctly, always maintaing the invariant that only the source shard accepts indexing and search
 * requests for the target prior to SPLIT_HANDOFF, and only the target shard accepts them afterwards.
 *
 * The state we preserve is:
 * * The old and new shard counts for a resize operation, so that the source and target shards
 *   know whether they are source or target, and which is their peer. For example, old:1 new:2 lets
 *   shard 1 figure out that it is the source shard for shard 2, and lets shard 2 figure out that it
 *   is the target for shard 1.
 * * For each target shard, its current state, one of SPLITTING, ANDOFF, CLEANUP, or DONE. If the target is in
 *   SPLITTING, then if either the source or target recovers, split is restarted from the top (though it may reuse any
 *   files already transferred), and the source is responsible for handling indexing and search requests to the target shard.
 *   If the target is in HANDOFF, then on recovery the source is not responsible for handling indexing and search requests
 *   for the target, and must instead filter out target requests from local search. If the target
 *   If the target is in CLEANUP, then the source and target can both delete the other's documents from their local shard.
 *   When the target has finished removing all the source's documents, it transitions to DONE and removes source filters.
 *   As the source completes deletes for each target it can remove filters for that shard as well.
 *   When all targets enter the DONE state and the source has completed removing the documents for every target shard, then
 *   resharding is complete and the IndexReshardingMetadata can be removed from persistent state.
 */
public record IndexReshardingMetadata(int oldShardCount, int newShardCount, ShardReshardingState[] targetShardStates)
    implements
        ToXContentFragment,
        Writeable {
    public enum ShardReshardingState implements Writeable {
        SPLITTING,
        HANDOFF,
        CLEANUP,
        DONE;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }

    // Copying from IndexMetadataStats here
    public static final ParseField OLD_SHARD_COUNT_FIELD = new ParseField("old_shard_count");
    public static final ParseField NEW_SHARD_COUNT_FIELD = new ParseField("new_shard_count");
    public static final ParseField TARGET_SHARD_STATES_FIELD = new ParseField("target_shard_states");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexReshardingMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "index_resharding_metadata_parser",
        false,
        (args, unused) -> new IndexReshardingMetadata(
            (int) args[0],
            (int) args[1],
            ((List<ShardReshardingState>) args[2]).toArray(new ShardReshardingState[0])
        )
    );

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), OLD_SHARD_COUNT_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NEW_SHARD_COUNT_FIELD);
        // XXX I'm not sure this is the best way to parse an array of enums
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (parser, c) -> ShardReshardingState.valueOf(parser.text()),
            TARGET_SHARD_STATES_FIELD
        );
    }

    static IndexReshardingMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(OLD_SHARD_COUNT_FIELD.getPreferredName(), oldShardCount);
        builder.field(NEW_SHARD_COUNT_FIELD.getPreferredName(), newShardCount);
        builder.field(TARGET_SHARD_STATES_FIELD.getPreferredName(), targetShardStates);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(oldShardCount);
        out.writeInt(newShardCount);
        out.writeArray(targetShardStates);
    }

    public IndexReshardingMetadata(StreamInput in) throws IOException {
        this(in.readInt(), in.readInt(), in.readArray(i -> i.readEnum(ShardReshardingState.class), ShardReshardingState[]::new));
    }

    public IndexReshardingMetadata(int oldShardCount, int newShardCount) {
        this(oldShardCount, newShardCount, initialTargetShardStates(newShardCount - oldShardCount));
    }

    public IndexReshardingMetadata(int oldShardCount, int newShardCount, ShardReshardingState[] targetShardStates) {
        assert newShardCount > oldShardCount : "Reshard currently only supports increasing the number of shards";
        assert newShardCount / oldShardCount * oldShardCount == newShardCount : "New shard count must be multiple of old shard count";
        assert targetShardStates.length == newShardCount - oldShardCount : "Must be one target shard state for each new shard";

        this.oldShardCount = oldShardCount;
        this.newShardCount = newShardCount;
        this.targetShardStates = targetShardStates;
    }

    // can't use record implementation because we need a deep comparison of targetShardStates
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        IndexReshardingMetadata otherMetadata = (IndexReshardingMetadata) other;
        return oldShardCount == otherMetadata.oldShardCount
            && newShardCount == otherMetadata.newShardCount
            && Arrays.equals(targetShardStates, otherMetadata.targetShardStates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldShardCount, newShardCount, Arrays.hashCode(targetShardStates));
    }

    public void setTargetShardState(int shard, ShardReshardingState shardState) {
        targetShardStates[shard] = shardState;
    }

    public ShardReshardingState getTargetShardState(int shard) {
        return targetShardStates[shard];
    }

    private static ShardReshardingState[] initialTargetShardStates(int targetShardCount) {
        ShardReshardingState[] targetShardStates = new ShardReshardingState[targetShardCount];
        Arrays.fill(targetShardStates, ShardReshardingState.SPLITTING);
        return targetShardStates;
    }
}
