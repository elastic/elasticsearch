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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * IndexReshardingMetadata holds persistent state managing an in-flight index resharding operation
 *
 * Resharding is changing the number of shards that make up an index, in place.
 * We currently only support splitting an index into an integer multiple of its current shard count,
 * e.g., going from 1 to 3 shards, or 2 to 4. This is because we route documents to shards by hash of
 * the document id modulo the shard count. Multiplying the shard count under this scheme lets us move
 * only the fraction of the documents that route to new shards while the rest stay where they were.
 *
 * During a split, we create new shards and then migrate the documents that belong to the new shards
 * according to the routing function to those new shards. While we're moving documents, search requests
 * may be ongoing, or new documents may be indexed. There must not be ambiguity about whether the source
 * shard or the target shards are responsible for documents being indexed or searched while this handoff
 * is occurring, to ensure that we don't lose or double-count documents during the process. We prevent this
 * by maintaining the state of the split on the source and target shards, and making an atomic (from the point
 * of view of indexing and search requests) transition from having the source shard handle requests for documents
 * that belong to the target shard, to having the target shard handle them itself.
 *
 * Before the handoff, the source shard has the entire document collection for both the source and target, and handles
 * indexing and search requests. After the handoff, documents that route to the target are handled by the target,
 * and the source does not necessarily have a complete view - it will be missing any documents that are indexed
 * to the target shard after handoff. Indeed, when the target becomes active, the source filters target documents
 * from its search results, so that they are not counted twice when the target shard is also searched. The handoff
 * is performed at the target by queueing incoming requests prior to entering handoff, waiting for the target to
 * be RUNNING, and then forwarding requests for the target shard to the target. Similarly, when the target first
 * becomes active it must filter out search results containing documents owned by the source shard, which may be
 * present if the target was created by copying the source shard's Lucene files.
 *
 * To ensure that we always route requests to the correct shard, even in the case of failure of either source or
 * target shards during split, we preserve the transition point in persistent state until the split is complete, so
 * that when the source or target recovers, it can resync and route correctly based on that state. This class holds
 * the persistent state required to recover correctly, always maintaining the invariant that only the source shard
 * accepts indexing and search requests for the target prior to handoff, and only the target shard accepts them afterward.
 *
 * The state we preserve is:
 * * The old and new shard counts for a resize operation, so that we can always identify which shards are sources
 *   and which are targets during resharding. For example, old:2 new:6 implies that shard 1 is the source shard for
 *   shards 3 and 5, and shard 2 is the source for shards 4 and 6.
 * * For each source shard, its current source state, which is either `SOURCE` or `DONE`.
 *   - If a source shard may still contain data for any target shard then it is in state `SOURCE`.
 *   - When all targets for a source have moved to `SPLIT` (see below), then the source deletes all documents from
 *     its store that are now the responsibility of the target shards and transitions to `DONE`.
 *   This isn't strictly required to be persistent for correctness, but it can save time on recovery
 *   by allowing a DONE shard to skip interrogating targets and repeating cleanup.
 * * For each target shard, its current target state, which is one of `CLONE`, `HANDOFF`, `SPLIT`, or `DONE`.
 *   - If the target has not yet copied all data from the source shard, then it is in `CLONE`.
 *   - It moves to `HANDOFF` when it has copied all of its data from the source to indicate that it is now ready to
 *     receive indexing actions, and starts RUNNING. After this point, the source may no longer contain the entire contents
 *     of the target and must not index documents belonging to the target. But since search shards can't start up until
 *     their corresponding index shards are active, search requests would fail if they routed to the target shard immediately
 *     after handoff. So at HANDOFF, the source shards continue to service searches, but block refresh since they cannot
 *     be guaranteed to have seen documents indexed after HANDOFF.
 *   - When the target shard's corresponding search replica has started running, the target requests that the source filter
 *     search results belonging to the target, and moves the target shard's state to `SPLIT`. The target's search replica
 *     likewise filters documents not belonging to the target, which may be present due to the target bootstrapping by copying
 *     the source's lucene files.
 *   - Upon entering `SPLIT`, the target starts deleting all documents from its lucene store that do not belong to it. When that
 *     is complete, it moves to `DONE` and removes filters for other shards, which are no longer necessary.
 *
 * Note that each target shard's split operates independently and all may happen concurrently.
 *
 * When all source shards have transitioned to `DONE`, the resize is complete and this metadata may be removed from cluster state.
 * We only allow at most a single resharding operation to be in flight for an index, so removing this metadata is a prerequisite
 * to beginning another resharding operation.
 */
public class IndexReshardingMetadata implements ToXContentFragment, Writeable {
    private static final String SPLIT_FIELD_NAME = "split";
    private static final ParseField SPLIT_FIELD = new ParseField(SPLIT_FIELD_NAME);
    // This exists only so that tests can verify that IndexReshardingMetadata supports more than one kind of operation.
    // It can be removed when we have defined a second real operation, such as shrink.
    private static final String NOOP_FIELD_NAME = "noop";
    private static final ParseField NOOP_FIELD = new ParseField(NOOP_FIELD_NAME);

    private static final ConstructingObjectParser<IndexReshardingMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "index_resharding_metadata",
        args -> {
            // the parser ensures exactly one argument will not be null
            if (args[0] != null) {
                return new IndexReshardingMetadata((IndexReshardingState) args[0]);
            } else {
                return new IndexReshardingMetadata((IndexReshardingState) args[1]);
            }
        }
    );

    static {
        PARSER.declareObjectOrNull(
            ConstructingObjectParser.optionalConstructorArg(),
            (parser, c) -> IndexReshardingState.Split.fromXContent(parser),
            null,
            SPLIT_FIELD
        );
        PARSER.declareObjectOrNull(
            ConstructingObjectParser.optionalConstructorArg(),
            (parser, c) -> IndexReshardingState.Noop.fromXContent(parser),
            null,
            NOOP_FIELD
        );
        PARSER.declareExclusiveFieldSet(SPLIT_FIELD.getPreferredName(), NOOP_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(SPLIT_FIELD.getPreferredName(), NOOP_FIELD.getPreferredName());
    }

    private final IndexReshardingState state;

    // visible for testing
    IndexReshardingMetadata(IndexReshardingState state) {
        this.state = state;
    }

    public IndexReshardingMetadata(StreamInput in) throws IOException {
        var stateName = in.readString();

        state = switch (stateName) {
            case NOOP_FIELD_NAME -> new IndexReshardingState.Noop(in);
            case SPLIT_FIELD_NAME -> new IndexReshardingState.Split(in);
            default -> throw new IllegalStateException("unknown operation [" + stateName + "]");
        };
    }

    // for testing
    IndexReshardingState getState() {
        return state;
    }

    static IndexReshardingMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        String name = switch (state) {
            case IndexReshardingState.Noop ignored -> NOOP_FIELD.getPreferredName();
            case IndexReshardingState.Split ignored -> SPLIT_FIELD.getPreferredName();
        };
        builder.startObject(name);
        state.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        String name = switch (state) {
            case IndexReshardingState.Noop ignored -> NOOP_FIELD.getPreferredName();
            case IndexReshardingState.Split ignored -> SPLIT_FIELD.getPreferredName();
        };
        out.writeString(name);
        state.writeTo(out);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        IndexReshardingMetadata otherMetadata = (IndexReshardingMetadata) other;

        return Objects.equals(state, otherMetadata.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state);
    }

    public String toString() {
        return "IndexReshardingMetadata [state=" + state + "]";
    }

    /**
     * Create resharding metadata representing a new split operation
     * Split only supports updating an index to a multiple of its current shard count
     * @param shardCount the number of shards in the index at the start of the operation
     * @param multiple the new shard count is shardCount * multiple
     * @return resharding metadata representing the start of the requested split
     */
    public static IndexReshardingMetadata newSplitByMultiple(int shardCount, int multiple) {
        return new IndexReshardingMetadata(IndexReshardingState.Split.newSplitByMultiple(shardCount, multiple));
    }

    public static boolean isSplitTarget(ShardId shardId, @Nullable IndexReshardingMetadata reshardingMetadata) {
        return reshardingMetadata != null && reshardingMetadata.isSplit() && reshardingMetadata.getSplit().isTargetShard(shardId.id());
    }

    public IndexReshardingMetadata transitionSplitTargetToNewState(
        ShardId shardId,
        IndexReshardingState.Split.TargetShardState newTargetState
    ) {
        assert state instanceof IndexReshardingState.Split;
        IndexReshardingState.Split.Builder builder = new IndexReshardingState.Split.Builder((IndexReshardingState.Split) state);
        builder.setTargetShardState(shardId.getId(), newTargetState);
        return new IndexReshardingMetadata(builder.build());
    }

    /**
     * @return the split state of this metadata block, or throw IllegalArgumentException if this metadata doesn't represent a split
     */
    public IndexReshardingState.Split getSplit() {
        return switch (state) {
            case IndexReshardingState.Noop ignored -> throw new IllegalArgumentException("resharding metadata is not a split");
            case IndexReshardingState.Split s -> s;
        };
    }

    public boolean isSplit() {
        return state instanceof IndexReshardingState.Split;
    }

    /**
     * @return the number of shards the index has at the start of this operation
     */
    public int shardCountBefore() {
        return state.shardCountBefore();
    }

    /**
     * @return the number of shards that the index will have when resharding completes
     */
    public int shardCountAfter() {
        return state.shardCountAfter();
    }
}
