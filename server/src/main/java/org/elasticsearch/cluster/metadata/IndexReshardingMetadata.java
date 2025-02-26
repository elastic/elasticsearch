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
public record IndexReshardingMetadata(
    int oldShardCount,
    int newShardCount,
    SourceShardState[] sourceShardStates,
    TargetShardState[] targetShardStates
) implements ToXContentFragment, Writeable {
    public enum SourceShardState implements Writeable {
        SOURCE,
        DONE;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }

    public enum TargetShardState implements Writeable {
        CLONE,
        HANDOFF,
        SPLIT,
        DONE;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }

    // Copying from IndexMetadataStats here
    public static final ParseField OLD_SHARD_COUNT_FIELD = new ParseField("old_shard_count");
    public static final ParseField NEW_SHARD_COUNT_FIELD = new ParseField("new_shard_count");
    public static final ParseField SOURCE_SHARD_STATES_FIELD = new ParseField("source_shard_states");
    public static final ParseField TARGET_SHARD_STATES_FIELD = new ParseField("target_shard_states");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexReshardingMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "index_resharding_metadata_parser",
        false,
        (args, unused) -> new IndexReshardingMetadata(
            (int) args[0],
            (int) args[1],
            ((List<SourceShardState>) args[2]).toArray(new SourceShardState[0]),
            ((List<TargetShardState>) args[3]).toArray(new TargetShardState[0])
        )
    );

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), OLD_SHARD_COUNT_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NEW_SHARD_COUNT_FIELD);
        // XXX I'm not sure this is the best way to parse an array of enums
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (parser, c) -> SourceShardState.valueOf(parser.text()),
            SOURCE_SHARD_STATES_FIELD
        );
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (parser, c) -> TargetShardState.valueOf(parser.text()),
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
        builder.field(SOURCE_SHARD_STATES_FIELD.getPreferredName(), sourceShardStates);
        builder.field(TARGET_SHARD_STATES_FIELD.getPreferredName(), targetShardStates);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(oldShardCount);
        out.writeInt(newShardCount);
        out.writeArray(sourceShardStates);
        out.writeArray(targetShardStates);
    }

    public IndexReshardingMetadata(StreamInput in) throws IOException {
        this(
            in.readInt(),
            in.readInt(),
            in.readArray(i -> i.readEnum(SourceShardState.class), SourceShardState[]::new),
            in.readArray(i -> i.readEnum(TargetShardState.class), TargetShardState[]::new)
        );
    }

    // the default record implementation compares arrays by pointer not by contents
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
            && Arrays.equals(sourceShardStates, otherMetadata.sourceShardStates)
            && Arrays.equals(targetShardStates, otherMetadata.targetShardStates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldShardCount, newShardCount, Arrays.hashCode(sourceShardStates), Arrays.hashCode(targetShardStates));
    }

    /**
     * Create resharding metadata representing a new split operation
     * Split only supports updating an index to a multiple of its current shard count
     * @param shardCount the number of shards in the index at the start of the operation
     * @param multiple the new shard count is shardCount * multiple
     * @return resharding metadata representing the start of the requested split
     */
    public static IndexReshardingMetadata newSplitByMultiple(int shardCount, int multiple) {
        assert multiple > 1 : "multiple must be greater than 1";

        final int newShardCount = shardCount * multiple;
        final var sourceShardStates = new SourceShardState[shardCount];
        final var targetShardStates = new TargetShardState[newShardCount - shardCount];
        Arrays.fill(sourceShardStates, SourceShardState.SOURCE);
        Arrays.fill(targetShardStates, TargetShardState.CLONE);

        return new IndexReshardingMetadata(shardCount, newShardCount, sourceShardStates, targetShardStates);
    }

    /**
     * Get the current shard state of a source shard
     * @param shardNum an index into the shards which must be no greater than the number of shards before split
     * @return the source shard state of the shard identified by shardNum
     */
    public SourceShardState getSourceShardState(int shardNum) {
        assert shardNum >= 0 && shardNum < sourceShardStates.length : "source shardNum is out of bounds";

        return sourceShardStates[shardNum];
    }

    /**
     * Set the current shard state of a source shard
     * Currently the only legal transition is from SOURCE to DONE and any other transition will assert.
     * This could be expressed through a markSourceDone API but this form is the same shape as {@link #setTargetShardState}
     * and leaves the door open for additional source states.
     * @param shardNum an index into the shards which must be no greater than the number of shards before split
     * @param sourceShardState the state to which the shard should be set
     */
    public void setSourceShardState(int shardNum, SourceShardState sourceShardState) {
        assert shardNum >= 0 && shardNum < sourceShardStates.length : "source shardNum is out of bounds";
        assert sourceShardStates[shardNum].ordinal() + 1 == sourceShardState.ordinal() : "invalid source shard state transition";
        assert sourceShardState == SourceShardState.DONE : "can only move source shard state to DONE";
        for (var target : getTargetStatesFor(shardNum)) {
            assert target == TargetShardState.DONE : "can only move source shard to DONE when all targets are DONE";
        }

        sourceShardStates[shardNum] = sourceShardState;
    }

    /**
     * Get the current target state of a shard
     * @param shardNum an index into shards greater than or equal to the old shard count and less than the new shard count
     * @return the target shard state for the shard identified by shardNum
     */
    public TargetShardState getTargetShardState(int shardNum) {
        var targetShardNum = shardNum - oldShardCount;

        assert targetShardNum >= 0 && targetShardNum < targetShardStates.length : "target shardNum is out of bounds";

        return targetShardStates[targetShardNum];
    }

    /**
     * Set the target state of a shard
     * The only legal state in the split state machine is the one following the shard's current state.
     * The reason for this API rather than an advanceState API is to confirm that the caller knows
     * what the current state is when setting it.
     * @param shardNum an index into shards greater than or equal to the old shard count and less than the new shard count
     * @param targetShardState the state to which the shard should be set
     */
    public void setTargetShardState(int shardNum, TargetShardState targetShardState) {
        var targetShardNum = shardNum - oldShardCount;

        assert targetShardNum >= 0 && targetShardNum < targetShardStates.length : "target shardNum is out of bounds";
        assert targetShardStates[targetShardNum].ordinal() + 1 == targetShardState.ordinal() : "invalid target shard state transition";

        targetShardStates[targetShardNum] = targetShardState;
    }

    /**
     * Get all the target shard states related to the given source shard
     * @param shardNum a source shard index greater than or equal to 0 and less than the original shard count
     * @return an array of target shard states in order for the given shard
     */
    public TargetShardState[] getTargetStatesFor(int shardNum) {
        final int numTargets = newShardCount / oldShardCount - 1;
        // it might be useful to return the target shard's index as well as state, can iterate on this
        TargetShardState[] targets = new TargetShardState[numTargets];

        int cur = shardNum + oldShardCount;
        for (int i = 0; i < numTargets; i++) {
            targets[i] = getTargetShardState(cur);
            cur += oldShardCount;
        }

        return targets;
    }

    /**
     * Check whether this metadata represents an incomplete split
     * @return true if the split is incomplete (not all source shards are DONE)
     */
    public boolean splitInProgress() {
        for (int i = 0; i < oldShardCount; i++) {
            if (sourceShardStates[i] == SourceShardState.SOURCE) {
                return true;
            }
        }

        return false;
    }
}
