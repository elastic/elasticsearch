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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * IndexReshardingState is an abstract class holding the persistent state of a generic resharding operation. It contains
 * concrete subclasses for the operations that are currently defined (which is only split for now).
 */
public abstract sealed class IndexReshardingState implements Writeable, ToXContentFragment {
    public static final class Split extends IndexReshardingState {

        public static final ParseField SOURCE_SHARDS_FIELD = new ParseField("source_shards");
        public static final ParseField TARGET_SHARDS_FIELD = new ParseField("target_shards");

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

        private final int oldShardCount;
        private final int newShardCount;
        private final SourceShardState[] sourceShards;
        private final TargetShardState[] targetShards;

        Split(SourceShardState[] sourceShards, TargetShardState[] targetShards) {
            this.sourceShards = sourceShards;
            this.targetShards = targetShards;

            oldShardCount = sourceShards.length;
            newShardCount = oldShardCount + targetShards.length;
        }

        Split(StreamInput in) throws IOException {
            this(
                in.readArray(i -> i.readEnum(SourceShardState.class), SourceShardState[]::new),
                in.readArray(i -> i.readEnum(TargetShardState.class), TargetShardState[]::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(sourceShards);
            out.writeArray(targetShards);
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(SOURCE_SHARDS_FIELD.getPreferredName(), sourceShards);
            builder.field(TARGET_SHARDS_FIELD.getPreferredName(), targetShards);
            return builder;
        }

        public static IndexReshardingState fromMap(Map<String, Object> map) {
            var sourceShardField = map.get(SOURCE_SHARDS_FIELD.getPreferredName());
            if (sourceShardField == null) {
                throw new IllegalArgumentException("missing [" + SOURCE_SHARDS_FIELD.getPreferredName() + "] field");
            }
            if (sourceShardField instanceof List == false) {
                throw new IllegalArgumentException("[" + SOURCE_SHARDS_FIELD.getPreferredName() + "] is not a list");
            }
            // there must be a nicer way
            @SuppressWarnings("unchecked")
            List<String> sourceShardStrings = (List<String>) sourceShardField;

            var targetShardField = map.get(TARGET_SHARDS_FIELD.getPreferredName());
            if (targetShardField == null) {
                throw new IllegalArgumentException("missing [" + TARGET_SHARDS_FIELD.getPreferredName() + "] field");
            }
            @SuppressWarnings("unchecked")
            List<String> targetShardStrings = (List<String>) targetShardField;

            try {
                SourceShardState[] sourceShards = sourceShardStrings.stream()
                    .map(SourceShardState::valueOf)
                    .toArray(SourceShardState[]::new);
                TargetShardState[] targetShards = targetShardStrings.stream()
                    .map(TargetShardState::valueOf)
                    .toArray(TargetShardState[]::new);

                return new Split(sourceShards, targetShards);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("invalid shard state:" + e.getMessage());
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Split otherState = (Split) other;
            // we can ignore oldShardCount and newShardCount since they are derived
            return Arrays.equals(sourceShards, otherState.sourceShards) && Arrays.equals(targetShards, otherState.targetShards);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(sourceShards), Arrays.hashCode(targetShards));
        }

        public int oldShardCount() {
            return oldShardCount;
        }

        public int newShardCount() {
            return newShardCount;
        }

        public SourceShardState[] sourceShards() {
            return sourceShards;
        }

        public TargetShardState[] targetShards() {
            return targetShards;
        }

        /**
         * Create resharding metadata representing a new split operation
         * Split only supports updating an index to a multiple of its current shard count
         * @param shardCount the number of shards in the index at the start of the operation
         * @param multiple the new shard count is shardCount * multiple
         * @return Split representing the start of the requested split
         */
        public static Split newSplitByMultiple(int shardCount, int multiple) {
            assert multiple > 1 : "multiple must be greater than 1";

            final int newShardCount = shardCount * multiple;
            final var sourceShards = new SourceShardState[shardCount];
            final var targetShards = new TargetShardState[newShardCount - shardCount];
            Arrays.fill(sourceShards, SourceShardState.SOURCE);
            Arrays.fill(targetShards, TargetShardState.CLONE);

            return new Split(sourceShards, targetShards);
        }

        /**
         * Get the current shard state of a source shard
         * @param shardNum an index into the shards which must be no greater than the number of shards before split
         * @return the source shard state of the shard identified by shardNum
         */
        public SourceShardState getSourceShardState(int shardNum) {
            assert shardNum >= 0 && shardNum < sourceShards.length : "source shardNum is out of bounds";

            return sourceShards[shardNum];
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
            assert shardNum >= 0 && shardNum < sourceShards.length : "source shardNum is out of bounds";
            assert sourceShards[shardNum].ordinal() + 1 == sourceShardState.ordinal() : "invalid source shard state transition";
            assert sourceShardState == SourceShardState.DONE : "can only move source shard state to DONE";
            for (var target : getTargetStatesFor(shardNum)) {
                assert target == TargetShardState.DONE : "can only move source shard to DONE when all targets are DONE";
            }

            sourceShards[shardNum] = sourceShardState;
        }

        /**
         * Get the current target state of a shard
         * @param shardNum an index into shards greater than or equal to the old shard count and less than the new shard count
         * @return the target shard state for the shard identified by shardNum
         */
        public TargetShardState getTargetShardState(int shardNum) {
            var targetShardNum = shardNum - oldShardCount;

            assert targetShardNum >= 0 && targetShardNum < targetShards.length : "target shardNum is out of bounds";

            return targetShards[targetShardNum];
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

            assert targetShardNum >= 0 && targetShardNum < targetShards.length : "target shardNum is out of bounds";
            assert targetShards[targetShardNum].ordinal() + 1 == targetShardState.ordinal() : "invalid target shard state transition";

            targetShards[targetShardNum] = targetShardState;
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
        public boolean inProgress() {
            for (int i = 0; i < oldShardCount; i++) {
                if (sourceShards[i] == SourceShardState.SOURCE) {
                    return true;
                }
            }

            return false;
        }
    }
}
