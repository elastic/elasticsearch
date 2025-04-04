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
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * IndexReshardingState is an abstract class holding the persistent state of a generic resharding operation. It contains
 * concrete subclasses for the operations that are currently defined (which is only split for now).
 */
public abstract sealed class IndexReshardingState implements Writeable, ToXContentFragment {
    /**
     * @return the number of shards the index has at the start of this operation
     */
    public abstract int shardCountBefore();

    /**
     * @return the number of shards that the index will have when resharding completes
     */
    public abstract int shardCountAfter();

    // This class exists only so that tests can check that IndexReshardingMetadata can support more than one kind of operation.
    // When we have another real operation such as Shrink this can be removed.
    public static final class Noop extends IndexReshardingState {
        private static final ObjectParser<Noop, Void> NOOP_PARSER = new ObjectParser<>("noop", Noop::new);

        Noop() {}

        Noop(StreamInput in) throws IOException {
            this();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        static IndexReshardingState fromXContent(XContentParser parser) throws IOException {
            return NOOP_PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            return other != null && getClass() == other.getClass();
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public int shardCountBefore() {
            return 1;
        }

        @Override
        public int shardCountAfter() {
            return 1;
        }
    }

    public static final class Split extends IndexReshardingState {
        public enum SourceShardState implements Writeable {
            /**
             * The argument is for serialization because using the ordinal breaks if
             * any values in the enum are reordered. It must not be changed once defined.
             */
            SOURCE((byte) 0),
            DONE((byte) 1);

            private final byte code;

            SourceShardState(byte code) {
                this.code = code;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeByte(this.code);
            }

            public static SourceShardState readFrom(StreamInput in) throws IOException {
                var code = in.readByte();
                return switch (code) {
                    case 0 -> SOURCE;
                    case 1 -> DONE;
                    default -> throw new IllegalStateException("unknown source shard state [" + code + "]");
                };
            }
        }

        public enum TargetShardState implements Writeable {
            /**
             * The argument is for serialization because using the ordinal breaks if
             * any values in the enum are reordered. It must not be changed once defined.
             */
            CLONE((byte) 0),
            HANDOFF((byte) 1),
            SPLIT((byte) 2),
            DONE((byte) 3);

            private final byte code;

            TargetShardState(byte code) {
                this.code = code;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeByte(this.code);
            }

            public static TargetShardState readFrom(StreamInput in) throws IOException {
                var code = in.readByte();
                return switch (code) {
                    case 0 -> CLONE;
                    case 1 -> HANDOFF;
                    case 2 -> SPLIT;
                    case 3 -> DONE;
                    default -> throw new IllegalStateException("unknown target shard state [" + code + "]");
                };
            }
        }

        private static final ParseField SOURCE_SHARDS_FIELD = new ParseField("source_shards");
        private static final ParseField TARGET_SHARDS_FIELD = new ParseField("target_shards");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Split, Void> SPLIT_PARSER = new ConstructingObjectParser<>(
            "split",
            args -> new Split(
                ((List<SourceShardState>) args[0]).toArray(new SourceShardState[0]),
                ((List<TargetShardState>) args[1]).toArray(new TargetShardState[0])
            )
        );

        static {
            SPLIT_PARSER.declareObjectArray(
                ConstructingObjectParser.constructorArg(),
                (parser, c) -> SourceShardState.valueOf(parser.text()),
                SOURCE_SHARDS_FIELD
            );
            SPLIT_PARSER.declareObjectArray(
                ConstructingObjectParser.constructorArg(),
                (parser, c) -> TargetShardState.valueOf(parser.text()),
                TARGET_SHARDS_FIELD
            );
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
                in.readArray(SourceShardState::readFrom, SourceShardState[]::new),
                in.readArray(TargetShardState::readFrom, TargetShardState[]::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(sourceShards);
            out.writeArray(targetShards);
        }

        static Split fromXContent(XContentParser parser) throws IOException {
            return SPLIT_PARSER.parse(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(SOURCE_SHARDS_FIELD.getPreferredName(), sourceShards);
            builder.field(TARGET_SHARDS_FIELD.getPreferredName(), targetShards);

            return builder;
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

        @Override
        public int shardCountBefore() {
            return oldShardCount;
        }

        @Override
        public int shardCountAfter() {
            return newShardCount;
        }

        // visible for testing
        SourceShardState[] sourceShards() {
            return sourceShards.clone();
        }

        // visible for testing
        TargetShardState[] targetShards() {
            return targetShards.clone();
        }

        public int sourceShard(int targetShard) {
            return targetShard % shardCountBefore();
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

        public static class Builder {
            private final SourceShardState[] sourceShards;
            private final TargetShardState[] targetShards;

            public Builder(IndexReshardingState.Split split) {
                this.sourceShards = split.sourceShards();
                this.targetShards = split.targetShards();
            }

            /**
             * Set the shard state of a source shard
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
                var split = new Split(sourceShards, targetShards);
                for (var target : split.getTargetStatesFor(shardNum)) {
                    assert target == TargetShardState.DONE : "can only move source shard to DONE when all targets are DONE";
                }

                sourceShards[shardNum] = sourceShardState;
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
                var targetShardNum = shardNum - sourceShards.length;

                assert targetShardNum >= 0 && targetShardNum < targetShards.length : "target shardNum is out of bounds";
                assert targetShards[targetShardNum].ordinal() + 1 == targetShardState.ordinal() : "invalid target shard state transition";

                targetShards[targetShardNum] = targetShardState;
            }

            /**
             * Build a new Split
             * @return Split reflecting the current state of the builder
             */
            public Split build() {
                return new Split(sourceShards, targetShards);
            }
        }

        /**
         * Create a Builder from the state of this Split
         * The Split itself is immutable. Modifications can be applied to the builder,
         * which can be used to replace the current Split in IndexMetadata when it in turn is built.
         * @return a Builder reflecting the state of this split
         */
        public Builder builder() {
            return new Builder(this);
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

        public boolean isTargetShard(int shardId) {
            return shardId >= shardCountBefore();
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

        public boolean targetStateAtLeast(int shardNum, TargetShardState targetShardState) {
            return getTargetShardState(shardNum).ordinal() >= targetShardState.ordinal();
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

        public Stream<TargetShardState> targetStates() {
            return Arrays.stream(targetShards);
        }

        /**
         * Check whether all target shards for the given source shard are done.
         * @param shardNum a source shard index greater than or equal to 0 and less than the original shard count
         * @return true if all target shards for the given source shard are done.
         */
        public boolean targetsDone(int shardNum) {
            var targets = getTargetStatesFor(shardNum);
            return Arrays.stream(targets).allMatch(target -> target == IndexReshardingState.Split.TargetShardState.DONE);
        }

        /**
         * Get all the target shard states related to the given source shard
         * @param shardNum a source shard index greater than or equal to 0 and less than the original shard count
         * @return an array of target shard states in order for the given shard
         */
        private TargetShardState[] getTargetStatesFor(int shardNum) {
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
    }
}
