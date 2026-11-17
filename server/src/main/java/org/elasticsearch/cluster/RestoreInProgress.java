/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Meta data about restore processes that are currently executing
 */
public class RestoreInProgress extends AbstractNamedDiffable<Custom> implements Custom, Iterable<RestoreInProgress.Entry> {

    public static final String TYPE = "restore";

    private static final TransportVersion RESTORE_IN_PROGRESS_REPORT_SHARD_RESTORING = TransportVersion.fromName(
        "restore_in_progress_report_shard_restoring"
    );

    static final TransportVersion RESTORE_SHARD_FAILURE_REASON = TransportVersion.fromName("restore_shard_failure_reason");

    public static final RestoreInProgress EMPTY = new RestoreInProgress(Map.of());

    private final Map<String, Entry> entries;

    public static RestoreInProgress get(ClusterState state) {
        return state.custom(TYPE, EMPTY);
    }

    /**
     * Constructs new restore metadata
     *
     * @param entries map of currently running restore processes keyed by their restore uuid
     */
    private RestoreInProgress(Map<String, Entry> entries) {
        this.entries = entries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return entries.equals(((RestoreInProgress) o).entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("RestoreInProgress[");
        entries.entrySet()
            .forEach(entry -> builder.append("{").append(entry.getKey()).append("}{").append(entry.getValue().snapshot).append("},"));
        builder.setCharAt(builder.length() - 1, ']');
        return builder.toString();
    }

    public Entry get(String restoreUUID) {
        return entries.get(restoreUUID);
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public Iterator<Entry> iterator() {
        return entries.values().iterator();
    }

    public static final class Builder {

        private final Map<String, Entry> entries = new HashMap<>();

        public Builder() {}

        public Builder(RestoreInProgress restoreInProgress) {
            entries.putAll(restoreInProgress.entries);
        }

        public Builder add(Entry entry) {
            entries.put(entry.uuid, entry);
            return this;
        }

        public RestoreInProgress build() {
            return entries.isEmpty() ? EMPTY : new RestoreInProgress(Collections.unmodifiableMap(entries));
        }
    }

    /**
     * Restore metadata
     */
    public record Entry(
        String uuid,
        Snapshot snapshot,
        State state,
        boolean quiet,
        List<String> indices,
        Map<ShardId, ShardRestoreStatus> shards,
        boolean reportShardRestoring
    ) {
        /**
         * Creates new restore metadata that does not report restoring shards as {@code ShardRestoringException}
         *
         * @param uuid     uuid of the restore
         * @param snapshot snapshot
         * @param state    current state of the restore process
         * @param quiet    {@code true} if logging of the start and completion of the snapshot restore should be at {@code DEBUG} log
         *                 level, else it should be at {@code INFO} log level
         * @param indices  list of indices being restored
         * @param shards   map of shards being restored to their current restore status
         */
        public Entry(
            String uuid,
            Snapshot snapshot,
            State state,
            boolean quiet,
            List<String> indices,
            Map<ShardId, ShardRestoreStatus> shards
        ) {
            this(uuid, snapshot, state, quiet, indices, shards, /* reportShardRestoring = */ false);
        }

        /**
         * Creates new restore metadata
         *
         * @param uuid                 uuid of the restore
         * @param snapshot             snapshot
         * @param state                current state of the restore process
         * @param quiet                {@code true} if logging of the start and completion of the snapshot restore should be at
         *                             {@code DEBUG} log level, else it should be at {@code INFO} log level
         * @param indices              list of indices being restored
         * @param shards               map of shards being restored to their current restore status
         * @param reportShardRestoring {@code true} if requests that hit a shard of this restore should fail with a
         *                             {@code ShardRestoringException} rather than the generic shard-unavailable errors. Chosen explicitly
         *                             by the caller that starts the restore. Nodes that do not support it read it as {@code false}.
         */
        public Entry(
            String uuid,
            Snapshot snapshot,
            State state,
            boolean quiet,
            List<String> indices,
            Map<ShardId, ShardRestoreStatus> shards,
            boolean reportShardRestoring
        ) {
            this.reportShardRestoring = reportShardRestoring;
            this.snapshot = Objects.requireNonNull(snapshot);
            this.state = Objects.requireNonNull(state);
            this.quiet = Objects.requireNonNull(quiet);
            this.indices = Objects.requireNonNull(indices);
            if (shards == null) {
                this.shards = Map.of();
            } else {
                this.shards = shards;
            }
            this.uuid = Objects.requireNonNull(uuid);
        }
    }

    /**
     * Why a shard's restore ended in {@link State#FAILURE}. The free-text {@link ShardRestoreStatus#reason()} carries the detail for
     * diagnostics, while this classifies the failure into a small fixed set so that it can be counted, for example as a metric attribute,
     * without the unbounded cardinality of the free text.
     */
    public enum ShardRestoreFailureReason {
        /** The shard failed or was missing in the snapshot, so a partial restore skipped it from the start. */
        FAILED_IN_SNAPSHOT("failed_in_snapshot"),
        /** The snapshot files were found to be corrupt while recovering the shard, so no retry can succeed. */
        CORRUPTION("corruption"),
        /** The allocation deciders refused every node, so the shard cannot be allocated to recover from the snapshot. */
        NO_ELIGIBLE_NODE("no_eligible_node"),
        /** The primary was allocated from something other than the snapshot, for example forced to an empty or a stale copy. */
        RECOVERY_SOURCE_CHANGED("recovery_source_changed"),
        /** The index was deleted before its shard finished restoring. */
        INDEX_DELETED("index_deleted");

        private final String value;

        ShardRestoreFailureReason(String value) {
            this.value = value;
        }

        /** The stable lower-case form, used as a metric attribute value. */
        public String value() {
            return value;
        }
    }

    /**
     * Represents status of a restored shard
     *
     * @param nodeId        the node the shard is restoring on, if known
     * @param state         the shard's restore state
     * @param reason        free-text detail of a failure, for diagnostics
     * @param failureReason the classified reason for a {@link State#FAILURE}, or {@code null} for any other state. It is also
     *                      {@code null} for a failure that was recorded by, or received from, a node that does not classify failures.
     */
    public record ShardRestoreStatus(String nodeId, State state, @Nullable String reason, @Nullable ShardRestoreFailureReason failureReason)
        implements
            Writeable {

        public ShardRestoreStatus {
            assert failureReason == null || state == State.FAILURE : "failure reason [" + failureReason + "] for state [" + state + "]";
        }

        /**
         * Constructs a new shard restore status in initializing state on the given node
         *
         * @param nodeId node id
         */
        public ShardRestoreStatus(String nodeId) {
            this(nodeId, State.INIT);
        }

        /**
         * Constructs a new shard restore status in with specified state on the given node
         *
         * @param nodeId node id
         * @param state  restore state
         */
        public ShardRestoreStatus(String nodeId, State state) {
            this(nodeId, state, null);
        }

        /**
         * Constructs a new shard restore status with the specified state and free-text reason, but no classified failure reason.
         */
        public ShardRestoreStatus(String nodeId, State state, @Nullable String reason) {
            this(nodeId, state, reason, null);
        }

        /**
         * Constructs the status of a shard whose restore failed for the given classified reason.
         *
         * @param nodeId        node id, if known
         * @param failureReason why the restore of the shard failed
         * @param reason        free-text detail, if any
         */
        public static ShardRestoreStatus failed(@Nullable String nodeId, ShardRestoreFailureReason failureReason, @Nullable String reason) {
            return new ShardRestoreStatus(nodeId, State.FAILURE, reason, Objects.requireNonNull(failureReason));
        }

        public static ShardRestoreStatus readFrom(StreamInput in) throws IOException {
            final String nodeId = in.readOptionalString();
            final State state = State.fromValue(in.readByte());
            final String reason = in.readOptionalString();
            // nodes that do not classify failures never send it, which leaves a failure unclassified
            final ShardRestoreFailureReason failureReason = in.getTransportVersion().supports(RESTORE_SHARD_FAILURE_REASON)
                ? in.readOptionalEnum(ShardRestoreFailureReason.class)
                : null;
            return new ShardRestoreStatus(nodeId, state, reason, failureReason);
        }

        /**
         * Writes restore status to stream output
         *
         * @param out stream input
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalString(reason);
            if (out.getTransportVersion().supports(RESTORE_SHARD_FAILURE_REASON)) {
                out.writeOptionalEnum(failureReason);
            }
        }
    }

    /**
     * Shard restore process state
     */
    public enum State {
        /**
         * Initializing state
         */
        INIT((byte) 0),
        /**
         * Started state
         */
        STARTED((byte) 1),
        /**
         * Restore finished successfully
         */
        SUCCESS((byte) 2),
        /**
         * Restore failed
         */
        FAILURE((byte) 3);

        private final byte value;

        /**
         * Constructs new state
         *
         * @param value state code
         */
        State(byte value) {
            this.value = value;
        }

        /**
         * Returns state code
         *
         * @return state code
         */
        public byte value() {
            return value;
        }

        /**
         * Returns true if restore process completed (either successfully or with failure)
         *
         * @return true if restore process completed
         */
        public boolean completed() {
            return this == SUCCESS || this == FAILURE;
        }

        /**
         * Returns state corresponding to state code
         *
         * @param value stat code
         * @return state
         */
        public static State fromValue(byte value) {
            return switch (value) {
                case 0 -> INIT;
                case 1 -> STARTED;
                case 2 -> SUCCESS;
                case 3 -> FAILURE;
                default -> throw new IllegalArgumentException("No snapshot state for value [" + value + "]");
            };
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    public RestoreInProgress(StreamInput in) throws IOException {
        int count = in.readVInt();
        final Map<String, Entry> entriesBuilder = Maps.newHashMapWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            final String uuid;
            uuid = in.readString();
            Snapshot snapshot = new Snapshot(in);
            State state = State.fromValue(in.readByte());
            boolean quiet;
            quiet = in.readBoolean();
            List<String> indices = in.readCollectionAsImmutableList(StreamInput::readString);
            final Map<ShardId, ShardRestoreStatus> shards = in.readImmutableMap(ShardId::new, ShardRestoreStatus::readFrom);
            // nodes that do not know about the flag never send it, which is the same as the restore not reporting shard restoring
            final boolean reportShardRestoring = in.getTransportVersion().supports(RESTORE_IN_PROGRESS_REPORT_SHARD_RESTORING)
                && in.readBoolean();
            entriesBuilder.put(uuid, new Entry(uuid, snapshot, state, quiet, indices, shards, reportShardRestoring));
        }
        this.entries = Collections.unmodifiableMap(entriesBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(entries.values(), (o, entry) -> {
            o.writeString(entry.uuid);
            entry.snapshot().writeTo(o);
            o.writeByte(entry.state().value());
            o.writeBoolean(entry.quiet());
            o.writeStringCollection(entry.indices);
            o.writeMap(entry.shards);
            if (o.getTransportVersion().supports(RESTORE_IN_PROGRESS_REPORT_SHARD_RESTORING)) {
                o.writeBoolean(entry.reportShardRestoring());
            }
        });
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            Iterators.single((builder, params) -> builder.startArray("snapshots")),
            Iterators.map(entries.values().iterator(), entry -> (builder, params) -> {
                builder.startObject();
                builder.field("snapshot", entry.snapshot().getSnapshotId().getName());
                builder.field("repository", entry.snapshot().getRepository());
                builder.field("state", entry.state());
                builder.startArray("indices");
                {
                    for (String index : entry.indices()) {
                        builder.value(index);
                    }
                }
                builder.endArray();
                builder.startArray("shards");
                {
                    for (Map.Entry<ShardId, ShardRestoreStatus> shardEntry : entry.shards.entrySet()) {
                        ShardId shardId = shardEntry.getKey();
                        ShardRestoreStatus status = shardEntry.getValue();
                        builder.startObject();
                        {
                            builder.field("index", shardId.getIndex());
                            builder.field("shard", shardId.getId());
                            builder.field("state", status.state());
                        }
                        builder.endObject();
                    }
                }

                builder.endArray();
                builder.endObject();
                return builder;
            }),
            Iterators.single((builder, params) -> builder.endArray())
        );
    }
}
