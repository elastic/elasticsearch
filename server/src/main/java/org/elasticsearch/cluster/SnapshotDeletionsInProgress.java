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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Represents the in-progress snapshot deletions in the cluster state.
 */
public class SnapshotDeletionsInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final SnapshotDeletionsInProgress EMPTY = new SnapshotDeletionsInProgress(List.of());

    public static final String TYPE = "snapshot_deletions";

    // the list of snapshot deletion request entries
    private final List<Entry> entries;

    private SnapshotDeletionsInProgress(List<Entry> entries) {
        this.entries = entries;
        assert entries.size() == entries.stream().map(Entry::uuid).distinct().count() : "Found duplicate UUIDs in entries " + entries;
        assert assertNoConcurrentDeletionsForSameRepository(entries);
    }

    public static SnapshotDeletionsInProgress of(List<SnapshotDeletionsInProgress.Entry> entries) {
        if (entries.isEmpty()) {
            return EMPTY;
        }
        return new SnapshotDeletionsInProgress(Collections.unmodifiableList(entries));
    }

    public SnapshotDeletionsInProgress(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(Entry::readFrom));
    }

    private static boolean assertNoConcurrentDeletionsForSameRepository(List<Entry> entries) {
        final Set<String> activeRepositories = new HashSet<>();
        for (Entry entry : entries) {
            if (entry.state() == State.STARTED) {
                final boolean added = activeRepositories.add(entry.repository());
                assert added : "Found multiple running deletes for a single repository in " + entries;
            }
        }
        return true;
    }

    public static SnapshotDeletionsInProgress get(ClusterState state) {
        return state.custom(TYPE, EMPTY);
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} which adds
     * the given {@link Entry} to the invoking instance.
     */
    public SnapshotDeletionsInProgress withAddedEntry(Entry entry) {
        return SnapshotDeletionsInProgress.of(CollectionUtils.appendToCopy(getEntries(), entry));
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} that has the entry with the given {@code deleteUUID} removed from its
     * entries.
     */
    public SnapshotDeletionsInProgress withRemovedEntry(String deleteUUID) {
        List<Entry> updatedEntries = new ArrayList<>(entries.size() - 1);
        boolean removed = false;
        for (Entry entry : entries) {
            if (entry.uuid().equals(deleteUUID)) {
                removed = true;
            } else {
                updatedEntries.add(entry);
            }
        }
        return removed ? SnapshotDeletionsInProgress.of(updatedEntries) : this;
    }

    /**
     * Returns an unmodifiable list of snapshot deletion entries.
     */
    public List<Entry> getEntries() {
        return entries;
    }

    /**
     * Checks if there is an actively executing delete operation for the given repository
     *
     * @param repository repository name
     */
    public boolean hasExecutingDeletion(String repository) {
        for (Entry entry : entries) {
            if (entry.state() == State.STARTED && entry.repository().equals(repository)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if there are snapshot deletions in progress in the cluster,
     * returns {@code false} otherwise.
     */
    public boolean hasDeletionsInProgress() {
        return entries.isEmpty() == false;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SnapshotDeletionsInProgress that = (SnapshotDeletionsInProgress) o;
        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return 31 + entries.hashCode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(entries);
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MINIMUM_COMPATIBLE;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            Iterators.single((builder, params) -> builder.startArray(TYPE)),
            Iterators.map(entries.iterator(), entry -> (builder, params) -> {
                builder.startObject();
                {
                    builder.field("repository", entry.repository());
                    builder.startArray("snapshots");
                    for (SnapshotId snapshot : entry.snapshots) {
                        builder.value(snapshot.getName());
                    }
                    builder.endArray();
                    builder.timestampFieldsFromUnixEpochMillis("start_time_millis", "start_time", entry.startTime);
                    builder.field("repository_state_id", entry.repositoryStateId);
                    builder.field("state", entry.state);
                }
                builder.endObject();
                return builder;
            }),
            Iterators.single((builder, params) -> builder.endArray())
        );
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("SnapshotDeletionsInProgress[");
        for (int i = 0; i < entries.size(); i++) {
            builder.append(entries.get(i).snapshots());
            if (i + 1 < entries.size()) {
                builder.append(",");
            }
        }
        return builder.append("]").toString();
    }

    /**
     * A class representing a snapshot deletion request entry in the cluster state.
     */
    public record Entry(String repoName, List<SnapshotId> snapshots, long startTime, long repositoryStateId, State state, String uuid)
        implements
            Writeable,
            RepositoryOperation {

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public Entry(String repoName, List<SnapshotId> snapshots, long startTime, long repositoryStateId, State state) {
            this(repoName, snapshots, startTime, repositoryStateId, state, UUIDs.randomBase64UUID());
        }

        public Entry {
            assert snapshots.size() == new HashSet<>(snapshots).size() : "Duplicate snapshot ids in " + snapshots;
        }

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public static Entry readFrom(StreamInput in) throws IOException {
            return new Entry(
                in.readString(),
                in.readCollectionAsImmutableList(SnapshotId::new),
                in.readVLong(),
                in.readLong(),
                State.readFrom(in),
                in.readString()
            );
        }

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public Entry started() {
            assert state == State.WAITING;
            return new Entry(repository(), snapshots, startTime, repositoryStateId, State.STARTED, uuid);
        }

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public Entry withAddedSnapshots(Collection<SnapshotId> newSnapshots) {
            assert state == State.WAITING;
            final Collection<SnapshotId> updatedSnapshots = new HashSet<>(snapshots);
            if (updatedSnapshots.addAll(newSnapshots) == false) {
                return this;
            }
            return new Entry(repository(), List.copyOf(updatedSnapshots), startTime, repositoryStateId, State.WAITING, uuid);
        }

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public Entry withSnapshots(Collection<SnapshotId> snapshots) {
            return new Entry(repository(), List.copyOf(snapshots), startTime, repositoryStateId, state, uuid);
        }

        @SuppressForbidden(reason = "using a private constructor within the same file")
        public Entry withRepoGen(long repoGen) {
            return new Entry(repository(), snapshots, startTime, repoGen, state, uuid);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repoName);
            out.writeCollection(snapshots);
            out.writeVLong(startTime);
            out.writeLong(repositoryStateId);
            state.writeTo(out);
            out.writeString(uuid);
        }

        @Override
        public String repository() {
            return repoName;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }
    }

    public enum State implements Writeable {

        /**
         * Delete is waiting to execute because there are snapshots and or a delete operation that has to complete before this delete may
         * run.
         */
        WAITING((byte) 0),

        /**
         * Delete is physically executing on the repository.
         */
        STARTED((byte) 1);

        private final byte value;

        State(byte value) {
            this.value = value;
        }

        public static State readFrom(StreamInput in) throws IOException {
            final byte value = in.readByte();
            return switch (value) {
                case 0 -> WAITING;
                case 1 -> STARTED;
                default -> throw new IllegalArgumentException("No snapshot delete state for value [" + value + "]");
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(value);
        }
    }
}
