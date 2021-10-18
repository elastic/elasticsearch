/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import static java.util.Collections.unmodifiableSortedSet;

/**
 * Represents snapshots marked as to be deleted and pending deletion.
 *
 * Snapshots pending deletion are added to the cluster state when searchable snapshots indices with a specific setting are deleted (see
 * MetadataDeleteIndexService#updateSnapshotDeletionsPending()). Because deleting snapshots requires a consistent view of the repository
 * they belong to it is not possible to delete searchable snapshots indices and their backing snapshots in the same cluster state update.
 *
 * Hence we keep in cluster state the snapshot that should be deleted from repositories. To be able to delete them we capture the snapshot
 * id, the snapshot name, the repository name and the repository id (if it exists) once, along with the time at which the snapshot was added
 * to the pending deletion, in a {@link SnapshotDeletionsPending} entry.
 *
 *
 * When cluster state is updated with such entries the {@link org.elasticsearch.snapshots.SnapshotsService} executes corresponding snapshot
 * delete requests to effectively delete the snapshot from the repository. It is possible that the deletion of a snapshot failed for various
 * reason (ex: conflicting snapshot operation, repository removed etc). In such cases the snapshot pending deletion is kept in the cluster
 * state and the deletion will be retried on the next cluster state update. To avoid too many snapshots pending deletion stored in cluster
 * state the number is limited to 500 and configurable through the {@link #MAX_PENDING_DELETIONS_SETTING} setting.
 */
public class SnapshotDeletionsPending extends AbstractNamedDiffable<Custom> implements Custom {

    public static final SnapshotDeletionsPending EMPTY = new SnapshotDeletionsPending(Collections.emptySortedSet());
    public static final String TYPE = "snapshot_deletions_pending";

    /**
     * Setting for the maximum number of snapshots pending deletion allowed in the cluster state.
     * <p>
     * This setting is here to prevent the cluster to grow too large. In the case that the number of snapshots pending deletion exceeds
     * the value of this setting the oldest entries are removed from the cluster state. Snapshots that are discarded are removed before
     * they can be deleted from their repository and are therefore considered as "leaking" and should be logged as such as warnings.
     */
    public static final Setting<Integer> MAX_PENDING_DELETIONS_SETTING = Setting.intSetting(
        "cluster.snapshot.snapshot_deletions_pending.size",
        500,
        Setting.Property.NodeScope
    );

    /**
     * A list of snapshots to delete, sorted by creation time
     */
    private final SortedSet<Entry> entries;

    private SnapshotDeletionsPending(SortedSet<Entry> entries) {
        this.entries = unmodifiableSortedSet(Objects.requireNonNull(entries));
    }

    public SnapshotDeletionsPending(StreamInput in) throws IOException {
        this(new TreeSet<>(in.readSet(Entry::new)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(entries);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public boolean contains(SnapshotId snapshotId) {
        return entries.stream().anyMatch(entry -> Objects.equals(entry.getSnapshotId(), snapshotId));
    }

    public SortedSet<Entry> entries() {
        return entries;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TYPE);
        for (Entry entry : entries) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public SnapshotDeletionsPending withRemovedSnapshots(Set<SnapshotId> snapshotIds) {
        if (snapshotIds == null || snapshotIds.isEmpty()) {
            return this;
        }
        boolean changed = false;
        final SortedSet<Entry> updatedEntries = new TreeSet<>(entries);
        if (updatedEntries.removeIf(entry -> snapshotIds.contains(entry.getSnapshotId()))) {
            changed = true;
        }
        if (changed == false) {
            return this;
        } else if (updatedEntries.isEmpty()) {
            return EMPTY;
        } else {
            return new SnapshotDeletionsPending(updatedEntries);
        }
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("SnapshotDeletionsPending[");
        boolean prepend = true;

        final Iterator<Entry> iterator = entries.stream().iterator();
        while (iterator.hasNext()) {
            if (prepend == false) {
                builder.append(',');
            }
            final Entry entry = iterator.next();
            builder.append('[').append(entry.repositoryName).append('/').append(entry.repositoryUuid).append(']');
            builder.append('[').append(entry.snapshotId).append(',').append(entry.creationTime).append(']');
            builder.append('\n');
            prepend = false;
        }
        builder.append(']');
        return builder.toString();
    }

    public static class Entry implements Writeable, ToXContentObject, Comparable<Entry> {

        private final String repositoryName;
        private final String repositoryUuid;
        private final SnapshotId snapshotId;
        private final long creationTime;

        public Entry(String repositoryName, String repositoryUuid, SnapshotId snapshotId, long creationTime) {
            this.repositoryName = Objects.requireNonNull(repositoryName);
            this.repositoryUuid = Objects.requireNonNull(repositoryUuid);
            this.snapshotId = Objects.requireNonNull(snapshotId);
            this.creationTime = creationTime;
        }

        private Entry(StreamInput in) throws IOException {
            this.repositoryName = in.readString();
            this.repositoryUuid = in.readString();
            this.creationTime = in.readVLong();
            this.snapshotId = new SnapshotId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repositoryName);
            out.writeString(repositoryUuid);
            out.writeVLong(creationTime);
            snapshotId.writeTo(out);
        }

        public String getRepositoryName() {
            return repositoryName;
        }

        public String getRepositoryUuid() {
            return repositoryUuid;
        }

        public SnapshotId getSnapshotId() {
            return snapshotId;
        }

        public long getCreationTime() {
            return creationTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            return creationTime == entry.creationTime
                && Objects.equals(repositoryName, entry.repositoryName)
                && Objects.equals(repositoryUuid, entry.repositoryUuid)
                && Objects.equals(snapshotId, entry.snapshotId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(repositoryName, repositoryUuid, snapshotId, creationTime);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("repository_name", repositoryName);
                builder.field("repository_uuid", repositoryUuid);
                builder.timeField("creation_time_millis", "creation_time", creationTime);
                builder.field("snapshot", snapshotId);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int compareTo(final Entry other) {
            return Comparator.comparingLong(Entry::getCreationTime)
                .reversed()
                .thenComparing(Entry::getSnapshotId)
                .compare(this, other);
        }
    }

    public static final class Builder {

        private final SortedSet<Entry> entries = new TreeSet<>();
        private final Consumer<Entry> consumer;

        public Builder(SnapshotDeletionsPending snapshotDeletionsPending, Consumer<Entry> onLimitExceeded) {
            entries.addAll(snapshotDeletionsPending.entries);
            this.consumer = onLimitExceeded;
        }

        private void ensureLimit(final int maxPendingDeletions) {
            while (entries.size() >= maxPendingDeletions) {
                final Entry removed = entries.last();
                entries.remove(removed);
                if (consumer != null) {
                    consumer.accept(removed);
                }
            }
        }

        public Builder add(String repositoryName, String repositoryUuid, SnapshotId snapshotId, long creationTime) {
            entries.add(new Entry(repositoryName, repositoryUuid, snapshotId, creationTime));
            return this;
        }

        public SnapshotDeletionsPending build(Settings settings) {
            final int maxPendingDeletions = MAX_PENDING_DELETIONS_SETTING.get(settings);
            ensureLimit(maxPendingDeletions);
            assert entries.size() <= maxPendingDeletions : entries.size() + " > " + maxPendingDeletions;
            return entries.isEmpty() == false ? new SnapshotDeletionsPending(entries) : EMPTY;
        }
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }
}
