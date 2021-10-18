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
 */
public class SnapshotDeletionsPending extends AbstractNamedDiffable<Custom> implements Custom {

    public static final SnapshotDeletionsPending EMPTY = new SnapshotDeletionsPending(Collections.emptySortedSet());
    public static final String TYPE = "snapshot_deletions_pending";

    public static final int MAX_PENDING_DELETIONS = 500;

    /**
     * A list of snapshots to delete, sorted by creation time
     */
    private final SortedSet<Entry> entries;

    private SnapshotDeletionsPending(SortedSet<Entry> entries) {
        this.entries = unmodifiableSortedSet(Objects.requireNonNull(entries));
        assert entries.size() <= MAX_PENDING_DELETIONS : entries.size() + " > " + MAX_PENDING_DELETIONS;
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

        private void ensureLimit() {
            while (entries.size() >= MAX_PENDING_DELETIONS) {
                final Entry removed = entries.last();
                entries.remove(removed);
                if (consumer != null) {
                    consumer.accept(removed);
                }
            }
        }

        public Builder add(String repositoryName, String repositoryUuid, SnapshotId snapshotId, long creationTime) {
            ensureLimit();
            entries.add(new Entry(repositoryName, repositoryUuid, snapshotId, creationTime));
            return this;
        }

        public SnapshotDeletionsPending build() {
            ensureLimit();
            return entries.isEmpty() == false ? new SnapshotDeletionsPending(entries) : EMPTY;
        }
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }
}
