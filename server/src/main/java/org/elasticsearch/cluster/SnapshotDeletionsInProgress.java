/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A class that represents the snapshot deletions that are in progress in the cluster.
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
        this(in.readList(Entry::new));
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

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} with the given
     * {@link Entry} added.
     */
    public static SnapshotDeletionsInProgress newInstance(Entry entry) {
        return new SnapshotDeletionsInProgress(Collections.singletonList(entry));
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} which adds
     * the given {@link Entry} to the invoking instance.
     */
    public SnapshotDeletionsInProgress withAddedEntry(Entry entry) {
        List<Entry> entries = new ArrayList<>(getEntries());
        entries.add(entry);
        return SnapshotDeletionsInProgress.of(entries);
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
        out.writeList(entries);
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TYPE);
        for (Entry entry : entries) {
            builder.startObject();
            {
                builder.field("repository", entry.repository());
                builder.startArray("snapshots");
                for (SnapshotId snapshot : entry.snapshots) {
                    builder.value(snapshot.getName());
                }
                builder.endArray();
                builder.humanReadableField("start_time_millis", "start_time", new TimeValue(entry.startTime));
                builder.field("repository_state_id", entry.repositoryStateId);
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("SnapshotDeletionsInProgress[");
        for (int i = 0; i < entries.size(); i++) {
            builder.append(entries.get(i).getSnapshots());
            if (i + 1 < entries.size()) {
                builder.append(",");
            }
        }
        return builder.append("]").toString();
    }

    /**
     * A class representing a snapshot deletion request entry in the cluster state.
     */
    public static final class Entry implements Writeable, RepositoryOperation {
        private final List<SnapshotId> snapshots;
        private final String repoName;
        private final State state;
        private final long startTime;
        private final long repositoryStateId;
        private final String uuid;

        public Entry(List<SnapshotId> snapshots, String repoName, long startTime, long repositoryStateId, State state) {
            this(snapshots, repoName, startTime, repositoryStateId, state, UUIDs.randomBase64UUID());
        }

        private Entry(List<SnapshotId> snapshots, String repoName, long startTime, long repositoryStateId, State state, String uuid) {
            this.snapshots = snapshots;
            assert snapshots.size() == new HashSet<>(snapshots).size() : "Duplicate snapshot ids in " + snapshots;
            this.repoName = repoName;
            this.startTime = startTime;
            this.repositoryStateId = repositoryStateId;
            this.state = state;
            this.uuid = uuid;
        }

        public Entry(StreamInput in) throws IOException {
            this.repoName = in.readString();
            this.snapshots = in.readList(SnapshotId::new);
            this.startTime = in.readVLong();
            this.repositoryStateId = in.readLong();
            if (in.getVersion().onOrAfter(SnapshotsService.FULL_CONCURRENCY_VERSION)) {
                this.state = State.readFrom(in);
                this.uuid = in.readString();
            } else {
                this.state = State.STARTED;
                this.uuid = IndexMetadata.INDEX_UUID_NA_VALUE;
            }
        }

        public Entry started() {
            assert state == State.WAITING;
            return new Entry(snapshots, repository(), startTime, repositoryStateId, State.STARTED, uuid);
        }

        public Entry withAddedSnapshots(Collection<SnapshotId> newSnapshots) {
            assert state == State.WAITING;
            final Collection<SnapshotId> updatedSnapshots = new HashSet<>(snapshots);
            if (updatedSnapshots.addAll(newSnapshots) == false) {
                return this;
            }
            return new Entry(List.copyOf(updatedSnapshots), repository(), startTime, repositoryStateId, State.WAITING, uuid);
        }

        public Entry withSnapshots(Collection<SnapshotId> snapshots) {
            return new Entry(List.copyOf(snapshots), repository(), startTime, repositoryStateId, state, uuid);
        }

        public Entry withRepoGen(long repoGen) {
            return new Entry(snapshots, repository(), startTime, repoGen, state, uuid);
        }

        public State state() {
            return state;
        }

        public String uuid() {
            return uuid;
        }

        public List<SnapshotId> getSnapshots() {
            return snapshots;
        }

        /**
         * The start time in milliseconds for deleting the snapshots.
         */
        public long getStartTime() {
            return startTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Entry that = (Entry) o;
            return repoName.equals(that.repoName)
                       && snapshots.equals(that.snapshots)
                       && startTime == that.startTime
                       && repositoryStateId == that.repositoryStateId
                       && state == that.state
                       && uuid.equals(that.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshots, repoName, startTime, repositoryStateId, state, uuid);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repoName);
            out.writeCollection(snapshots);
            out.writeVLong(startTime);
            out.writeLong(repositoryStateId);
            if (out.getVersion().onOrAfter(SnapshotsService.FULL_CONCURRENCY_VERSION)) {
                state.writeTo(out);
                out.writeString(uuid);
            }
        }

        @Override
        public String repository() {
            return repoName;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }

        @Override
        public String toString() {
            return "SnapshotDeletionsInProgress.Entry[[" + uuid + "][" + state + "]" + snapshots + "]";
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
            switch (value) {
                case 0:
                    return WAITING;
                case 1:
                    return STARTED;
                default:
                    throw new IllegalArgumentException("No snapshot delete state for value [" + value + "]");
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(value);
        }
    }
}
