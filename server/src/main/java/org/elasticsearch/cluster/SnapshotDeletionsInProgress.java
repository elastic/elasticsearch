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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

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
    }

    public static SnapshotDeletionsInProgress of(List<SnapshotDeletionsInProgress.Entry> entries) {
        if (entries.isEmpty()) {
            return EMPTY;
        }
        return new SnapshotDeletionsInProgress(Collections.unmodifiableList(entries));
    }

    public SnapshotDeletionsInProgress(StreamInput in) throws IOException {
        this.entries = Collections.unmodifiableList(in.readList(Entry::new));
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
     * Returns a new instance of {@link SnapshotDeletionsInProgress} which removes
     * the given entry from the invoking instance.
     */
    public SnapshotDeletionsInProgress withRemovedEntry(Entry entry) {
        List<Entry> entries = new ArrayList<>(getEntries());
        entries.remove(entry);
        return SnapshotDeletionsInProgress.of(entries);
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
        private final long startTime;
        private final long repositoryStateId;

        public Entry(List<SnapshotId> snapshots, String repoName, long startTime, long repositoryStateId) {
            this.snapshots = snapshots;
            assert snapshots.size() == new HashSet<>(snapshots).size() : "Duplicate snapshot ids in " + snapshots;
            this.repoName = repoName;
            this.startTime = startTime;
            this.repositoryStateId = repositoryStateId;
            assert repositoryStateId > RepositoryData.EMPTY_REPO_GEN :
                "Can't delete based on an empty or unknown repository generation but saw [" + repositoryStateId + "]";
        }

        public Entry(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(SnapshotsService.MULTI_DELETE_VERSION)) {
                this.repoName = in.readString();
                this.snapshots = in.readList(SnapshotId::new);
            } else {
                final Snapshot snapshot = new Snapshot(in);
                this.snapshots = Collections.singletonList(snapshot.getSnapshotId());
                this.repoName = snapshot.getRepository();
            }
            this.startTime = in.readVLong();
            this.repositoryStateId = in.readLong();
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
                       && repositoryStateId == that.repositoryStateId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshots, repoName, startTime, repositoryStateId);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(SnapshotsService.MULTI_DELETE_VERSION)) {
                out.writeString(repoName);
                out.writeCollection(snapshots);
            } else {
                assert snapshots.size() == 1 : "Only single deletion allowed in mixed version cluster containing [" + out.getVersion() +
                        "] but saw " + snapshots;
                new Snapshot(repoName, snapshots.get(0)).writeTo(out);
            }
            out.writeVLong(startTime);
            out.writeLong(repositoryStateId);
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
}
