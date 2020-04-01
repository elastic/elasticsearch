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
import java.util.List;
import java.util.Objects;

/**
 * A class that represents the snapshot deletions that are in progress in the cluster.
 */
public class SnapshotDeletionsInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final String TYPE = "snapshot_deletions";

    // the list of snapshot deletion request entries
    private final List<Entry> entries;

    public SnapshotDeletionsInProgress(List<Entry> entries) {
        this.entries = Collections.unmodifiableList(entries);
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
        return new SnapshotDeletionsInProgress(entries);
    }

    /**
     * Returns a new instance of {@link SnapshotDeletionsInProgress} which removes
     * the given entry from the invoking instance.
     */
    public SnapshotDeletionsInProgress withRemovedEntry(Entry entry) {
        List<Entry> entries = new ArrayList<>(getEntries());
        entries.remove(entry);
        return new SnapshotDeletionsInProgress(entries);
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
                builder.field("snapshot", entry.getPattern());
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
            builder.append(entries.get(i).getPattern());
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
        private final List<SnapshotId> snapshotIds;
        private final String repo;
        private final String pattern;
        private final long startTime;
        private final long repositoryStateId;

        public Entry(String repo, String pattern, long startTime) {
            this.repo = repo;
            this.pattern = pattern;
            this.startTime = startTime;
            this.snapshotIds = Collections.emptyList();
            this.repositoryStateId = RepositoryData.UNKNOWN_REPO_GEN;
        }

        public Entry(Snapshot snapshot, long startTime, long repositoryStateId) {
            this.snapshotIds = Collections.singletonList(snapshot.getSnapshotId());
            this.repo = snapshot.getRepository();
            this.pattern = snapshot.getSnapshotId().getName();
            this.startTime = startTime;
            this.repositoryStateId = repositoryStateId;
        }

        public Entry(Entry entry, long repositoryStateId) {
            this.snapshotIds = entry.snapshotIds;
            this.repo = entry.repo;
            this.pattern = entry.pattern;
            this.startTime = entry.startTime;
            this.repositoryStateId = repositoryStateId;
        }

        public Entry(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(SnapshotsService.TWO_STEP_DELETE_VERSION)) {
                this.repo = in.readString();
                this.pattern = in.readString();
                this. snapshotIds = in.readList(SnapshotId::new);
            } else {
                final Snapshot snapshot = new Snapshot(in);
                this.snapshotIds = Collections.singletonList(snapshot.getSnapshotId());
                this.repo = snapshot.getRepository();
                this.pattern = snapshot.getSnapshotId().getName();
            }
            this.startTime = in.readVLong();
            this.repositoryStateId = in.readLong();
        }

        public String getPattern() {
            return pattern;
        }

        public boolean matches(Snapshot snapshot) {
            if (repo.equals(snapshot.getRepository()) == false) {
                return false;
            }
            if (repositoryStateId == RepositoryData.UNKNOWN_REPO_GEN) {
                return pattern.equals(snapshot.getSnapshotId().getName());
            }
            return snapshotIds.contains(snapshot.getSnapshotId());
        }

        public List<SnapshotId> getSnapshotIds() {
            return snapshotIds;
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
            return pattern.equals(that.pattern)
                       && snapshotIds.equals(that.snapshotIds)
                       && startTime == that.startTime
                       && repositoryStateId == that.repositoryStateId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern, snapshotIds, startTime, repositoryStateId);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(SnapshotsService.TWO_STEP_DELETE_VERSION)) {
                out.writeString(repo);
                out.writeString(pattern);
                out.writeList(snapshotIds);
            } else {
                new Snapshot(repo, snapshotIds.get(0)).writeTo(out);
            }
            out.writeVLong(startTime);
            out.writeLong(repositoryStateId);
        }

        @Override
        public String repository() {
            return repo;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }

        @Override
        public String toString() {
            return "Entry[repository=" + repo + ", repositoryStateId=" + repositoryStateId + ", snapshotIds="
                + snapshotIds + ", pattern=" + pattern + "]";
        }
    }
}
