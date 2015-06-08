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

package org.elasticsearch.index.deletionpolicy;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.name.Named;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShardComponent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Snapshot deletion policy allows to get snapshots of an index state (last commit or all commits)
 * and if the deletion policy is used with all open index writers (JVM level) then the snapshot
 * state will not be deleted until it will be released.
 *
 *
 */
public class SnapshotDeletionPolicy extends AbstractESDeletionPolicy {

    private final IndexDeletionPolicy primary;

    private final ConcurrentMap<Long, SnapshotHolder> snapshots = ConcurrentCollections.newConcurrentMap();

    private volatile List<SnapshotIndexCommit> commits;

    private final Object mutex = new Object();

    private SnapshotIndexCommit lastCommit;

    /**
     * Constructs a new snapshot deletion policy that wraps the provided deletion policy.
     */
    @Inject
    public SnapshotDeletionPolicy(@Named("actual") IndexDeletionPolicy primary) {
        super(((IndexShardComponent) primary).shardId(), ((IndexShardComponent) primary).indexSettings());
        this.primary = primary;
    }

    /**
     * Called by Lucene. Same as {@link #onCommit(java.util.List)}.
     */
    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        if (!commits.isEmpty()) { // this might be empty if we create a new index. 
            // the behavior has changed in Lucene 4.4 that calls onInit even with an empty commits list.
            onCommit(commits);
        }
    }

    /**
     * Called by Lucene.. Wraps the provided commits with {@link SnapshotIndexCommit}
     * and delegates to the wrapped deletion policy.
     */
    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        assert !commits.isEmpty() : "Commits must not be empty";
        synchronized (mutex) {
            List<SnapshotIndexCommit> snapshotCommits = wrapCommits(commits);
            primary.onCommit(snapshotCommits);

            // clean snapshots that their respective counts are 0 (should not really happen)
            for (Iterator<SnapshotHolder> it = snapshots.values().iterator(); it.hasNext(); ) {
                SnapshotHolder holder = it.next();
                if (holder.counter <= 0) {
                    it.remove();
                }
            }
            // build the current commits list (all the ones that are not deleted by the primary)
            List<SnapshotIndexCommit> newCommits = new ArrayList<>();
            for (SnapshotIndexCommit commit : snapshotCommits) {
                if (!commit.isDeleted()) {
                    newCommits.add(commit);
                }
            }
            this.commits = newCommits;
            // the last commit that is not deleted
            this.lastCommit = newCommits.get(newCommits.size() - 1);     
           
        }
    }

    /**
     * Snapshots all the current commits in the index. Make sure to call
     * {@link SnapshotIndexCommits#close()} to release it.
     */
    public SnapshotIndexCommits snapshots() throws IOException {
        synchronized (mutex) {
            if (snapshots == null) {
                throw new IllegalStateException("Snapshot deletion policy has not been init yet...");
            }
            List<SnapshotIndexCommit> result = new ArrayList<>(commits.size());
            for (SnapshotIndexCommit commit : commits) {
                result.add(snapshot(commit));
            }
            return new SnapshotIndexCommits(result);
        }
    }

    /**
     * Returns a snapshot of the index (for the last commit point). Make
     * sure to call {@link SnapshotIndexCommit#close()} in order to release it.
     */
    public SnapshotIndexCommit snapshot() throws IOException {
        synchronized (mutex) {
            if (lastCommit == null) {
                throw new IllegalStateException("Snapshot deletion policy has not been init yet...");
            }
            return snapshot(lastCommit);
        }
    }

    @Override
    public IndexDeletionPolicy clone() {
       // Lucene IW makes a clone internally but since we hold on to this instance 
       // the clone will just be the identity. See InternalEngine recovery why we need this.
       return this;
    }

    /**
     * Helper method to snapshot a give commit.
     */
    private SnapshotIndexCommit snapshot(SnapshotIndexCommit commit) throws IOException {
        SnapshotHolder snapshotHolder = snapshots.get(commit.getGeneration());
        if (snapshotHolder == null) {
            snapshotHolder = new SnapshotHolder(0);
            snapshots.put(commit.getGeneration(), snapshotHolder);
        }
        snapshotHolder.counter++;
        return new OneTimeReleaseSnapshotIndexCommit(this, commit);
    }

    /**
     * Returns <tt>true</tt> if the version has been snapshotted.
     */
    boolean isHeld(long version) {
        SnapshotDeletionPolicy.SnapshotHolder holder = snapshots.get(version);
        return holder != null && holder.counter > 0;
    }

    /**
     * Releases the version provided. Returns <tt>true</tt> if the release was successful.
     */
    boolean close(long version) {
        synchronized (mutex) {
            SnapshotDeletionPolicy.SnapshotHolder holder = snapshots.get(version);
            if (holder == null) {
                return false;
            }
            if (holder.counter <= 0) {
                snapshots.remove(version);
                return false;
            }
            if (--holder.counter == 0) {
                snapshots.remove(version);
            }
            return true;
        }
    }

    /**
     * A class that wraps an {@link SnapshotIndexCommit} and makes sure that release will only
     * be called once on it.
     */
    private static class OneTimeReleaseSnapshotIndexCommit extends SnapshotIndexCommit {
        private volatile boolean released = false;

        OneTimeReleaseSnapshotIndexCommit(SnapshotDeletionPolicy deletionPolicy, IndexCommit cp) throws IOException {
            super(deletionPolicy, cp);
        }

        @Override
        public void close() {
            if (released) {
                return;
            }
            released = true;
            ((SnapshotIndexCommit) delegate).close();
        }
    }

    private static class SnapshotHolder {
        int counter;

        private SnapshotHolder(int counter) {
            this.counter = counter;
        }
    }

    private List<SnapshotIndexCommit> wrapCommits(List<? extends IndexCommit> commits) throws IOException {
        final int count = commits.size();
        List<SnapshotIndexCommit> snapshotCommits = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            snapshotCommits.add(new SnapshotIndexCommit(this, commits.get(i)));
        return snapshotCommits;
    }
}
