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

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class contains utility methods for mutating the shard lucene index and translog as a preparation to be opened.
 */
public abstract class EngineDiskUtils {

    /**
     * creates an empty lucene index and a corresponding empty translog. Any existing data will be deleted.
     */
    public static void createEmpty(final Directory dir, final Path translogPath, final ShardId shardId) throws IOException {
        try (IndexWriter writer = newIndexWriter(true, dir, null)) {
            final String translogUuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId);
            final Map<String, String> map = new HashMap<>();
            map.put(Translog.TRANSLOG_GENERATION_KEY, "1");
            map.put(Translog.TRANSLOG_UUID_KEY, translogUuid);
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
            updateCommitData(writer, map);
        }
    }

    /**
     * Keeping existing unsafe commits when opening an engine can be problematic because these commits are not safe
     * at the recovering time but they can suddenly become safe in the future.
     * The following issues can happen if unsafe commits are kept oninit.
     * <p>
     * 1. Replica can use unsafe commit in peer-recovery. This happens when a replica with a safe commit c1(max_seqno=1)
     * and an unsafe commit c2(max_seqno=2) recovers from a primary with c1(max_seqno=1). If a new document(seqno=2)
     * is added without flushing, the global checkpoint is advanced to 2; and the replica recovers again, it will use
     * the unsafe commit c2(max_seqno=2 at most gcp=2) as the starting commit for sequenced-based recovery even the
     * commit c2 contains a stale operation and the document(with seqno=2) will not be replicated to the replica.
     * <p>
     * 2. Min translog gen for recovery can go backwards in peer-recovery. This happens when are replica with a safe commit
     * c1(local_checkpoint=1, recovery_translog_gen=1) and an unsafe commit c2(local_checkpoint=2, recovery_translog_gen=2).
     * The replica recovers from a primary, and keeps c2 as the last commit, then sets last_translog_gen to 2. Flushing a new
     * commit on the replica will cause exception as the new last commit c3 will have recovery_translog_gen=1. The recovery
     * translog generation of a commit is calculated based on the current local checkpoint. The local checkpoint of c3 is 1
     * while the local checkpoint of c2 is 2.
     * <p>
     * 3. Commit without translog can be used in recovery. An old index, which was created before multiple-commits is introduced
     * (v6.2), may not have a safe commit. If that index has a snapshotted commit without translog and an unsafe commit,
     * the policy can consider the snapshotted commit as a safe commit for recovery even the commit does not have translog.
     */
    public static void trimUnsafeCommits(final Directory dir, final Path translogPath,
                                         final Version indexVersionCreated) throws IOException {
        final List<IndexCommit> existingCommits = DirectoryReader.listCommits(dir);
        if (existingCommits.size() == 0) {
            throw new IllegalArgumentException("No index found to trim");
        }
        final String translogUUID = existingCommits.get(existingCommits.size()-1).getUserData().get(Translog.TRANSLOG_UUID_KEY);
        final IndexCommit startingIndexCommit;
        final long lastSyncedGlobalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
        final long minRetainedTranslogGen = Translog.readMinTranslogGeneration(translogPath, translogUUID);
        // We may not have a safe commit if an index was create before v6.2; and if there is a snapshotted commit whose translog
        // are not retained but max_seqno is at most the global checkpoint, we may mistakenly select it as a starting commit.
        // To avoid this issue, we only select index commits whose translog are fully retained.
        if (indexVersionCreated.before(Version.V_6_2_0)) {
            final List<IndexCommit> recoverableCommits = new ArrayList<>();
            for (IndexCommit commit : existingCommits) {
                if (minRetainedTranslogGen <= Long.parseLong(commit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY))) {
                    recoverableCommits.add(commit);
                }
            }
            assert recoverableCommits.isEmpty() == false : "No commit point with translog found; " +
                "commits [" + existingCommits + "], minRetainedTranslogGen [" + minRetainedTranslogGen + "]";
            startingIndexCommit = CombinedDeletionPolicy.findSafeCommitPoint(recoverableCommits, lastSyncedGlobalCheckpoint);
        } else {
            // TODO: Asserts the starting commit is a safe commit once peer-recovery sets global checkpoint.
            startingIndexCommit = CombinedDeletionPolicy.findSafeCommitPoint(existingCommits, lastSyncedGlobalCheckpoint);
        }

        if (translogUUID.equals(startingIndexCommit.getUserData().get(Translog.TRANSLOG_UUID_KEY)) == false) {
            throw new IllegalStateException("starting commit translog uuid ["
                + startingIndexCommit.getUserData().get(Translog.TRANSLOG_UUID_KEY) + "] is not equal to last commit's translog uuid ["
                + translogUUID + "]");
        }
        if (startingIndexCommit.equals(existingCommits.get(existingCommits.size() - 1)) == false) {
            try (IndexWriter writer = newIndexWriter(false, dir, startingIndexCommit)) {
                // this achieves two things:
                // - by committing a new commit based on the starting commit, it make sure the starting commit will be opened
                // - deletes any other commit (by lucene standard deletion policy)
                //
                // note that we can't just use IndexCommit.delete() as we really want to make sure that those files won't be used
                // even if a virus scanner causes the files not to be used.

                // TODO: speak to @s1monw about the fact that we can' use  getUserData(writer) as that uses that last's commit user
                // data rather then the starting commit.
                writer.setLiveCommitData(startingIndexCommit.getUserData().entrySet());
                writer.commit();
            }
        }
    }


    /**
     * Converts an existing lucene index and marks it with a new history uuid. Also creates a new empty translog file.
     * This is used to make sure no existing shard will recovery from this index using ops based recovery.
     */
    public static void bootstrapNewHistoryFromLuceneIndex(final Directory dir, final Path translogPath, final ShardId shardId)
        throws IOException {
        assert DirectoryReader.listCommits(dir).size() == 1 : "bootstrapping a new history from an index with multiple commits";
        try (IndexWriter writer = newIndexWriter(false, dir, null)) {
            final Map<String, String> userData = getUserData(writer);
            final long maxSeqNo = Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO));
            final String translogUuid = Translog.createEmptyTranslog(translogPath, maxSeqNo, shardId);
            final Map<String, String> map = new HashMap<>();
            map.put(Translog.TRANSLOG_GENERATION_KEY, "1");
            map.put(Translog.TRANSLOG_UUID_KEY, translogUuid);
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo));
            updateCommitData(writer, map);
        }
    }

    /**
     * Creates a new empty translog and associates it with an existing lucene index.
     */
    public static void createNewTranslog(final Directory dir, final Path translogPath, long initialGlobalCheckpoint, final ShardId shardId)
        throws IOException {
        if (Assertions.ENABLED) {
            final List<IndexCommit> existingCommits = DirectoryReader.listCommits(dir);
            assert existingCommits.size() == 1 : "creating a translog translog should have one commit, commits[" + existingCommits + "]";
            SequenceNumbers.CommitInfo commitInfo = Store.loadSeqNoInfo(existingCommits.get(0));
            assert commitInfo.localCheckpoint >= initialGlobalCheckpoint :
                "trying to create a shard whose local checkpoint [" + commitInfo.localCheckpoint + "] is < global checkpoint ["
                + initialGlobalCheckpoint + "]";
        }

        try (IndexWriter writer = newIndexWriter(false, dir, null)) {
            final String translogUuid = Translog.createEmptyTranslog(translogPath, initialGlobalCheckpoint, shardId);
            final Map<String, String> map = new HashMap<>();
            map.put(Translog.TRANSLOG_GENERATION_KEY, "1");
            map.put(Translog.TRANSLOG_UUID_KEY, translogUuid);
            updateCommitData(writer, map);
        }
    }


    /**
     * Checks that the Lucene index contains a history uuid marker. If not, a new one is generated and committed.
     */
    public static void ensureIndexHasHistoryUUID(final Directory dir) throws IOException {
        assert DirectoryReader.listCommits(dir).size() == 1 : "can't ensure index history with multiple commits";
        try (IndexWriter writer = newIndexWriter(false, dir, null)) {
            final Map<String, String> userData = getUserData(writer);
            if (userData.containsKey(Engine.HISTORY_UUID_KEY) == false) {
                updateCommitData(writer, Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()));
            }
        }
    }

    private static void updateCommitData(IndexWriter writer, Map<String, String> keysToUpdate) throws IOException {
        final Map<String, String> userData = getUserData(writer);
        userData.putAll(keysToUpdate);
        writer.setLiveCommitData(userData.entrySet());
        writer.commit();
    }

    private static Map<String, String> getUserData(IndexWriter writer) {
        final Map<String, String> userData = new HashMap<>();
        writer.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));
        return userData;
    }

    private static IndexWriter newIndexWriter(final boolean create, final Directory dir, final IndexCommit commit) throws IOException {
        assert create == false || commit == null : "can't specify create flag with a commit";
        IndexWriterConfig iwc = new IndexWriterConfig(null)
            .setCommitOnClose(false)
            .setIndexCommit(commit)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);

        return new IndexWriter(dir, iwc);
    }
}
