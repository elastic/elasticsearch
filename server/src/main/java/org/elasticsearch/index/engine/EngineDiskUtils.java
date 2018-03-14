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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
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
        try (IndexWriter writer = newIndexWriter(true, dir)) {
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
     * Converts an existing lucene index and marks it with a new history uuid. Also creates a new empty translog file.
     * This is used to make sure no existing shard will recovery from this index using ops based recovery.
     */
    public static void bootstrapNewHistoryFromLuceneIndex(final Directory dir, final Path translogPath, final ShardId shardId)
        throws IOException {
        try (IndexWriter writer = newIndexWriter(false, dir)) {
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

        try (IndexWriter writer = newIndexWriter(false, dir)) {
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
        try (IndexWriter writer = newIndexWriter(false, dir)) {
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

    private static IndexWriter newIndexWriter(final boolean create, final Directory dir) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(null)
            .setCommitOnClose(false)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
        return new IndexWriter(dir, iwc);
    }
}
