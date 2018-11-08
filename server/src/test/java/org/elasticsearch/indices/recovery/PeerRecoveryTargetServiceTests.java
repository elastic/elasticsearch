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

package org.elasticsearch.indices.recovery;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.Translog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PeerRecoveryTargetServiceTests extends IndexShardTestCase {

    public void testGetStartingSeqNo() throws Exception {
        final IndexShard replica = newShard(false);
        try {
            // Empty store
            {
                recoveryEmptyReplica(replica, true);
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(0L));
                recoveryTarget.decRef();
            }
            // Last commit is good - use it.
            final long initDocs = scaledRandomIntBetween(1, 10);
            {
                for (int i = 0; i < initDocs; i++) {
                    indexDoc(replica, "_doc", Integer.toString(i));
                    if (randomBoolean()) {
                        flushShard(replica);
                    }
                }
                flushShard(replica);
                replica.updateGlobalCheckpointOnReplica(initDocs - 1, "test");
                replica.sync();
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(initDocs));
                recoveryTarget.decRef();
            }
            // Global checkpoint does not advance, last commit is not good - use the previous commit
            final int moreDocs = randomIntBetween(1, 10);
            {
                for (int i = 0; i < moreDocs; i++) {
                    indexDoc(replica, "_doc", Long.toString(i));
                    if (randomBoolean()) {
                        flushShard(replica);
                    }
                }
                flushShard(replica);
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(initDocs));
                recoveryTarget.decRef();
            }
            // Advances the global checkpoint, a safe commit also advances
            {
                replica.updateGlobalCheckpointOnReplica(initDocs + moreDocs - 1, "test");
                replica.sync();
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(initDocs + moreDocs));
                recoveryTarget.decRef();
            }
            // Different translogUUID, fallback to file-based
            {
                replica.close("test", false);
                final List<IndexCommit> commits = DirectoryReader.listCommits(replica.store().directory());
                IndexWriterConfig iwc = new IndexWriterConfig(null)
                    .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                    .setCommitOnClose(false)
                    .setMergePolicy(NoMergePolicy.INSTANCE)
                    .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
                try (IndexWriter writer = new IndexWriter(replica.store().directory(), iwc)) {
                    final Map<String, String> userData = new HashMap<>(commits.get(commits.size() - 1).getUserData());
                    userData.put(Translog.TRANSLOG_UUID_KEY, UUIDs.randomBase64UUID());
                    writer.setLiveCommitData(userData.entrySet());
                    writer.commit();
                }
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
                recoveryTarget.decRef();
            }
        } finally {
            closeShards(replica);
        }
    }
}
