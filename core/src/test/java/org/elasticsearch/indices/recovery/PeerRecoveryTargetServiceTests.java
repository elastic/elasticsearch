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

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogWriter;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class PeerRecoveryTargetServiceTests extends IndexShardTestCase {

    public void testGetStartingSeqNo() throws Exception {
        IndexShard replica = newShard(false);
        final AtomicReference<Path> translogLocation = new AtomicReference<>();
        RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null) {
            @Override
            Path translogLocation() {
                return translogLocation.get();
            }
        };
        try {
            recoveryEmptyReplica(replica);
            int docs = randomIntBetween(1, 10);
            final String index = replica.shardId().getIndexName();
            long seqNo = 0;
            for (int i = 0; i < docs; i++) {
                replica.applyIndexOperationOnReplica(seqNo++, 1, VersionType.EXTERNAL,
                    IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false,
                    SourceToParse.source(index, "type", "doc_" + i, new BytesArray("{}"), XContentType.JSON),
                    update -> {});
                if (rarely()) {
                    // insert a gap
                    seqNo++;
                }
            }

            final long maxSeqNo = replica.seqNoStats().getMaxSeqNo();
            final long localCheckpoint = replica.getLocalCheckpoint();

            translogLocation.set(replica.getTranslog().location());

            final Translog translog = replica.getTranslog();
            final String translogUUID = translog.getTranslogUUID();
            assertThat(PeerRecoveryTargetService.getStartingSeqNo(recoveryTarget), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));

            translogLocation.set(writeTranslog(replica.shardId(), translogUUID, translog.currentFileGeneration(), maxSeqNo - 1));

            // commit is good, global checkpoint is at least max *committed* which is NO_OPS_PERFORMED
            assertThat(PeerRecoveryTargetService.getStartingSeqNo(recoveryTarget), equalTo(0L));

            replica.flush(new FlushRequest());

            translogLocation.set(replica.getTranslog().location());

            // commit is not good, global checkpoint is below max
            assertThat(PeerRecoveryTargetService.getStartingSeqNo(recoveryTarget), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));

            translogLocation.set(writeTranslog(replica.shardId(), translogUUID, translog.currentFileGeneration(), maxSeqNo));

            // commit is good, global checkpoint is above max
            assertThat(PeerRecoveryTargetService.getStartingSeqNo(recoveryTarget), equalTo(localCheckpoint + 1));
        } finally {
            closeShards(replica);
            recoveryTarget.decRef();
        }
    }

    private Path writeTranslog(
            final ShardId shardId,
            final String translogUUID,
            final long generation,
            final long globalCheckpoint
            ) throws IOException {
        final Path tempDir = createTempDir();
        final Path resolve = tempDir.resolve(Translog.getFilename(generation));
        Files.createFile(tempDir.resolve(Translog.CHECKPOINT_FILE_NAME));
        try (TranslogWriter ignored = TranslogWriter.create(
                shardId,
                translogUUID,
                generation,
                resolve,
                FileChannel::open,
                TranslogConfig.DEFAULT_BUFFER_SIZE, () -> globalCheckpoint, generation, () -> generation)) {}
        return tempDir;
    }

}
