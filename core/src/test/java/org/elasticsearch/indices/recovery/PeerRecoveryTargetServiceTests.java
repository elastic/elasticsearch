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

import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;

import static org.hamcrest.Matchers.equalTo;

public class PeerRecoveryTargetServiceTests extends IndexShardTestCase {

    public void testGetStartingSeqNo() throws Exception {
        final IndexShard replica = newShard(false);
        try {
            // Empty store
            {
                recoveryEmptyReplica(replica);
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(recoveryTarget), equalTo(0L));
                recoveryTarget.decRef();
            }
            // Last commit is good - use it.
            final long initDocs = scaledRandomIntBetween(1, 10);
            {
                for (int i = 0; i < initDocs; i++) {
                    indexDoc(replica, "doc", Integer.toString(i));
                    replica.updateGlobalCheckpointOnReplica(i, "test");
                }
                flushShard(replica);
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(recoveryTarget), equalTo(initDocs));
                recoveryTarget.decRef();
            }
            // Last commit is not good - use the previous commit
            {
                int moreDocs = randomIntBetween(1, 10);
                for (int i = 0; i < moreDocs; i++) {
                    indexDoc(replica, "doc", Long.toString(i));
                    if (rarely()) {
                        getEngine(replica).getLocalCheckpointTracker().generateSeqNo(); // Create a gap in seqno.
                    }
                }
                flushShard(replica);
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(recoveryTarget), equalTo(initDocs));
                recoveryTarget.decRef();
            }
        } finally {
            closeShards(replica);
        }
    }
}
