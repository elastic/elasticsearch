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
package org.elasticsearch.index.seqno;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

/** a very light weight implementation. will be replaced with proper machinery later */
public class SequenceNumbersService extends AbstractIndexShardComponent {

    public final static long UNASSIGNED_SEQ_NO = -1L;
    final LocalCheckpointService localCheckpointService;

    public SequenceNumbersService(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
        localCheckpointService = new LocalCheckpointService(shardId, indexSettings);
    }

    /**
     * generates a new sequence number.
     * Note: you must call {@link #markSeqNoAsCompleted(long)} after the operation for which this seq# was generated
     * was completed (whether successfully or with a failure)
     */
    public long generateSeqNo() {
        return localCheckpointService.generateSeqNo();
    }

    /**
     * marks the given seqNo as completed. See {@link LocalCheckpointService#markSeqNoAsCompleted(long)}
     * more details
     */
    public void markSeqNoAsCompleted(long seqNo) {
        localCheckpointService.markSeqNoAsCompleted(seqNo);
    }

    /**
     * Gets sequence number related stats
     */
    public SeqNoStats stats() {
        return new SeqNoStats(localCheckpointService.getMaxSeqNo(), localCheckpointService.getCheckpoint());
    }
}
