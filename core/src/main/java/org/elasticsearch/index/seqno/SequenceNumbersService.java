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

import java.util.concurrent.atomic.AtomicLong;

/** a very light weight implementation. will be replaced with proper machinery later */
public class SequenceNumbersService extends AbstractIndexShardComponent {

    public final static long UNASSIGNED_SEQ_NO = -1L;

    AtomicLong seqNoGenerator = new AtomicLong();

    public SequenceNumbersService(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
    }

    /**
     * generates a new sequence number.
     * Note: you must call {@link #markSeqNoAsCompleted(long)} after the operation for which this seq# was generated
     * was completed (whether successfully or with a failure
     */
    public long generateSeqNo() {
        return seqNoGenerator.getAndIncrement();
    }

    public void markSeqNoAsCompleted(long seqNo) {
        // this is temporary to make things semi sane on primary promotion and recovery. will be replaced with better machinery
        boolean success;
        do {
            long maxSeqNo = seqNoGenerator.get();
            if (seqNo > maxSeqNo) {
                success = seqNoGenerator.compareAndSet(maxSeqNo, seqNo);
            } else {
                success = true;
            }
        } while (success == false);
    }

}
