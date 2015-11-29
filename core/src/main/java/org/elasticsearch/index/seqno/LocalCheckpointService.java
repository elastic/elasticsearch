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

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

public class LocalCheckpointService extends AbstractIndexShardComponent {

    public static String SETTINGS_INDEX_LAG_THRESHOLD = "index.seq_no.index_lag.threshold";
    public static String SETTINGS_INDEX_LAG_MAX_WAIT = "index.seq_no.index_lag.max_wait";
    final static int DEFAULT_INDEX_LAG_THRESHOLD = 1024;
    final static TimeValue DEFAULT_INDEX_LAG_MAX_WAIT = TimeValue.timeValueSeconds(30);

    final Object mutex = new Object();
    final FixedBitSet processedSeqNo;
    final int indexLagThreshold;
    final TimeValue indexLagMaxWait;


    volatile long nextSeqNo = 0;
    volatile long checkpoint = -1;

    public LocalCheckpointService(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
        indexLagThreshold = indexSettings.getSettings().getAsInt(SETTINGS_INDEX_LAG_THRESHOLD, DEFAULT_INDEX_LAG_THRESHOLD);
        indexLagMaxWait = indexSettings.getSettings().getAsTime(SETTINGS_INDEX_LAG_MAX_WAIT, DEFAULT_INDEX_LAG_MAX_WAIT);
        processedSeqNo = new FixedBitSet(indexLagThreshold);

    }

    public long generateSeqNo() {
        synchronized (mutex) {
            // we have to keep checking when ensure capacity returns because it release the lock and nextSeqNo may change
            while (hasCapacity(nextSeqNo) == false) {
                ensureCapacity(nextSeqNo);
            }
            return nextSeqNo++;
        }
    }

    public long markSeqNoAsCompleted(long seqNo) {
        synchronized (mutex) {
            // make sure we track highest seen seqNo
            if (seqNo >= nextSeqNo) {
                nextSeqNo = seqNo + 1;
            }
            if (seqNo <= checkpoint) {
                // this is possible during recover where we might replay an op that was also replicated
                return checkpoint;
            }
            // just to be safe (previous calls to generateSeqNo/markSeqNoAsStarted should ensure this is OK)
            ensureCapacity(seqNo);
            int offset = seqNoToOffset(seqNo);
            processedSeqNo.set(offset);
            if (seqNo == checkpoint + 1) {
                do {
                    // clear the flag as we are making it free for future operations. do se before we expose it
                    // by moving the checkpoint
                    processedSeqNo.clear(offset);
                    checkpoint++;
                    offset = seqNoToOffset(checkpoint + 1);
                } while (processedSeqNo.get(offset));
                mutex.notifyAll();
            }
        }
        return checkpoint;
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public long getMaxSeqNo() {
        return nextSeqNo - 1;
    }


    private boolean hasCapacity(long seqNo) {
        assert Thread.holdsLock(mutex);
        return (seqNo - checkpoint) <= indexLagThreshold;
    }

    private void ensureCapacity(long seqNo) {
        assert Thread.holdsLock(mutex);
        long retry = 0;
        final long maxRetries = indexLagMaxWait.seconds();
        while (hasCapacity(seqNo) == false) {
            try {
                if (retry > maxRetries) {
                    ElasticsearchException e = new EsRejectedExecutionException("indexing lag exceeds [{}] (seq# requested [{}], local checkpoint [{}]",
                            indexLagThreshold, seqNo, checkpoint);
                    e.setShard(shardId());
                    throw e;
                }

                // this temporary releases the lock on mutex
                mutex.wait(Math.min(1000, indexLagMaxWait.millis() - retry * 1000));
                retry++;
            } catch (InterruptedException ie) {
                ElasticsearchException exp = new ElasticsearchException("interrupted while waiting on index lag");
                exp.setShard(shardId());
                throw exp;
            }
        }
    }

    private int seqNoToOffset(long seqNo) {
        assert seqNo - checkpoint <= indexLagThreshold;
        assert seqNo > checkpoint;
        return (int) (seqNo % indexLagThreshold);
    }

}
