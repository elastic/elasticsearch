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

/**
 * This class generates sequences numbers and keeps track of the so called local checkpoint - the highest number for which
 * all previous seqNo have been processed (including)
 */
public class LocalCheckpointService extends AbstractIndexShardComponent {

    /** sets the maximum spread between lowest and highest seq no in flight */
    public static String SETTINGS_INDEX_LAG_THRESHOLD = "index.seq_no.index_lag.threshold";

    /**
     * how long should an incoming indexing request which violates {@link #SETTINGS_INDEX_LAG_THRESHOLD } should wait
     * before being rejected
     */
    public static String SETTINGS_INDEX_LAG_MAX_WAIT = "index.seq_no.index_lag.max_wait";

    /** default value for {@link #SETTINGS_INDEX_LAG_THRESHOLD} */
    final static int DEFAULT_INDEX_LAG_THRESHOLD = 1024;

    /** default value for {@link #SETTINGS_INDEX_LAG_MAX_WAIT} */
    final static TimeValue DEFAULT_INDEX_LAG_MAX_WAIT = TimeValue.timeValueSeconds(30);

    /** protects changes to all internal state and signals changes in {@link #checkpoint} */
    final Object mutex = new Object();

    /** each bits maps to a seqNo in round robin fashion. a set bit means the seqNo has been processed */
    final FixedBitSet processedSeqNo;

    /** value of {@link #SETTINGS_INDEX_LAG_THRESHOLD } */
    final int indexLagThreshold;
    /** value of {#link #SETTINGS_INDEX_LAG_THRESHOLD } */
    final TimeValue indexLagMaxWait;


    /** the next available seqNo - used for seqNo generation */
    volatile long nextSeqNo = 0;

    /** the current local checkpoint, i.e., all seqNo lower<= this number have been completed */
    volatile long checkpoint = -1;

    public LocalCheckpointService(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
        indexLagThreshold = indexSettings.getSettings().getAsInt(SETTINGS_INDEX_LAG_THRESHOLD, DEFAULT_INDEX_LAG_THRESHOLD);
        indexLagMaxWait = indexSettings.getSettings().getAsTime(SETTINGS_INDEX_LAG_MAX_WAIT, DEFAULT_INDEX_LAG_MAX_WAIT);
        processedSeqNo = new FixedBitSet(indexLagThreshold);

    }

    /**
     * issue the next sequence number
     *
     * Note that this method can block to honour maximum indexing lag . See {@link #SETTINGS_INDEX_LAG_THRESHOLD }
     **/
    public long generateSeqNo() {
        synchronized (mutex) {
            // we have to keep checking when ensure capacity returns because it release the lock and nextSeqNo may change
            while (hasCapacity(nextSeqNo) == false) {
                ensureCapacity(nextSeqNo);
            }
            return nextSeqNo++;
        }
    }

    /**
     * marks the processing of the given seqNo have been completed
     * Note that this method can block to honour maximum indexing lag . See {@link #SETTINGS_INDEX_LAG_THRESHOLD }
     **/
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

    /** get's the current check point */
    public long getCheckpoint() {
        return checkpoint;
    }

    /** get's the maximum seqno seen so far */
    public long getMaxSeqNo() {
        return nextSeqNo - 1;
    }


    /** checks if seqNo violates {@link #SETTINGS_INDEX_LAG_THRESHOLD } */
    private boolean hasCapacity(long seqNo) {
        assert Thread.holdsLock(mutex);
        return (seqNo - checkpoint) <= indexLagThreshold;
    }

    /** blocks until {@link #SETTINGS_INDEX_LAG_THRESHOLD } is honoured or raises {@link EsRejectedExecutionException }*/
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

    /** maps the given seqNo to a position in {@link #processedSeqNo} */
    private int seqNoToOffset(long seqNo) {
        assert seqNo - checkpoint <= indexLagThreshold;
        assert seqNo > checkpoint;
        return (int) (seqNo % indexLagThreshold);
    }

}
