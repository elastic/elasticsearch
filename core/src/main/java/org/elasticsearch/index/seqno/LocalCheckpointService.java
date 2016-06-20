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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.SnapshotStatus;

import java.util.LinkedList;

/**
 * This class generates sequences numbers and keeps track of the so called local checkpoint - the highest number for which
 * all previous seqNo have been processed (including)
 */
public class LocalCheckpointService extends AbstractIndexShardComponent {

    /**
     * we keep a bit for each seq No that is still pending. to optimize allocation, we do so in multiple arrays
     * allocating them on demand and cleaning up while completed. This setting controls the size of the arrays
     */
    public static Setting<Integer> SETTINGS_BIT_ARRAYS_SIZE = Setting.intSetting("index.seq_no.checkpoint.bit_arrays_size", 1024,
        4, Setting.Property.IndexScope);

    /**
     * an ordered list of bit arrays representing pending seq nos. The list is "anchored" in {@link #firstProcessedSeqNo}
     * which marks the seqNo the fist bit in the first array corresponds to.
     */
    final LinkedList<FixedBitSet> processedSeqNo;
    final int bitArraysSize;
    long firstProcessedSeqNo;

    /** the current local checkpoint, i.e., all seqNo lower (&lt;=) than this number have been completed */
    volatile long checkpoint;

    /** the next available seqNo - used for seqNo generation */
    volatile long nextSeqNo;

    public LocalCheckpointService(final ShardId shardId, final IndexSettings indexSettings) {
        this(shardId, indexSettings, SequenceNumbersService.NO_OPS_PERFORMED, SequenceNumbersService.NO_OPS_PERFORMED);
    }

    public LocalCheckpointService(final ShardId shardId, final IndexSettings indexSettings, final long maxSeqNo, final long checkpoint) {
        super(shardId, indexSettings);
        bitArraysSize = SETTINGS_BIT_ARRAYS_SIZE.get(indexSettings.getSettings());
        processedSeqNo = new LinkedList<>();
        firstProcessedSeqNo = checkpoint + 1;
        this.nextSeqNo = maxSeqNo + 1;
        this.checkpoint = checkpoint;
    }

    /**
     * issue the next sequence number
     **/
    public synchronized long generateSeqNo() {
        return nextSeqNo++;
    }

    /**
     * marks the processing of the given seqNo have been completed
     **/
    public synchronized void markSeqNoAsCompleted(long seqNo) {
        // make sure we track highest seen seqNo
        if (seqNo >= nextSeqNo) {
            nextSeqNo = seqNo + 1;
        }
        if (seqNo <= checkpoint) {
            // this is possible during recovery where we might replay an op that was also replicated
            return;
        }
        FixedBitSet bitSet = getBitSetForSeqNo(seqNo);
        int offset = seqNoToBitSetOffset(seqNo);
        bitSet.set(offset);
        if (seqNo == checkpoint + 1) {
            updateCheckpoint();
        }
    }

    /** gets the current check point */
    public long getCheckpoint() {
        return checkpoint;
    }

    /** gets the maximum seqno seen so far */
    public long getMaxSeqNo() {
        return nextSeqNo - 1;
    }

    /**
     * moves the checkpoint to the last consecutively processed seqNo
     * Note: this method assumes that the seqNo following the current checkpoint is processed.
     */
    private void updateCheckpoint() {
        assert Thread.holdsLock(this);
        assert checkpoint < firstProcessedSeqNo + bitArraysSize - 1 :
            "checkpoint should be below the end of the first bit set (o.w. current bit set is completed and shouldn't be there)";
        assert getBitSetForSeqNo(checkpoint + 1) == processedSeqNo.getFirst() :
            "checkpoint + 1 doesn't point to the first bit set (o.w. current bit set is completed and shouldn't be there)";
        assert getBitSetForSeqNo(checkpoint + 1).get(seqNoToBitSetOffset(checkpoint + 1)) :
            "updateCheckpoint is called but the bit following the checkpoint is not set";
        // keep it simple for now, get the checkpoint one by one. in the future we can optimize and read words
        FixedBitSet current = processedSeqNo.getFirst();
        do {
            checkpoint++;
            // the checkpoint always falls in the first bit set or just before. If it falls
            // on the last bit of the current bit set, we can clean it.
            if (checkpoint == firstProcessedSeqNo + bitArraysSize - 1) {
                processedSeqNo.removeFirst();
                firstProcessedSeqNo += bitArraysSize;
                assert checkpoint - firstProcessedSeqNo < bitArraysSize;
                current = processedSeqNo.peekFirst();
            }
        } while (current != null && current.get(seqNoToBitSetOffset(checkpoint + 1)));
    }

    /**
     * gets the bit array for the given seqNo, allocating new ones if needed.
     */
    private FixedBitSet getBitSetForSeqNo(long seqNo) {
        assert Thread.holdsLock(this);
        assert seqNo >= firstProcessedSeqNo : "seqNo: " + seqNo + " firstProcessedSeqNo: " + firstProcessedSeqNo;
        int bitSetOffset = ((int) (seqNo - firstProcessedSeqNo)) / bitArraysSize;
        while (bitSetOffset >= processedSeqNo.size()) {
            processedSeqNo.add(new FixedBitSet(bitArraysSize));
        }
        return processedSeqNo.get(bitSetOffset);
    }

    /** maps the given seqNo to a position in the bit set returned by {@link #getBitSetForSeqNo} */
    private int seqNoToBitSetOffset(long seqNo) {
        assert Thread.holdsLock(this);
        assert seqNo >= firstProcessedSeqNo;
        return ((int) (seqNo - firstProcessedSeqNo)) % bitArraysSize;
    }

}
