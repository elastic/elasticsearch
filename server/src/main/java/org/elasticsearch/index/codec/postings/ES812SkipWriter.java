/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2022 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.MultiLevelSkipListWriter;
import org.apache.lucene.index.Impact;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Write skip lists with multiple levels, and support skip within block ints.
 *
 * <p>Assume that docFreq = 28, skipInterval = blockSize = 12
 *
 * <pre>
 *  |       block#0       | |      block#1        | |vInts|
 *  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
 *                          ^                       ^       (level 0 skip point)
 * </pre>
 *
 * <p>Note that skipWriter will ignore first document in block#0, since it is useless as a skip
 * point. Also, we'll never skip into the vInts block, only record skip data at the start its start
 * point(if it exist).
 *
 * <p>For each skip point, we will record: 1. docID in former position, i.e. for position 12, record
 * docID[11], etc. 2. its related file points(position, payload), 3. related numbers or
 * uptos(position, payload). 4. start offset.
 */
final class ES812SkipWriter extends MultiLevelSkipListWriter {
    private final int[] lastSkipDoc;
    private final long[] lastSkipDocPointer;
    private long[] lastSkipPosPointer;
    private long[] lastSkipPayPointer;

    private final IndexOutput docOut;
    private final IndexOutput posOut;
    private final IndexOutput payOut;

    private int curDoc;
    private long curDocPointer;
    private long curPosPointer;
    private long curPayPointer;
    private int curPosBufferUpto;
    private int curPayloadByteUpto;
    private final CompetitiveImpactAccumulator[] curCompetitiveFreqNorms;
    private boolean fieldHasPositions;
    private boolean fieldHasOffsets;
    private boolean fieldHasPayloads;

    ES812SkipWriter(int maxSkipLevels, int blockSize, int docCount, IndexOutput docOut, IndexOutput posOut, IndexOutput payOut) {
        super(blockSize, 8, maxSkipLevels, docCount);
        this.docOut = docOut;
        this.posOut = posOut;
        this.payOut = payOut;

        lastSkipDoc = new int[maxSkipLevels];
        lastSkipDocPointer = new long[maxSkipLevels];
        if (posOut != null) {
            lastSkipPosPointer = new long[maxSkipLevels];
            if (payOut != null) {
                lastSkipPayPointer = new long[maxSkipLevels];
            }
        }
        curCompetitiveFreqNorms = new CompetitiveImpactAccumulator[maxSkipLevels];
        for (int i = 0; i < maxSkipLevels; ++i) {
            curCompetitiveFreqNorms[i] = new CompetitiveImpactAccumulator();
        }
    }

    void setField(boolean fieldHasPositions, boolean fieldHasOffsets, boolean fieldHasPayloads) {
        this.fieldHasPositions = fieldHasPositions;
        this.fieldHasOffsets = fieldHasOffsets;
        this.fieldHasPayloads = fieldHasPayloads;
    }

    // tricky: we only skip data for blocks (terms with more than 128 docs), but re-init'ing the
    // skipper
    // is pretty slow for rare terms in large segments as we have to fill O(log #docs in segment) of
    // junk.
    // this is the vast majority of terms (worst case: ID field or similar). so in resetSkip() we
    // save
    // away the previous pointers, and lazy-init only if we need to buffer skip data for the term.
    private boolean initialized;
    long lastDocFP;
    long lastPosFP;
    long lastPayFP;

    @Override
    public void resetSkip() {
        lastDocFP = docOut.getFilePointer();
        if (fieldHasPositions) {
            lastPosFP = posOut.getFilePointer();
            if (fieldHasOffsets || fieldHasPayloads) {
                lastPayFP = payOut.getFilePointer();
            }
        }
        if (initialized) {
            for (CompetitiveImpactAccumulator acc : curCompetitiveFreqNorms) {
                acc.clear();
            }
        }
        initialized = false;
    }

    private void initSkip() {
        if (initialized == false) {
            super.resetSkip();
            Arrays.fill(lastSkipDoc, 0);
            Arrays.fill(lastSkipDocPointer, lastDocFP);
            if (fieldHasPositions) {
                Arrays.fill(lastSkipPosPointer, lastPosFP);
                if (fieldHasOffsets || fieldHasPayloads) {
                    Arrays.fill(lastSkipPayPointer, lastPayFP);
                }
            }
            // sets of competitive freq,norm pairs should be empty at this point
            assert Arrays.stream(curCompetitiveFreqNorms)
                .map(CompetitiveImpactAccumulator::getCompetitiveFreqNormPairs)
                .mapToInt(Collection::size)
                .sum() == 0;
            initialized = true;
        }
    }

    /** Sets the values for the current skip data. */
    public void bufferSkip(
        int doc,
        CompetitiveImpactAccumulator competitiveFreqNorms,
        int numDocs,
        long posFP,
        long payFP,
        int posBufferUpto,
        int payloadByteUpto
    ) throws IOException {
        initSkip();
        this.curDoc = doc;
        this.curDocPointer = docOut.getFilePointer();
        this.curPosPointer = posFP;
        this.curPayPointer = payFP;
        this.curPosBufferUpto = posBufferUpto;
        this.curPayloadByteUpto = payloadByteUpto;
        this.curCompetitiveFreqNorms[0].addAll(competitiveFreqNorms);
        bufferSkip(numDocs);
    }

    private final ByteBuffersDataOutput freqNormOut = ByteBuffersDataOutput.newResettableInstance();

    @Override
    protected void writeSkipData(int level, DataOutput skipBuffer) throws IOException {

        int delta = curDoc - lastSkipDoc[level];

        skipBuffer.writeVInt(delta);
        lastSkipDoc[level] = curDoc;

        skipBuffer.writeVLong(curDocPointer - lastSkipDocPointer[level]);
        lastSkipDocPointer[level] = curDocPointer;

        if (fieldHasPositions) {

            skipBuffer.writeVLong(curPosPointer - lastSkipPosPointer[level]);
            lastSkipPosPointer[level] = curPosPointer;
            skipBuffer.writeVInt(curPosBufferUpto);

            if (fieldHasPayloads) {
                skipBuffer.writeVInt(curPayloadByteUpto);
            }

            if (fieldHasOffsets || fieldHasPayloads) {
                skipBuffer.writeVLong(curPayPointer - lastSkipPayPointer[level]);
                lastSkipPayPointer[level] = curPayPointer;
            }
        }

        CompetitiveImpactAccumulator competitiveFreqNorms = curCompetitiveFreqNorms[level];
        assert competitiveFreqNorms.getCompetitiveFreqNormPairs().isEmpty() == false;
        if (level + 1 < numberOfSkipLevels) {
            curCompetitiveFreqNorms[level + 1].addAll(competitiveFreqNorms);
        }
        writeImpacts(competitiveFreqNorms, freqNormOut);
        skipBuffer.writeVInt(Math.toIntExact(freqNormOut.size()));
        freqNormOut.copyTo(skipBuffer);
        freqNormOut.reset();
        competitiveFreqNorms.clear();
    }

    static void writeImpacts(CompetitiveImpactAccumulator acc, DataOutput out) throws IOException {
        Collection<Impact> impacts = acc.getCompetitiveFreqNormPairs();
        Impact previous = new Impact(0, 0);
        for (Impact impact : impacts) {
            assert impact.freq > previous.freq;
            assert Long.compareUnsigned(impact.norm, previous.norm) > 0;
            int freqDelta = impact.freq - previous.freq - 1;
            long normDelta = impact.norm - previous.norm - 1;
            if (normDelta == 0) {
                // most of time, norm only increases by 1, so we can fold everything in a single byte
                out.writeVInt(freqDelta << 1);
            } else {
                out.writeVInt((freqDelta << 1) | 1);
                out.writeZLong(normDelta);
            }
            previous = impact;
        }
    }
}
