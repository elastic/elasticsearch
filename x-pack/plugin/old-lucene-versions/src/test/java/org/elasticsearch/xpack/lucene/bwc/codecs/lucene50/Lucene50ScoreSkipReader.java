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
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene50;

import org.apache.lucene.index.FreqAndNormBuffer;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;
import java.util.Arrays;

final class Lucene50ScoreSkipReader extends Lucene50SkipReader {

    private final byte[][] impactData;
    private final int[] impactDataLength;
    private final ByteArrayDataInput badi = new ByteArrayDataInput();
    private final Impacts impacts;
    private int numLevels = 1;
    private final FreqAndNormBuffer[] perLevelImpacts;

    Lucene50ScoreSkipReader(
        int version,
        IndexInput skipStream,
        int maxSkipLevels,
        boolean hasPos,
        boolean hasOffsets,
        boolean hasPayloads
    ) {
        super(version, skipStream, maxSkipLevels, hasPos, hasOffsets, hasPayloads);
        if (version < BWCLucene50PostingsFormat.VERSION_IMPACT_SKIP_DATA) {
            throw new IllegalStateException("Cannot skip based on scores if impacts are not indexed");
        }
        this.impactData = new byte[maxSkipLevels][];
        Arrays.fill(impactData, new byte[0]);
        this.impactDataLength = new int[maxSkipLevels];
        this.perLevelImpacts = new FreqAndNormBuffer[maxSkipLevels];
        for (int i = 0; i < perLevelImpacts.length; ++i) {
            perLevelImpacts[i] = new FreqAndNormBuffer();
            perLevelImpacts[i].add(Integer.MAX_VALUE, 1L);
        }
        impacts = new Impacts() {

            @Override
            public int numLevels() {
                return numLevels;
            }

            @Override
            public int getDocIdUpTo(int level) {
                return skipDoc[level];
            }

            @Override
            public FreqAndNormBuffer getImpacts(int level) {
                assert level < numLevels;
                if (impactDataLength[level] > 0) {
                    badi.reset(impactData[level], 0, impactDataLength[level]);
                    perLevelImpacts[level] = readImpacts(badi, perLevelImpacts[level]);
                    impactDataLength[level] = 0;
                }
                return perLevelImpacts[level];
            }
        };
    }

    @Override
    public int skipTo(int target) throws IOException {
        int result = super.skipTo(target);
        if (numberOfSkipLevels > 0) {
            numLevels = numberOfSkipLevels;
        } else {
            // End of postings don't have skip data anymore, so we fill with dummy data
            // like SlowImpactsEnum.
            numLevels = 1;
            perLevelImpacts[0].size = 1;
            perLevelImpacts[0].freqs[0] = Integer.MAX_VALUE;
            perLevelImpacts[0].norms[0] = 1L;
            impactDataLength[0] = 0;
        }
        return result;
    }

    Impacts getImpacts() {
        return impacts;
    }

    @Override
    protected void readImpacts(int level, IndexInput skipStream) throws IOException {
        int length = skipStream.readVInt();
        if (impactData[level].length < length) {
            impactData[level] = new byte[ArrayUtil.oversize(length, Byte.BYTES)];
        }
        skipStream.readBytes(impactData[level], 0, length);
        impactDataLength[level] = length;
    }

    static FreqAndNormBuffer readImpacts(ByteArrayDataInput in, FreqAndNormBuffer reuse) {
        int maxNumImpacts = in.length(); // at most one impact per byte
        reuse.growNoCopy(maxNumImpacts);

        int freq = 0;
        long norm = 0;
        int size = 0;
        while (in.getPosition() < in.length()) {
            int freqDelta = in.readVInt();
            if ((freqDelta & 0x01) != 0) {
                freq += 1 + (freqDelta >>> 1);
                try {
                    norm += 1 + in.readZLong();
                } catch (IOException e) {
                    throw new RuntimeException(e); // cannot happen on a BADI
                }
            } else {
                freq += 1 + (freqDelta >>> 1);
                norm++;
            }
            reuse.freqs[size] = freq;
            reuse.norms[size] = norm;
            size++;
        }
        reuse.size = size;
        return reuse;
    }
}
