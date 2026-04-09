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
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene40.blocktree;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Transition;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.FST;

import java.io.IOException;
import java.util.Arrays;

/**
 * This is a copy of the class with same name shipped with Lucene, which is though package protected hence not accessible.
 * We need to copy it because we have our own fork of {@link FieldReader}.
 */
final class IntersectTermsEnumFrame {
    final int ord;
    long fp;
    long fpOrig;
    long fpEnd;
    long lastSubFP;

    // private static boolean DEBUG = IntersectTermsEnum.DEBUG;

    // State in automaton
    int state;

    // State just before the last label
    int lastState;

    int metaDataUpto;

    byte[] suffixBytes = new byte[128];
    final ByteArrayDataInput suffixesReader = new ByteArrayDataInput();

    byte[] suffixLengthBytes;
    final ByteArrayDataInput suffixLengthsReader;

    byte[] statBytes = new byte[64];
    int statsSingletonRunLength = 0;
    final ByteArrayDataInput statsReader = new ByteArrayDataInput();

    byte[] floorData = new byte[32];
    final ByteArrayDataInput floorDataReader = new ByteArrayDataInput();

    // Length of prefix shared by all terms in this block
    int prefix;

    // Number of entries (term or sub-block) in this block
    int entCount;

    // Which term we will next read
    int nextEnt;

    // True if this block is either not a floor block,
    // or, it's the last sub-block of a floor block
    boolean isLastInFloor;

    // True if all entries are terms
    boolean isLeafBlock;

    int numFollowFloorBlocks;
    int nextFloorLabel;

    final Transition transition = new Transition();
    int transitionIndex;
    int transitionCount;

    FST.Arc<BytesRef> arc;

    final BlockTermState termState;

    // metadata buffer
    byte[] bytes = new byte[32];

    final ByteArrayDataInput bytesReader = new ByteArrayDataInput();

    // Cumulative output so far
    BytesRef outputPrefix;

    int startBytePos;
    int suffix;

    private final IntersectTermsEnum ite;
    private final int version;

    IntersectTermsEnumFrame(IntersectTermsEnum ite, int ord) throws IOException {
        this.ite = ite;
        this.ord = ord;
        this.termState = ite.fr.parent.postingsReader.newTermState();
        this.termState.totalTermFreq = -1;
        this.version = ite.fr.parent.version;
        if (version >= Lucene40BlockTreeTermsReader.VERSION_COMPRESSED_SUFFIXES) {
            suffixLengthBytes = new byte[32];
            suffixLengthsReader = new ByteArrayDataInput();
        } else {
            suffixLengthBytes = null;
            suffixLengthsReader = suffixesReader;
        }
    }

    void loadNextFloorBlock() throws IOException {
        assert numFollowFloorBlocks > 0 : "nextFloorLabel=" + nextFloorLabel;

        do {
            fp = fpOrig + (floorDataReader.readVLong() >>> 1);
            numFollowFloorBlocks--;
            if (numFollowFloorBlocks != 0) {
                nextFloorLabel = floorDataReader.readByte() & 0xff;
            } else {
                nextFloorLabel = 256;
            }
        } while (numFollowFloorBlocks != 0 && nextFloorLabel <= transition.min);

        load(null);
    }

    public void setState(int state) {
        this.state = state;
        transitionIndex = 0;
        transitionCount = ite.automaton.getNumTransitions(state);
        if (transitionCount != 0) {
            ite.automaton.initTransition(state, transition);
            ite.automaton.getNextTransition(transition);
        } else {

            // Must set min to -1 so the "label < min" check never falsely triggers:
            transition.min = -1;

            // Must set max to -1 so we immediately realize we need to step to the next transition and
            // then pop this frame:
            transition.max = -1;
        }
    }

    void load(BytesRef frameIndexData) throws IOException {
        if (frameIndexData != null) {
            floorDataReader.reset(frameIndexData.bytes, frameIndexData.offset, frameIndexData.length);
            // Skip first long -- has redundant fp, hasTerms
            // flag, isFloor flag
            final long code = floorDataReader.readVLong();
            if ((code & Lucene40BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR) != 0) {
                // Floor frame
                numFollowFloorBlocks = floorDataReader.readVInt();
                nextFloorLabel = floorDataReader.readByte() & 0xff;

                // If current state is not accept, and has transitions, we must process
                // first block in case it has empty suffix:
                if (ite.runAutomaton.isAccept(state) == false && transitionCount != 0) {
                    // Maybe skip floor blocks:
                    assert transitionIndex == 0 : "transitionIndex=" + transitionIndex;
                    while (numFollowFloorBlocks != 0 && nextFloorLabel <= transition.min) {
                        fp = fpOrig + (floorDataReader.readVLong() >>> 1);
                        numFollowFloorBlocks--;
                        if (numFollowFloorBlocks != 0) {
                            nextFloorLabel = floorDataReader.readByte() & 0xff;
                        } else {
                            nextFloorLabel = 256;
                        }
                    }
                }
            }
        }

        ite.in.seek(fp);
        int code = ite.in.readVInt();
        entCount = code >>> 1;
        assert entCount > 0;
        isLastInFloor = (code & 1) != 0;

        // term suffixes:
        if (version >= Lucene40BlockTreeTermsReader.VERSION_COMPRESSED_SUFFIXES) {
            final long codeL = ite.in.readVLong();
            isLeafBlock = (codeL & 0x04) != 0;
            final int numSuffixBytes = (int) (codeL >>> 3);
            if (suffixBytes.length < numSuffixBytes) {
                suffixBytes = new byte[ArrayUtil.oversize(numSuffixBytes, 1)];
            }
            final CompressionAlgorithm compressionAlg;
            try {
                compressionAlg = CompressionAlgorithm.byCode((int) codeL & 0x03);
            } catch (IllegalArgumentException e) {
                throw new CorruptIndexException(e.getMessage(), ite.in, e);
            }
            compressionAlg.read(ite.in, suffixBytes, numSuffixBytes);
            suffixesReader.reset(suffixBytes, 0, numSuffixBytes);

            int numSuffixLengthBytes = ite.in.readVInt();
            final boolean allEqual = (numSuffixLengthBytes & 0x01) != 0;
            numSuffixLengthBytes >>>= 1;
            if (suffixLengthBytes.length < numSuffixLengthBytes) {
                suffixLengthBytes = new byte[ArrayUtil.oversize(numSuffixLengthBytes, 1)];
            }
            if (allEqual) {
                Arrays.fill(suffixLengthBytes, 0, numSuffixLengthBytes, ite.in.readByte());
            } else {
                ite.in.readBytes(suffixLengthBytes, 0, numSuffixLengthBytes);
            }
            suffixLengthsReader.reset(suffixLengthBytes, 0, numSuffixLengthBytes);
        } else {
            code = ite.in.readVInt();
            isLeafBlock = (code & 1) != 0;
            int numBytes = code >>> 1;
            if (suffixBytes.length < numBytes) {
                suffixBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
            }
            ite.in.readBytes(suffixBytes, 0, numBytes);
            suffixesReader.reset(suffixBytes, 0, numBytes);
        }

        // stats
        int numBytes = ite.in.readVInt();
        if (statBytes.length < numBytes) {
            statBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
        }
        ite.in.readBytes(statBytes, 0, numBytes);
        statsReader.reset(statBytes, 0, numBytes);
        statsSingletonRunLength = 0;
        metaDataUpto = 0;

        termState.termBlockOrd = 0;
        nextEnt = 0;

        // metadata
        numBytes = ite.in.readVInt();
        if (bytes.length < numBytes) {
            bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
        }
        ite.in.readBytes(bytes, 0, numBytes);
        bytesReader.reset(bytes, 0, numBytes);

        if (isLastInFloor == false) {
            // Sub-blocks of a single floor block are always
            // written one after another -- tail recurse:
            fpEnd = ite.in.getFilePointer();
        }
    }

    // TODO: maybe add scanToLabel; should give perf boost

    // Decodes next entry; returns true if it's a sub-block
    public boolean next() {
        if (isLeafBlock) {
            nextLeaf();
            return false;
        } else {
            return nextNonLeaf();
        }
    }

    public void nextLeaf() {
        assert nextEnt != -1 && nextEnt < entCount : "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
        nextEnt++;
        suffix = suffixLengthsReader.readVInt();
        startBytePos = suffixesReader.getPosition();
        suffixesReader.skipBytes(suffix);
    }

    public boolean nextNonLeaf() {
        assert nextEnt != -1 && nextEnt < entCount : "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
        nextEnt++;
        final int code = suffixLengthsReader.readVInt();
        suffix = code >>> 1;
        startBytePos = suffixesReader.getPosition();
        suffixesReader.skipBytes(suffix);
        if ((code & 1) == 0) {
            // A normal term
            termState.termBlockOrd++;
            return false;
        } else {
            // A sub-block; make sub-FP absolute:
            lastSubFP = fp - suffixLengthsReader.readVLong();
            return true;
        }
    }

    public int getTermBlockOrd() {
        return isLeafBlock ? nextEnt : termState.termBlockOrd;
    }

    public void decodeMetaData() throws IOException {

        // lazily catch up on metadata decode:
        final int limit = getTermBlockOrd();
        boolean absolute = metaDataUpto == 0;
        assert limit > 0;

        // TODO: better API would be "jump straight to term=N"???
        while (metaDataUpto < limit) {

            // TODO: we could make "tiers" of metadata, ie,
            // decode docFreq/totalTF but don't decode postings
            // metadata; this way caller could get
            // docFreq/totalTF w/o paying decode cost for
            // postings

            // TODO: if docFreq were bulk decoded we could
            // just skipN here:

            // stats
            if (version >= Lucene40BlockTreeTermsReader.VERSION_COMPRESSED_SUFFIXES) {
                if (statsSingletonRunLength > 0) {
                    termState.docFreq = 1;
                    termState.totalTermFreq = 1;
                    statsSingletonRunLength--;
                } else {
                    int token = statsReader.readVInt();
                    if (version >= Lucene40BlockTreeTermsReader.VERSION_COMPRESSED_SUFFIXES && (token & 1) == 1) {
                        termState.docFreq = 1;
                        termState.totalTermFreq = 1;
                        statsSingletonRunLength = token >>> 1;
                    } else {
                        termState.docFreq = token >>> 1;
                        if (ite.fr.fieldInfo.getIndexOptions() == IndexOptions.DOCS) {
                            termState.totalTermFreq = termState.docFreq;
                        } else {
                            termState.totalTermFreq = termState.docFreq + statsReader.readVLong();
                        }
                    }
                }
            } else {
                termState.docFreq = statsReader.readVInt();
                // if (DEBUG) System.out.println(" dF=" + state.docFreq);
                if (ite.fr.fieldInfo.getIndexOptions() == IndexOptions.DOCS) {
                    termState.totalTermFreq = termState.docFreq; // all postings have freq=1
                } else {
                    termState.totalTermFreq = termState.docFreq + statsReader.readVLong();
                    // if (DEBUG) System.out.println(" totTF=" + state.totalTermFreq);
                }
            }
            // metadata
            ite.fr.parent.postingsReader.decodeTerm(bytesReader, ite.fr.fieldInfo, termState, absolute);

            metaDataUpto++;
            absolute = false;
        }
        termState.termBlockOrd = metaDataUpto;
    }
}
