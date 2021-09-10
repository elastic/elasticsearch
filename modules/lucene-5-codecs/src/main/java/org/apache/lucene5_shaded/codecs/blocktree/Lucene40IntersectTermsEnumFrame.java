/*
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
 */
package org.apache.lucene5_shaded.codecs.blocktree;

import java.io.IOException;

import org.apache.lucene5_shaded.codecs.BlockTermState;
import org.apache.lucene5_shaded.index.IndexOptions;
import org.apache.lucene5_shaded.store.ByteArrayDataInput;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.automaton.Transition;
import org.apache.lucene5_shaded.util.fst.FST;



/**
 * @deprecated Only for 4.x backcompat
 */
// TODO: can we share this with the frame in STE?
@Deprecated
final class Lucene40IntersectTermsEnumFrame {
  final int ord;
  long fp;
  long fpOrig;
  long fpEnd;
  long lastSubFP;

  // State in automaton
  int state;

  int metaDataUpto;

  byte[] suffixBytes = new byte[128];
  final ByteArrayDataInput suffixesReader = new ByteArrayDataInput();

  byte[] statBytes = new byte[64];
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
        
  Transition transition = new Transition();
  int curTransitionMax;
  int transitionIndex;
  int transitionCount;

  FST.Arc<BytesRef> arc;

  final BlockTermState termState;
  
  // metadata buffer, holding monotonic values
  public long[] longs;
  // metadata buffer, holding general values
  public byte[] bytes;
  ByteArrayDataInput bytesReader;

  // Cumulative output so far
  BytesRef outputPrefix;

  int startBytePos;
  int suffix;

  private final Lucene40IntersectTermsEnum ite;

  public Lucene40IntersectTermsEnumFrame(Lucene40IntersectTermsEnum ite, int ord) throws IOException {
    this.ite = ite;
    this.ord = ord;
    this.termState = ite.fr.parent.postingsReader.newTermState();
    this.termState.totalTermFreq = -1;
    this.longs = new long[ite.fr.longsSize];
  }

  void loadNextFloorBlock() throws IOException {
    assert numFollowFloorBlocks > 0;
    //if (DEBUG) System.out.println("    loadNextFoorBlock trans=" + transitions[transitionIndex]);

    do {
      fp = fpOrig + (floorDataReader.readVLong() >>> 1);
      numFollowFloorBlocks--;
      // if (DEBUG) System.out.println("    skip floor block2!  nextFloorLabel=" + (char) nextFloorLabel + " vs target=" + (char) transitions[transitionIndex].getMin() + " newFP=" + fp + " numFollowFloorBlocks=" + numFollowFloorBlocks);
      if (numFollowFloorBlocks != 0) {
        nextFloorLabel = floorDataReader.readByte() & 0xff;
      } else {
        nextFloorLabel = 256;
      }
      // if (DEBUG) System.out.println("    nextFloorLabel=" + (char) nextFloorLabel);
    } while (numFollowFloorBlocks != 0 && nextFloorLabel <= transition.min);

    load(null);
  }

  public void setState(int state) {
    this.state = state;
    transitionIndex = 0;
    transitionCount = ite.compiledAutomaton.automaton.getNumTransitions(state);
    if (transitionCount != 0) {
      ite.compiledAutomaton.automaton.initTransition(state, transition);
      ite.compiledAutomaton.automaton.getNextTransition(transition);
      curTransitionMax = transition.max;
    } else {
      curTransitionMax = -1;
    }
  }

  void load(BytesRef frameIndexData) throws IOException {

    // if (DEBUG) System.out.println("    load fp=" + fp + " fpOrig=" + fpOrig + " frameIndexData=" + frameIndexData + " trans=" + (transitions.length != 0 ? transitions[0] : "n/a" + " state=" + state));

    if (frameIndexData != null && transitionCount != 0) {
      // Floor frame
      if (floorData.length < frameIndexData.length) {
        this.floorData = new byte[ArrayUtil.oversize(frameIndexData.length, 1)];
      }
      System.arraycopy(frameIndexData.bytes, frameIndexData.offset, floorData, 0, frameIndexData.length);
      floorDataReader.reset(floorData, 0, frameIndexData.length);
      // Skip first long -- has redundant fp, hasTerms
      // flag, isFloor flag
      final long code = floorDataReader.readVLong();
      if ((code & Lucene40BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR) != 0) {
        numFollowFloorBlocks = floorDataReader.readVInt();
        nextFloorLabel = floorDataReader.readByte() & 0xff;
        // if (DEBUG) System.out.println("    numFollowFloorBlocks=" + numFollowFloorBlocks + " nextFloorLabel=" + nextFloorLabel);

        // If current state is accept, we must process
        // first block in case it has empty suffix:
        if (!ite.runAutomaton.isAccept(state)) {
          // Maybe skip floor blocks:
          assert transitionIndex == 0: "transitionIndex=" + transitionIndex;
          while (numFollowFloorBlocks != 0 && nextFloorLabel <= transition.min) {
            fp = fpOrig + (floorDataReader.readVLong() >>> 1);
            numFollowFloorBlocks--;
            // if (DEBUG) System.out.println("    skip floor block!  nextFloorLabel=" + (char) nextFloorLabel + " vs target=" + (char) transitions[0].getMin() + " newFP=" + fp + " numFollowFloorBlocks=" + numFollowFloorBlocks);
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
    code = ite.in.readVInt();
    isLeafBlock = (code & 1) != 0;
    int numBytes = code >>> 1;
    // if (DEBUG) System.out.println("      entCount=" + entCount + " lastInFloor?=" + isLastInFloor + " leafBlock?=" + isLeafBlock + " numSuffixBytes=" + numBytes);
    if (suffixBytes.length < numBytes) {
      suffixBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
    }
    ite.in.readBytes(suffixBytes, 0, numBytes);
    suffixesReader.reset(suffixBytes, 0, numBytes);

    // stats
    numBytes = ite.in.readVInt();
    if (statBytes.length < numBytes) {
      statBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
    }
    ite.in.readBytes(statBytes, 0, numBytes);
    statsReader.reset(statBytes, 0, numBytes);
    metaDataUpto = 0;

    termState.termBlockOrd = 0;
    nextEnt = 0;
         
    // metadata
    numBytes = ite.in.readVInt();
    if (bytes == null) {
      bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
      bytesReader = new ByteArrayDataInput();
    } else if (bytes.length < numBytes) {
      bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
    }
    ite.in.readBytes(bytes, 0, numBytes);
    bytesReader.reset(bytes, 0, numBytes);

    if (!isLastInFloor) {
      // Sub-blocks of a single floor block are always
      // written one after another -- tail recurse:
      fpEnd = ite.in.getFilePointer();
    }
  }

  // TODO: maybe add scanToLabel; should give perf boost

  public boolean next() {
    return isLeafBlock ? nextLeaf() : nextNonLeaf();
  }

  // Decodes next entry; returns true if it's a sub-block
  public boolean nextLeaf() {
    //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
    assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
    nextEnt++;
    suffix = suffixesReader.readVInt();
    startBytePos = suffixesReader.getPosition();
    suffixesReader.skipBytes(suffix);
    return false;
  }

  public boolean nextNonLeaf() {
    //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
    assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
    nextEnt++;
    final int code = suffixesReader.readVInt();
    suffix = code >>> 1;
    startBytePos = suffixesReader.getPosition();
    suffixesReader.skipBytes(suffix);
    if ((code & 1) == 0) {
      // A normal term
      termState.termBlockOrd++;
      return false;
    } else {
      // A sub-block; make sub-FP absolute:
      lastSubFP = fp - suffixesReader.readVLong();
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
      termState.docFreq = statsReader.readVInt();
      //if (DEBUG) System.out.println("    dF=" + state.docFreq);
      if (ite.fr.fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
        termState.totalTermFreq = termState.docFreq + statsReader.readVLong();
        //if (DEBUG) System.out.println("    totTF=" + state.totalTermFreq);
      }
      // metadata 
      for (int i = 0; i < ite.fr.longsSize; i++) {
        longs[i] = bytesReader.readVLong();
      }
      ite.fr.parent.postingsReader.decodeTerm(longs, bytesReader, ite.fr.fieldInfo, termState, absolute);

      metaDataUpto++;
      absolute = false;
    }
    termState.termBlockOrd = metaDataUpto;
  }
}
