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

// TODO: can we share this with the frame in STE?
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
        
  final Transition transition = new Transition();
  int transitionIndex;
  int transitionCount;

  final boolean versionAutoPrefix;

  FST.Arc<BytesRef> arc;

  final BlockTermState termState;
  
  // metadata buffer, holding monotonic values
  final long[] longs;

  // metadata buffer, holding general values
  byte[] bytes = new byte[32];

  final ByteArrayDataInput bytesReader = new ByteArrayDataInput();

  // Cumulative output so far
  BytesRef outputPrefix;

  int startBytePos;
  int suffix;

  // When we are on an auto-prefix term this is the starting lead byte
  // of the suffix (e.g. 'a' for the foo[a-m]* case):
  int floorSuffixLeadStart;

  // When we are on an auto-prefix term this is the ending lead byte
  // of the suffix (e.g. 'm' for the foo[a-m]* case):
  int floorSuffixLeadEnd;

  // True if the term we are currently on is an auto-prefix term:
  boolean isAutoPrefixTerm;

  private final IntersectTermsEnum ite;

  public IntersectTermsEnumFrame(IntersectTermsEnum ite, int ord) throws IOException {
    this.ite = ite;
    this.ord = ord;
    this.termState = ite.fr.parent.postingsReader.newTermState();
    this.termState.totalTermFreq = -1;
    this.longs = new long[ite.fr.longsSize];
    this.versionAutoPrefix = ite.fr.parent.anyAutoPrefixTerms;
  }

  void loadNextFloorBlock() throws IOException {
    assert numFollowFloorBlocks > 0: "nextFloorLabel=" + nextFloorLabel;

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

      // Must set max to -1 so we immediately realize we need to step to the next transition and then pop this frame:
      transition.max = -1;
    }
  }

  void load(BytesRef frameIndexData) throws IOException {
    if (frameIndexData != null) {
      floorDataReader.reset(frameIndexData.bytes, frameIndexData.offset, frameIndexData.length);
      // Skip first long -- has redundant fp, hasTerms
      // flag, isFloor flag
      final long code = floorDataReader.readVLong();
      if ((code & BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR) != 0) {
        // Floor frame
        numFollowFloorBlocks = floorDataReader.readVInt();
        nextFloorLabel = floorDataReader.readByte() & 0xff;

        // If current state is not accept, and has transitions, we must process
        // first block in case it has empty suffix:
        if (ite.runAutomaton.isAccept(state) == false && transitionCount != 0) {
          // Maybe skip floor blocks:
          assert transitionIndex == 0: "transitionIndex=" + transitionIndex;
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
    code = ite.in.readVInt();
    isLeafBlock = (code & 1) != 0;
    int numBytes = code >>> 1;
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
    if (bytes.length < numBytes) {
      bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
    }
    ite.in.readBytes(bytes, 0, numBytes);
    bytesReader.reset(bytes, 0, numBytes);

    if (!isLastInFloor) {
      // Sub-blocks of a single floor block are always
      // written one after another -- tail recurse:
      fpEnd = ite.in.getFilePointer();
    }

    // Necessary in case this ord previously was an auto-prefix
    // term but now we recurse to a new leaf block
    isAutoPrefixTerm = false;
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
    assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
    nextEnt++;
    suffix = suffixesReader.readVInt();
    startBytePos = suffixesReader.getPosition();
    suffixesReader.skipBytes(suffix);
  }

  public boolean nextNonLeaf() {
    assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
    nextEnt++;
    final int code = suffixesReader.readVInt();
    if (versionAutoPrefix == false) {
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
    } else {
      suffix = code >>> 2;
      startBytePos = suffixesReader.getPosition();
      suffixesReader.skipBytes(suffix);
      switch (code & 3) {
      case 0:
        // A normal term
        isAutoPrefixTerm = false;
        termState.termBlockOrd++;
        return false;
      case 1:
        // A sub-block; make sub-FP absolute:
        isAutoPrefixTerm = false;
        lastSubFP = fp - suffixesReader.readVLong();
        return true;
      case 2:
        // A normal prefix term, suffix leads with empty string
        floorSuffixLeadStart = -1;
        termState.termBlockOrd++;
        floorSuffixLeadEnd = suffixesReader.readByte() & 0xff;
        if (floorSuffixLeadEnd == 0xff) {
          floorSuffixLeadEnd = -1;
        }
        isAutoPrefixTerm = true;
        return false;
      case 3:
        // A floor'd prefix term, suffix leads with real byte
        if (suffix == 0) {
          // TODO: this is messy, but necessary because we are an auto-prefix term, but our suffix is the empty string here, so we have to
          // look at the parent block to get the lead suffix byte:
          assert ord > 0;
          IntersectTermsEnumFrame parent = ite.stack[ord-1];
          floorSuffixLeadStart = parent.suffixBytes[parent.startBytePos+parent.suffix-1] & 0xff;
        } else {
          floorSuffixLeadStart = suffixBytes[startBytePos+suffix-1] & 0xff;
        }
        termState.termBlockOrd++;
        isAutoPrefixTerm = true;
        floorSuffixLeadEnd = suffixesReader.readByte() & 0xff;
        return false;
      default:
        // Silly javac:
        assert false;
        return false;
      }
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
      if (ite.fr.fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
        termState.totalTermFreq = termState.docFreq + statsReader.readVLong();
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
