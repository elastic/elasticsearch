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

import org.apache.lucene5_shaded.index.DocsAndPositionsEnum;
import org.apache.lucene5_shaded.index.IndexOptions;
import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.TermState;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.StringHelper;
import org.apache.lucene5_shaded.util.automaton.CompiledAutomaton;
import org.apache.lucene5_shaded.util.automaton.RunAutomaton;
import org.apache.lucene5_shaded.util.fst.ByteSequenceOutputs;
import org.apache.lucene5_shaded.util.fst.FST;
import org.apache.lucene5_shaded.util.fst.Outputs;

// NOTE: cannot seek!

/**
 * @deprecated Only for 4.x backcompat
 */
@Deprecated
final class Lucene40IntersectTermsEnum extends TermsEnum {
  final IndexInput in;
  final static Outputs<BytesRef> fstOutputs = ByteSequenceOutputs.getSingleton();

  private Lucene40IntersectTermsEnumFrame[] stack;
      
  @SuppressWarnings({"rawtypes","unchecked"}) private FST.Arc<BytesRef>[] arcs = new FST.Arc[5];

  final RunAutomaton runAutomaton;
  final CompiledAutomaton compiledAutomaton;

  private Lucene40IntersectTermsEnumFrame currentFrame;

  private final BytesRef term = new BytesRef();

  private final FST.BytesReader fstReader;

  final Lucene40FieldReader fr;

  private BytesRef savedStartTerm;
      
  // TODO: in some cases we can filter by length?  eg
  // regexp foo*bar must be at least length 6 bytes
  public Lucene40IntersectTermsEnum(Lucene40FieldReader fr, CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    // if (DEBUG) {
    //   System.out.println("\nintEnum.init seg=" + segment + " commonSuffix=" + brToString(compiled.commonSuffixRef));
    // }
    this.fr = fr;
    runAutomaton = compiled.runAutomaton;
    compiledAutomaton = compiled;
    in = fr.parent.in.clone();
    stack = new Lucene40IntersectTermsEnumFrame[5];
    for(int idx=0;idx<stack.length;idx++) {
      stack[idx] = new Lucene40IntersectTermsEnumFrame(this, idx);
    }
    for(int arcIdx=0;arcIdx<arcs.length;arcIdx++) {
      arcs[arcIdx] = new FST.Arc<>();
    }

    if (fr.index == null) {
      fstReader = null;
    } else {
      fstReader = fr.index.getBytesReader();
    }

    // TODO: if the automaton is "smallish" we really
    // should use the terms index to seek at least to
    // the initial term and likely to subsequent terms
    // (or, maybe just fallback to ATE for such cases).
    // Else the seek cost of loading the frames will be
    // too costly.

    final FST.Arc<BytesRef> arc = fr.index.getFirstArc(arcs[0]);
    // Empty string prefix must have an output in the index!
    assert arc.isFinal();

    // Special pushFrame since it's the first one:
    final Lucene40IntersectTermsEnumFrame f = stack[0];
    f.fp = f.fpOrig = fr.rootBlockFP;
    f.prefix = 0;
    f.setState(runAutomaton.getInitialState());
    f.arc = arc;
    f.outputPrefix = arc.output;
    f.load(fr.rootCode);

    // for assert:
    assert setSavedStartTerm(startTerm);

    currentFrame = f;
    if (startTerm != null) {
      seekToStartTerm(startTerm);
    }
  }

  // only for assert:
  private boolean setSavedStartTerm(BytesRef startTerm) {
    savedStartTerm = startTerm == null ? null : BytesRef.deepCopyOf(startTerm);
    return true;
  }

  @Override
  public TermState termState() throws IOException {
    currentFrame.decodeMetaData();
    return currentFrame.termState.clone();
  }

  private Lucene40IntersectTermsEnumFrame getFrame(int ord) throws IOException {
    if (ord >= stack.length) {
      final Lucene40IntersectTermsEnumFrame[] next = new Lucene40IntersectTermsEnumFrame[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(stack, 0, next, 0, stack.length);
      for(int stackOrd=stack.length;stackOrd<next.length;stackOrd++) {
        next[stackOrd] = new Lucene40IntersectTermsEnumFrame(this, stackOrd);
      }
      stack = next;
    }
    assert stack[ord].ord == ord;
    return stack[ord];
  }

  private FST.Arc<BytesRef> getArc(int ord) {
    if (ord >= arcs.length) {
      @SuppressWarnings({"rawtypes","unchecked"}) final FST.Arc<BytesRef>[] next =
      new FST.Arc[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(arcs, 0, next, 0, arcs.length);
      for(int arcOrd=arcs.length;arcOrd<next.length;arcOrd++) {
        next[arcOrd] = new FST.Arc<>();
      }
      arcs = next;
    }
    return arcs[ord];
  }

  private Lucene40IntersectTermsEnumFrame pushFrame(int state) throws IOException {
    final Lucene40IntersectTermsEnumFrame f = getFrame(currentFrame == null ? 0 : 1+currentFrame.ord);
        
    f.fp = f.fpOrig = currentFrame.lastSubFP;
    f.prefix = currentFrame.prefix + currentFrame.suffix;
    // if (DEBUG) System.out.println("    pushFrame state=" + state + " prefix=" + f.prefix);
    f.setState(state);

    // Walk the arc through the index -- we only
    // "bother" with this so we can get the floor data
    // from the index and skip floor blocks when
    // possible:
    FST.Arc<BytesRef> arc = currentFrame.arc;
    int idx = currentFrame.prefix;
    assert currentFrame.suffix > 0;
    BytesRef output = currentFrame.outputPrefix;
    while (idx < f.prefix) {
      final int target = term.bytes[idx] & 0xff;
      // TODO: we could be more efficient for the next()
      // case by using current arc as starting point,
      // passed to findTargetArc
      arc = fr.index.findTargetArc(target, arc, getArc(1+idx), fstReader);
      assert arc != null;
      output = fstOutputs.add(output, arc.output);
      idx++;
    }

    f.arc = arc;
    f.outputPrefix = output;
    assert arc.isFinal();
    f.load(fstOutputs.add(output, arc.nextFinalOutput));
    return f;
  }

  @Override
  public BytesRef term() {
    return term;
  }

  @Override
  public int docFreq() throws IOException {
    //if (DEBUG) System.out.println("BTIR.docFreq");
    currentFrame.decodeMetaData();
    //if (DEBUG) System.out.println("  return " + currentFrame.termState.docFreq);
    return currentFrame.termState.docFreq;
  }

  @Override
  public long totalTermFreq() throws IOException {
    currentFrame.decodeMetaData();
    return currentFrame.termState.totalTermFreq;
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    if (PostingsEnum.featureRequested(flags, DocsAndPositionsEnum.OLD_NULL_SEMANTICS) && fr.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
      // Positions were not indexed:
      return null;
    }
    currentFrame.decodeMetaData();
    return fr.parent.postingsReader.postings(fr.fieldInfo, currentFrame.termState, reuse, flags);
  }

  private int getState() {
    int state = currentFrame.state;
    for(int idx=0;idx<currentFrame.suffix;idx++) {
      state = runAutomaton.step(state,  currentFrame.suffixBytes[currentFrame.startBytePos+idx] & 0xff);
      assert state != -1;
    }
    return state;
  }

  // NOTE: specialized to only doing the first-time
  // seek, but we could generalize it to allow
  // arbitrary seekExact/Ceil.  Note that this is a
  // seekFloor!
  private void seekToStartTerm(BytesRef target) throws IOException {
    //if (DEBUG) System.out.println("seek to startTerm=" + target.utf8ToString());
    assert currentFrame.ord == 0;
    if (term.length < target.length) {
      term.bytes = ArrayUtil.grow(term.bytes, target.length);
    }
    FST.Arc<BytesRef> arc = arcs[0];
    assert arc == currentFrame.arc;

    for(int idx=0;idx<=target.length;idx++) {

      while (true) {
        final int savePos = currentFrame.suffixesReader.getPosition();
        final int saveStartBytePos = currentFrame.startBytePos;
        final int saveSuffix = currentFrame.suffix;
        final long saveLastSubFP = currentFrame.lastSubFP;
        final int saveTermBlockOrd = currentFrame.termState.termBlockOrd;

        final boolean isSubBlock = currentFrame.next();

        //if (DEBUG) System.out.println("    cycle ent=" + currentFrame.nextEnt + " (of " + currentFrame.entCount + ") prefix=" + currentFrame.prefix + " suffix=" + currentFrame.suffix + " isBlock=" + isSubBlock + " firstLabel=" + (currentFrame.suffix == 0 ? "" : (currentFrame.suffixBytes[currentFrame.startBytePos])&0xff));
        term.length = currentFrame.prefix + currentFrame.suffix;
        if (term.bytes.length < term.length) {
          term.bytes = ArrayUtil.grow(term.bytes, term.length);
        }
        System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);

        if (isSubBlock && StringHelper.startsWith(target, term)) {
          // Recurse
          //if (DEBUG) System.out.println("      recurse!");
          currentFrame = pushFrame(getState());
          break;
        } else {
          final int cmp = term.compareTo(target);
          if (cmp < 0) {
            if (currentFrame.nextEnt == currentFrame.entCount) {
              if (!currentFrame.isLastInFloor) {
                //if (DEBUG) System.out.println("  load floorBlock");
                currentFrame.loadNextFloorBlock();
                continue;
              } else {
                //if (DEBUG) System.out.println("  return term=" + brToString(term));
                return;
              }
            }
            continue;
          } else if (cmp == 0) {
            //if (DEBUG) System.out.println("  return term=" + brToString(term));
            return;
          } else {
            // Fallback to prior entry: the semantics of
            // this method is that the first call to
            // next() will return the term after the
            // requested term
            currentFrame.nextEnt--;
            currentFrame.lastSubFP = saveLastSubFP;
            currentFrame.startBytePos = saveStartBytePos;
            currentFrame.suffix = saveSuffix;
            currentFrame.suffixesReader.setPosition(savePos);
            currentFrame.termState.termBlockOrd = saveTermBlockOrd;
            System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);
            term.length = currentFrame.prefix + currentFrame.suffix;
            // If the last entry was a block we don't
            // need to bother recursing and pushing to
            // the last term under it because the first
            // next() will simply skip the frame anyway
            return;
          }
        }
      }
    }

    assert false;
  }

  @Override
  public BytesRef next() throws IOException {

    // if (DEBUG) {
    //   System.out.println("\nintEnum.next seg=" + segment);
    //   System.out.println("  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
    // }

    nextTerm:
    while(true) {
      // Pop finished frames
      while (currentFrame.nextEnt == currentFrame.entCount) {
        if (!currentFrame.isLastInFloor) {
          //if (DEBUG) System.out.println("    next-floor-block");
          currentFrame.loadNextFloorBlock();
          //if (DEBUG) System.out.println("\n  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
        } else {
          //if (DEBUG) System.out.println("  pop frame");
          if (currentFrame.ord == 0) {
            return null;
          }
          final long lastFP = currentFrame.fpOrig;
          currentFrame = stack[currentFrame.ord-1];
          assert currentFrame.lastSubFP == lastFP;
          //if (DEBUG) System.out.println("\n  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
        }
      }

      final boolean isSubBlock = currentFrame.next();
      // if (DEBUG) {
      //   final BytesRef suffixRef = new BytesRef();
      //   suffixRef.bytes = currentFrame.suffixBytes;
      //   suffixRef.offset = currentFrame.startBytePos;
      //   suffixRef.length = currentFrame.suffix;
      //   System.out.println("    " + (isSubBlock ? "sub-block" : "term") + " " + currentFrame.nextEnt + " (of " + currentFrame.entCount + ") suffix=" + brToString(suffixRef));
      // }

      if (currentFrame.suffix != 0) {
        final int label = currentFrame.suffixBytes[currentFrame.startBytePos] & 0xff;
        while (label > currentFrame.curTransitionMax) {
          if (currentFrame.transitionIndex >= currentFrame.transitionCount-1) {
            // Stop processing this frame -- no further
            // matches are possible because we've moved
            // beyond what the max transition will allow
            //if (DEBUG) System.out.println("      break: trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]));

            // sneaky!  forces a pop above
            currentFrame.isLastInFloor = true;
            currentFrame.nextEnt = currentFrame.entCount;
            continue nextTerm;
          }
          currentFrame.transitionIndex++;
          compiledAutomaton.automaton.getNextTransition(currentFrame.transition);
          currentFrame.curTransitionMax = currentFrame.transition.max;
          //if (DEBUG) System.out.println("      next trans=" + currentFrame.transitions[currentFrame.transitionIndex]);
        }
      }

      // First test the common suffix, if set:
      if (compiledAutomaton.commonSuffixRef != null && !isSubBlock) {
        final int termLen = currentFrame.prefix + currentFrame.suffix;
        if (termLen < compiledAutomaton.commonSuffixRef.length) {
          // No match
          // if (DEBUG) {
          //   System.out.println("      skip: common suffix length");
          // }
          continue nextTerm;
        }

        final byte[] suffixBytes = currentFrame.suffixBytes;
        final byte[] commonSuffixBytes = compiledAutomaton.commonSuffixRef.bytes;

        final int lenInPrefix = compiledAutomaton.commonSuffixRef.length - currentFrame.suffix;
        assert compiledAutomaton.commonSuffixRef.offset == 0;
        int suffixBytesPos;
        int commonSuffixBytesPos = 0;

        if (lenInPrefix > 0) {
          // A prefix of the common suffix overlaps with
          // the suffix of the block prefix so we first
          // test whether the prefix part matches:
          final byte[] termBytes = term.bytes;
          int termBytesPos = currentFrame.prefix - lenInPrefix;
          assert termBytesPos >= 0;
          final int termBytesPosEnd = currentFrame.prefix;
          while (termBytesPos < termBytesPosEnd) {
            if (termBytes[termBytesPos++] != commonSuffixBytes[commonSuffixBytesPos++]) {
              // if (DEBUG) {
              //   System.out.println("      skip: common suffix mismatch (in prefix)");
              // }
              continue nextTerm;
            }
          }
          suffixBytesPos = currentFrame.startBytePos;
        } else {
          suffixBytesPos = currentFrame.startBytePos + currentFrame.suffix - compiledAutomaton.commonSuffixRef.length;
        }

        // Test overlapping suffix part:
        final int commonSuffixBytesPosEnd = compiledAutomaton.commonSuffixRef.length;
        while (commonSuffixBytesPos < commonSuffixBytesPosEnd) {
          if (suffixBytes[suffixBytesPos++] != commonSuffixBytes[commonSuffixBytesPos++]) {
            // if (DEBUG) {
            //   System.out.println("      skip: common suffix mismatch");
            // }
            continue nextTerm;
          }
        }
      }

      // TODO: maybe we should do the same linear test
      // that AutomatonTermsEnum does, so that if we
      // reach a part of the automaton where .* is
      // "temporarily" accepted, we just blindly .next()
      // until the limit

      // See if the term prefix matches the automaton:
      int state = currentFrame.state;
      for (int idx=0;idx<currentFrame.suffix;idx++) {
        state = runAutomaton.step(state,  currentFrame.suffixBytes[currentFrame.startBytePos+idx] & 0xff);
        if (state == -1) {
          // No match
          //System.out.println("    no s=" + state);
          continue nextTerm;
        } else {
          //System.out.println("    c s=" + state);
        }
      }

      if (isSubBlock) {
        // Match!  Recurse:
        //if (DEBUG) System.out.println("      sub-block match to state=" + state + "; recurse fp=" + currentFrame.lastSubFP);
        copyTerm();
        currentFrame = pushFrame(state);
        //if (DEBUG) System.out.println("\n  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
      } else if (runAutomaton.isAccept(state)) {
        copyTerm();
        //if (DEBUG) System.out.println("      term match to state=" + state + "; return term=" + brToString(term));
        assert savedStartTerm == null || term.compareTo(savedStartTerm) > 0: "saveStartTerm=" + savedStartTerm.utf8ToString() + " term=" + term.utf8ToString();
        return term;
      } else {
        //System.out.println("    no s=" + state);
      }
    }
  }

  private void copyTerm() {
    //System.out.println("      copyTerm cur.prefix=" + currentFrame.prefix + " cur.suffix=" + currentFrame.suffix + " first=" + (char) currentFrame.suffixBytes[currentFrame.startBytePos]);
    final int len = currentFrame.prefix + currentFrame.suffix;
    if (term.bytes.length < len) {
      term.bytes = ArrayUtil.grow(term.bytes, len);
    }
    System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);
    term.length = len;
  }

  @Override
  public boolean seekExact(BytesRef text) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekExact(long ord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long ord() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) {
    throw new UnsupportedOperationException();
  }
}
