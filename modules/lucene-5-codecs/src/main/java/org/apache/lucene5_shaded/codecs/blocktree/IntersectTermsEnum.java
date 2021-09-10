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

import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.TermState;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.StringHelper;
import org.apache.lucene5_shaded.util.automaton.Automaton;
import org.apache.lucene5_shaded.util.automaton.RunAutomaton;
import org.apache.lucene5_shaded.util.automaton.Transition;
import org.apache.lucene5_shaded.util.fst.ByteSequenceOutputs;
import org.apache.lucene5_shaded.util.fst.FST;
import org.apache.lucene5_shaded.util.fst.Outputs;

/** This is used to implement efficient {@link Terms#intersect} for
 *  block-tree.  Note that it cannot seek, except for the initial term on
 *  init.  It just "nexts" through the intersection of the automaton and
 *  the terms.  It does not use the terms index at all: on init, it
 *  loads the root block, and scans its way to the initial term.
 *  Likewise, in next it scans until it finds a term that matches the
 *  current automaton transition.  If the index has auto-prefix terms
 *  (only for DOCS_ONLY fields currently) it will visit these terms
 *  when possible and then skip the real terms that auto-prefix term
 *  matched. */

final class IntersectTermsEnum extends TermsEnum {

  //static boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  final IndexInput in;
  final static Outputs<BytesRef> fstOutputs = ByteSequenceOutputs.getSingleton();

  IntersectTermsEnumFrame[] stack;
      
  @SuppressWarnings({"rawtypes","unchecked"}) private FST.Arc<BytesRef>[] arcs = new FST.Arc[5];

  final RunAutomaton runAutomaton;
  final Automaton automaton;
  final BytesRef commonSuffix;

  private IntersectTermsEnumFrame currentFrame;
  private Transition currentTransition;

  private final BytesRef term = new BytesRef();

  private final FST.BytesReader fstReader;

  private final boolean allowAutoPrefixTerms;

  final FieldReader fr;

  /** Which state in the automaton accepts all possible suffixes. */
  private final int sinkState;

  private BytesRef savedStartTerm;
      
  /** True if we did return the current auto-prefix term */
  private boolean useAutoPrefixTerm;

  // TODO: in some cases we can filter by length?  eg
  // regexp foo*bar must be at least length 6 bytes
  public IntersectTermsEnum(FieldReader fr, Automaton automaton, RunAutomaton runAutomaton, BytesRef commonSuffix, BytesRef startTerm, int sinkState) throws IOException {
    this.fr = fr;
    this.sinkState = sinkState;

    assert automaton != null;
    assert runAutomaton != null;

    this.runAutomaton = runAutomaton;
    this.allowAutoPrefixTerms = sinkState != -1;
    this.automaton = automaton;
    this.commonSuffix = commonSuffix;

    in = fr.parent.termsIn.clone();
    stack = new IntersectTermsEnumFrame[5];
    for(int idx=0;idx<stack.length;idx++) {
      stack[idx] = new IntersectTermsEnumFrame(this, idx);
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
    final IntersectTermsEnumFrame f = stack[0];
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
    currentTransition = currentFrame.transition;
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

  private IntersectTermsEnumFrame getFrame(int ord) throws IOException {
    if (ord >= stack.length) {
      final IntersectTermsEnumFrame[] next = new IntersectTermsEnumFrame[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(stack, 0, next, 0, stack.length);
      for(int stackOrd=stack.length;stackOrd<next.length;stackOrd++) {
        next[stackOrd] = new IntersectTermsEnumFrame(this, stackOrd);
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

  private IntersectTermsEnumFrame pushFrame(int state) throws IOException {
    assert currentFrame != null;

    final IntersectTermsEnumFrame f = getFrame(currentFrame == null ? 0 : 1+currentFrame.ord);
        
    f.fp = f.fpOrig = currentFrame.lastSubFP;
    f.prefix = currentFrame.prefix + currentFrame.suffix;
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
    currentFrame.decodeMetaData();
    return currentFrame.termState.docFreq;
  }

  @Override
  public long totalTermFreq() throws IOException {
    currentFrame.decodeMetaData();
    return currentFrame.termState.totalTermFreq;
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
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
    assert currentFrame.ord == 0;
    if (term.length < target.length) {
      term.bytes = ArrayUtil.grow(term.bytes, target.length);
    }
    FST.Arc<BytesRef> arc = arcs[0];
    assert arc == currentFrame.arc;

    for(int idx=0;idx<=target.length;idx++) {

      while (true) {
        final int savNextEnt = currentFrame.nextEnt;
        final int savePos = currentFrame.suffixesReader.getPosition();
        final int saveStartBytePos = currentFrame.startBytePos;
        final int saveSuffix = currentFrame.suffix;
        final long saveLastSubFP = currentFrame.lastSubFP;
        final int saveTermBlockOrd = currentFrame.termState.termBlockOrd;
        final boolean saveIsAutoPrefixTerm = currentFrame.isAutoPrefixTerm;

        final boolean isSubBlock = currentFrame.next();

        term.length = currentFrame.prefix + currentFrame.suffix;
        if (term.bytes.length < term.length) {
          term.bytes = ArrayUtil.grow(term.bytes, term.length);
        }
        System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);

        if (isSubBlock && StringHelper.startsWith(target, term)) {
          // Recurse
          currentFrame = pushFrame(getState());
          break;
        } else {
          final int cmp = term.compareTo(target);
          if (cmp < 0) {
            if (currentFrame.nextEnt == currentFrame.entCount) {
              if (!currentFrame.isLastInFloor) {
                // Advance to next floor block
                currentFrame.loadNextFloorBlock();
                continue;
              } else {
                return;
              }
            }
            continue;
          } else if (cmp == 0) {
            if (allowAutoPrefixTerms == false && currentFrame.isAutoPrefixTerm) {
              continue;
            }
            return;
          } else if (allowAutoPrefixTerms || currentFrame.isAutoPrefixTerm == false) {
            // Fallback to prior entry: the semantics of
            // this method is that the first call to
            // next() will return the term after the
            // requested term
            currentFrame.nextEnt = savNextEnt;
            currentFrame.lastSubFP = saveLastSubFP;
            currentFrame.startBytePos = saveStartBytePos;
            currentFrame.suffix = saveSuffix;
            currentFrame.suffixesReader.setPosition(savePos);
            currentFrame.termState.termBlockOrd = saveTermBlockOrd;
            currentFrame.isAutoPrefixTerm = saveIsAutoPrefixTerm;
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

  private boolean popPushNext() throws IOException {
    // Pop finished frames
    while (currentFrame.nextEnt == currentFrame.entCount) {
      if (!currentFrame.isLastInFloor) {
        // Advance to next floor block
        currentFrame.loadNextFloorBlock();
        break;
      } else {
        if (currentFrame.ord == 0) {
          throw NoMoreTermsException.INSTANCE;
        }
        final long lastFP = currentFrame.fpOrig;
        currentFrame = stack[currentFrame.ord-1];
        currentTransition = currentFrame.transition;
        assert currentFrame.lastSubFP == lastFP;
      }
    }

    return currentFrame.next();
  }

  private boolean skipPastLastAutoPrefixTerm() throws IOException {
    assert currentFrame.isAutoPrefixTerm;
    useAutoPrefixTerm = false;
    currentFrame.termState.isRealTerm = true;

    // If we last returned an auto-prefix term, we must now skip all
    // actual terms sharing that prefix.  At most, that skipping
    // requires popping one frame, but it can also require simply
    // scanning ahead within the current frame.  This scanning will
    // skip sub-blocks that contain many terms, which is why the
    // optimization "works":
    int floorSuffixLeadEnd = currentFrame.floorSuffixLeadEnd;

    boolean isSubBlock;

    if (floorSuffixLeadEnd == -1) {
      // An ordinary prefix, e.g. foo*
      int prefix = currentFrame.prefix;
      int suffix = currentFrame.suffix;
      if (suffix == 0) {

        // Easy case: the prefix term's suffix is the empty string,
        // meaning the prefix corresponds to all terms in the
        // current block, so we just pop this entire block:
        if (currentFrame.ord == 0) {
          throw NoMoreTermsException.INSTANCE;
        }
        currentFrame = stack[currentFrame.ord-1];
        currentTransition = currentFrame.transition;

        return popPushNext();

      } else {

        // Just next() until we hit an entry that doesn't share this
        // prefix.  The first next should be a sub-block sharing the
        // same prefix, because if there are enough terms matching a
        // given prefix to warrant an auto-prefix term, then there
        // must also be enough to make a sub-block (assuming
        // minItemsInPrefix > minItemsInBlock):
        scanPrefix:
        while (true) {
          if (currentFrame.nextEnt == currentFrame.entCount) {
            if (currentFrame.isLastInFloor == false) {
              currentFrame.loadNextFloorBlock();
            } else if (currentFrame.ord == 0) {
              throw NoMoreTermsException.INSTANCE;
            } else {
              // Pop frame, which also means we've moved beyond this
              // auto-prefix term:
              currentFrame = stack[currentFrame.ord-1];
              currentTransition = currentFrame.transition;

              return popPushNext();
            }
          }
          isSubBlock = currentFrame.next();
          for(int i=0;i<suffix;i++) {
            if (term.bytes[prefix+i] != currentFrame.suffixBytes[currentFrame.startBytePos+i]) {
              break scanPrefix;
            }
          }
        }
      }
    } else {
      // Floor'd auto-prefix term; in this case we must skip all
      // terms e.g. matching foo[a-m]*.  We are currently "on" fooa,
      // which the automaton accepted (fooa* through foom*), and
      // floorSuffixLeadEnd is m, so we must now scan to foon:
      int prefix = currentFrame.prefix;
      int suffix = currentFrame.suffix;

      if (currentFrame.floorSuffixLeadStart == -1) {
        suffix++;
      }

      if (suffix == 0) {

        // This means current frame is fooa*, so we have to first
        // pop the current frame, then scan in parent frame:
        if (currentFrame.ord == 0) {
          throw NoMoreTermsException.INSTANCE;
        }
        currentFrame = stack[currentFrame.ord-1];
        currentTransition = currentFrame.transition;

        // Current (parent) frame is now foo*, so now we just scan
        // until the lead suffix byte is > floorSuffixLeadEnd
        //assert currentFrame.prefix == prefix-1;
        //prefix = currentFrame.prefix;

        // In case when we pop, and the parent block is not just prefix-1, e.g. in block 417* on
        // its first term = floor prefix term 41[7-9], popping to block 4*:
        prefix = currentFrame.prefix;

        suffix = term.length - currentFrame.prefix;
      } else {
        // No need to pop; just scan in currentFrame:
      }

      // Now we scan until the lead suffix byte is > floorSuffixLeadEnd
      scanFloor:
      while (true) {
        if (currentFrame.nextEnt == currentFrame.entCount) {
          if (currentFrame.isLastInFloor == false) {
            currentFrame.loadNextFloorBlock();
          } else if (currentFrame.ord == 0) {
            throw NoMoreTermsException.INSTANCE;
          } else {
            // Pop frame, which also means we've moved beyond this
            // auto-prefix term:
            currentFrame = stack[currentFrame.ord-1];
            currentTransition = currentFrame.transition;

            return popPushNext();
          }
        }
        isSubBlock = currentFrame.next();
        for(int i=0;i<suffix-1;i++) {
          if (term.bytes[prefix+i] != currentFrame.suffixBytes[currentFrame.startBytePos+i]) {
            break scanFloor;
          }
        }
        if (currentFrame.suffix >= suffix && (currentFrame.suffixBytes[currentFrame.startBytePos+suffix-1]&0xff) > floorSuffixLeadEnd) {
          // Done scanning: we are now on the first term after all
          // terms matched by this auto-prefix term
          break;
        }
      }
    }

    return isSubBlock;
  }

  // Only used internally when there are no more terms in next():
  private static final class NoMoreTermsException extends RuntimeException {

    // Only used internally when there are no more terms in next():
    public static final NoMoreTermsException INSTANCE = new NoMoreTermsException();

    private NoMoreTermsException() {
    }

    @Override
    public Throwable fillInStackTrace() {
      // Do nothing:
      return this;
    }    
  }

  @Override
  public BytesRef next() throws IOException {
    try {
      return _next();
    } catch (NoMoreTermsException eoi) {
      // Provoke NPE if we are (illegally!) called again:
      currentFrame = null;
      return null;
    }
  }

  private BytesRef _next() throws IOException {

    boolean isSubBlock;

    if (useAutoPrefixTerm) {
      // If the current term was an auto-prefix term, we have to skip past it:
      isSubBlock = skipPastLastAutoPrefixTerm();
      assert useAutoPrefixTerm == false;
    } else {
      isSubBlock = popPushNext();
    }

    nextTerm:

    while (true) {
      assert currentFrame.transition == currentTransition;

      int state;
      int lastState;

      // NOTE: suffix == 0 can only happen on the first term in a block, when
      // there is a term exactly matching a prefix in the index.  If we
      // could somehow re-org the code so we only checked this case immediately
      // after pushing a frame...
      if (currentFrame.suffix != 0) {

        final byte[] suffixBytes = currentFrame.suffixBytes;

        // This is the first byte of the suffix of the term we are now on:
        final int label = suffixBytes[currentFrame.startBytePos] & 0xff;

        if (label < currentTransition.min) {
          // Common case: we are scanning terms in this block to "catch up" to
          // current transition in the automaton:
          int minTrans = currentTransition.min;
          while (currentFrame.nextEnt < currentFrame.entCount) {
            isSubBlock = currentFrame.next();
            if ((suffixBytes[currentFrame.startBytePos] & 0xff) >= minTrans) {
              continue nextTerm;
            }
          }

          // End of frame:
          isSubBlock = popPushNext();
          continue nextTerm;
        }

        // Advance where we are in the automaton to match this label:

        while (label > currentTransition.max) {
          if (currentFrame.transitionIndex >= currentFrame.transitionCount-1) {
            // Pop this frame: no further matches are possible because
            // we've moved beyond what the max transition will allow
            if (currentFrame.ord == 0) {
              // Provoke NPE if we are (illegally!) called again:
              currentFrame = null;
              return null;
            }
            currentFrame = stack[currentFrame.ord-1];
            currentTransition = currentFrame.transition;
            isSubBlock = popPushNext();
            continue nextTerm;
          }
          currentFrame.transitionIndex++;
          automaton.getNextTransition(currentTransition);

          if (label < currentTransition.min) {
            int minTrans = currentTransition.min;
            while (currentFrame.nextEnt < currentFrame.entCount) {
              isSubBlock = currentFrame.next();
              if ((suffixBytes[currentFrame.startBytePos] & 0xff) >= minTrans) {
                continue nextTerm;
              }
            }

            // End of frame:
            isSubBlock = popPushNext();
            continue nextTerm;
          }
        }

        if (commonSuffix != null && !isSubBlock) {
          final int termLen = currentFrame.prefix + currentFrame.suffix;
          if (termLen < commonSuffix.length) {
            // No match
            isSubBlock = popPushNext();
            continue nextTerm;
          }

          final byte[] commonSuffixBytes = commonSuffix.bytes;

          final int lenInPrefix = commonSuffix.length - currentFrame.suffix;
          assert commonSuffix.offset == 0;
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
                isSubBlock = popPushNext();
                continue nextTerm;
              }
            }
            suffixBytesPos = currentFrame.startBytePos;
          } else {
            suffixBytesPos = currentFrame.startBytePos + currentFrame.suffix - commonSuffix.length;
          }

          // Test overlapping suffix part:
          final int commonSuffixBytesPosEnd = commonSuffix.length;
          while (commonSuffixBytesPos < commonSuffixBytesPosEnd) {
            if (suffixBytes[suffixBytesPos++] != commonSuffixBytes[commonSuffixBytesPos++]) {
              isSubBlock = popPushNext();
              continue nextTerm;
            }
          }
        }

        // TODO: maybe we should do the same linear test
        // that AutomatonTermsEnum does, so that if we
        // reach a part of the automaton where .* is
        // "temporarily" accepted, we just blindly .next()
        // until the limit

        // See if the term suffix matches the automaton:

        // We know from above that the first byte in our suffix (label) matches
        // the current transition, so we step from the 2nd byte
        // in the suffix:
        lastState = currentFrame.state;
        state = currentTransition.dest;

        int end = currentFrame.startBytePos + currentFrame.suffix;
        for (int idx=currentFrame.startBytePos+1;idx<end;idx++) {
          lastState = state;
          state = runAutomaton.step(state, suffixBytes[idx] & 0xff);
          if (state == -1) {
            // No match
            isSubBlock = popPushNext();
            continue nextTerm;
          }
        }
      } else {
        state = currentFrame.state;
        lastState = currentFrame.lastState;
      }

      if (isSubBlock) {
        // Match!  Recurse:
        copyTerm();
        currentFrame = pushFrame(state);
        currentTransition = currentFrame.transition;
        currentFrame.lastState = lastState;
      } else if (currentFrame.isAutoPrefixTerm) {
        // We are on an auto-prefix term, meaning this term was compiled
        // at indexing time, matching all terms sharing this prefix (or,
        // a floor'd subset of them if that count was too high).  A
        // prefix term represents a range of terms, so we now need to
        // test whether, from the current state in the automaton, it
        // accepts all terms in that range.  As long as it does, we can
        // use this term and then later skip ahead past all terms in
        // this range:
        if (allowAutoPrefixTerms) {

          if (currentFrame.floorSuffixLeadEnd == -1) {
            // Simple prefix case
            useAutoPrefixTerm = state == sinkState;
          } else {
            if (currentFrame.floorSuffixLeadStart == -1) {
              // Must also accept the empty string in this case
              if (automaton.isAccept(state)) {
                useAutoPrefixTerm = acceptsSuffixRange(state, 0, currentFrame.floorSuffixLeadEnd);
              }
            } else {
              useAutoPrefixTerm = acceptsSuffixRange(lastState, currentFrame.floorSuffixLeadStart, currentFrame.floorSuffixLeadEnd);
            }
          }

          if (useAutoPrefixTerm) {
            // All suffixes of this auto-prefix term are accepted by the automaton, so we can use it:
            copyTerm();
            currentFrame.termState.isRealTerm = false;
            return term;
          } else {
            // We move onto the next term
          }
        } else {
          // We are not allowed to use auto-prefix terms, so we just skip it
        }
      } else if (runAutomaton.isAccept(state)) {
        copyTerm();
        assert savedStartTerm == null || term.compareTo(savedStartTerm) > 0: "saveStartTerm=" + savedStartTerm.utf8ToString() + " term=" + term.utf8ToString();
        return term;
      } else {
        // This term is a prefix of a term accepted by the automaton, but is not itself acceptd
      }

      isSubBlock = popPushNext();
    }
  }

  private final Transition scratchTransition = new Transition();

  /** Returns true if, from this state, the automaton accepts any suffix
   *  starting with a label between start and end, inclusive.  We just
   *  look for a transition, matching this range, to the sink state.  */
  private boolean acceptsSuffixRange(int state, int start, int end) {

    int count = automaton.initTransition(state, scratchTransition);
    for(int i=0;i<count;i++) {
      automaton.getNextTransition(scratchTransition);
      if (start >= scratchTransition.min && end <= scratchTransition.max && scratchTransition.dest == sinkState) {
        return true;
      }
    }

    return false;
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      // If BytesRef isn't actually UTF8, or it's eg a
      // prefix of UTF8 that ends mid-unicode-char, we
      // fallback to hex:
      return b.toString();
    }
  }

  private void copyTerm() {
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
