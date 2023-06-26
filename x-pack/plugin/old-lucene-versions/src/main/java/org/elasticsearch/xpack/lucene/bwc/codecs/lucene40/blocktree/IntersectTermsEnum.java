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

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RunAutomaton;
import org.apache.lucene.util.automaton.Transition;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.ByteSequenceOutputs;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.FST;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.Outputs;

import java.io.IOException;

/**
 * This is used to implement efficient {@link Terms#intersect} for block-tree. Note that it cannot
 * seek, except for the initial term on init. It just "nexts" through the intersection of the
 * automaton and the terms. It does not use the terms index at all: on init, it loads the root
 * block, and scans its way to the initial term. Likewise, in next it scans until it finds a term
 * that matches the current automaton transition.
 */
final class IntersectTermsEnum extends BaseTermsEnum {

    // static boolean DEBUG = BlockTreeTermsWriter.DEBUG;

    final IndexInput in;
    static final Outputs<BytesRef> fstOutputs = ByteSequenceOutputs.getSingleton();

    IntersectTermsEnumFrame[] stack;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private FST.Arc<BytesRef>[] arcs = new FST.Arc[5];

    final RunAutomaton runAutomaton;
    final Automaton automaton;
    final BytesRef commonSuffix;

    private IntersectTermsEnumFrame currentFrame;
    private Transition currentTransition;

    private final BytesRef term = new BytesRef();

    private final FST.BytesReader fstReader;

    final FieldReader fr;

    private BytesRef savedStartTerm;

    // TODO: in some cases we can filter by length? eg
    // regexp foo*bar must be at least length 6 bytes
    IntersectTermsEnum(FieldReader fr, Automaton automaton, RunAutomaton runAutomaton, BytesRef commonSuffix, BytesRef startTerm)
        throws IOException {
        this.fr = fr;

        assert automaton != null;
        assert runAutomaton != null;

        this.runAutomaton = runAutomaton;
        this.automaton = automaton;
        this.commonSuffix = commonSuffix;

        in = fr.parent.termsIn.clone();
        stack = new IntersectTermsEnumFrame[5];
        for (int idx = 0; idx < stack.length; idx++) {
            stack[idx] = new IntersectTermsEnumFrame(this, idx);
        }
        for (int arcIdx = 0; arcIdx < arcs.length; arcIdx++) {
            arcs[arcIdx] = new FST.Arc<>();
        }

        fstReader = fr.index.getBytesReader();

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
        f.setState(0);
        f.arc = arc;
        f.outputPrefix = arc.output();
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
            final IntersectTermsEnumFrame[] next = new IntersectTermsEnumFrame[ArrayUtil.oversize(
                1 + ord,
                RamUsageEstimator.NUM_BYTES_OBJECT_REF
            )];
            System.arraycopy(stack, 0, next, 0, stack.length);
            for (int stackOrd = stack.length; stackOrd < next.length; stackOrd++) {
                next[stackOrd] = new IntersectTermsEnumFrame(this, stackOrd);
            }
            stack = next;
        }
        assert stack[ord].ord == ord;
        return stack[ord];
    }

    private FST.Arc<BytesRef> getArc(int ord) {
        if (ord >= arcs.length) {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            final FST.Arc<BytesRef>[] next = new FST.Arc[ArrayUtil.oversize(1 + ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
            System.arraycopy(arcs, 0, next, 0, arcs.length);
            for (int arcOrd = arcs.length; arcOrd < next.length; arcOrd++) {
                next[arcOrd] = new FST.Arc<>();
            }
            arcs = next;
        }
        return arcs[ord];
    }

    private IntersectTermsEnumFrame pushFrame(int state) throws IOException {
        assert currentFrame != null;

        final IntersectTermsEnumFrame f = getFrame(currentFrame == null ? 0 : 1 + currentFrame.ord);

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
            arc = fr.index.findTargetArc(target, arc, getArc(1 + idx), fstReader);
            assert arc != null;
            output = fstOutputs.add(output, arc.output());
            idx++;
        }

        f.arc = arc;
        f.outputPrefix = output;
        assert arc.isFinal();
        f.load(fstOutputs.add(output, arc.nextFinalOutput()));
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

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        currentFrame.decodeMetaData();
        return fr.parent.postingsReader.impacts(fr.fieldInfo, currentFrame.termState, flags);
    }

    private int getState() {
        int state = currentFrame.state;
        for (int idx = 0; idx < currentFrame.suffix; idx++) {
            state = runAutomaton.step(state, currentFrame.suffixBytes[currentFrame.startBytePos + idx] & 0xff);
            assert state != -1;
        }
        return state;
    }

    // NOTE: specialized to only doing the first-time
    // seek, but we could generalize it to allow
    // arbitrary seekExact/Ceil. Note that this is a
    // seekFloor!
    private void seekToStartTerm(BytesRef target) throws IOException {
        assert currentFrame.ord == 0;
        if (term.length < target.length) {
            term.bytes = ArrayUtil.grow(term.bytes, target.length);
        }
        FST.Arc<BytesRef> arc = arcs[0];
        assert arc == currentFrame.arc;

        for (int idx = 0; idx <= target.length; idx++) {

            while (true) {
                final int savNextEnt = currentFrame.nextEnt;
                final int savePos = currentFrame.suffixesReader.getPosition();
                final int saveLengthPos = currentFrame.suffixLengthsReader.getPosition();
                final int saveStartBytePos = currentFrame.startBytePos;
                final int saveSuffix = currentFrame.suffix;
                final long saveLastSubFP = currentFrame.lastSubFP;
                final int saveTermBlockOrd = currentFrame.termState.termBlockOrd;

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
                            if (currentFrame.isLastInFloor == false) {
                                // Advance to next floor block
                                currentFrame.loadNextFloorBlock();
                                continue;
                            } else {
                                return;
                            }
                        }
                        continue;
                    } else if (cmp == 0) {
                        return;
                    } else {
                        // Fallback to prior entry: the semantics of
                        // this method is that the first call to
                        // next() will return the term after the
                        // requested term
                        currentFrame.nextEnt = savNextEnt;
                        currentFrame.lastSubFP = saveLastSubFP;
                        currentFrame.startBytePos = saveStartBytePos;
                        currentFrame.suffix = saveSuffix;
                        currentFrame.suffixesReader.setPosition(savePos);
                        currentFrame.suffixLengthsReader.setPosition(saveLengthPos);
                        currentFrame.termState.termBlockOrd = saveTermBlockOrd;
                        System.arraycopy(
                            currentFrame.suffixBytes,
                            currentFrame.startBytePos,
                            term.bytes,
                            currentFrame.prefix,
                            currentFrame.suffix
                        );
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
            if (currentFrame.isLastInFloor == false) {
                // Advance to next floor block
                currentFrame.loadNextFloorBlock();
                break;
            } else {
                if (currentFrame.ord == 0) {
                    throw NoMoreTermsException.INSTANCE;
                }
                final long lastFP = currentFrame.fpOrig;
                currentFrame = stack[currentFrame.ord - 1];
                currentTransition = currentFrame.transition;
                assert currentFrame.lastSubFP == lastFP;
            }
        }

        return currentFrame.next();
    }

    // Only used internally when there are no more terms in next():
    private static final class NoMoreTermsException extends RuntimeException {

        // Only used internally when there are no more terms in next():
        public static final NoMoreTermsException INSTANCE = new NoMoreTermsException();

        private NoMoreTermsException() {}

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
        } catch (@SuppressWarnings("unused") NoMoreTermsException eoi) {
            // Provoke NPE if we are (illegally!) called again:
            currentFrame = null;
            return null;
        }
    }

    private BytesRef _next() throws IOException {

        boolean isSubBlock = popPushNext();

        nextTerm: while (true) {
            assert currentFrame.transition == currentTransition;

            int state;
            int lastState;

            // NOTE: suffix == 0 can only happen on the first term in a block, when
            // there is a term exactly matching a prefix in the index. If we
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
                    if (currentFrame.transitionIndex >= currentFrame.transitionCount - 1) {
                        // Pop this frame: no further matches are possible because
                        // we've moved beyond what the max transition will allow
                        if (currentFrame.ord == 0) {
                            // Provoke NPE if we are (illegally!) called again:
                            currentFrame = null;
                            return null;
                        }
                        currentFrame = stack[currentFrame.ord - 1];
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

                if (commonSuffix != null && isSubBlock == false) {
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
                for (int idx = currentFrame.startBytePos + 1; idx < end; idx++) {
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
                // Match! Recurse:
                copyTerm();
                currentFrame = pushFrame(state);
                currentTransition = currentFrame.transition;
                currentFrame.lastState = lastState;
            } else if (runAutomaton.isAccept(state)) {
                copyTerm();
                assert savedStartTerm == null || term.compareTo(savedStartTerm) > 0
                    : "saveStartTerm=" + savedStartTerm.utf8ToString() + " term=" + term.utf8ToString();
                return term;
            } else {
                // This term is a prefix of a term accepted by the automaton, but is not itself accepted
            }

            isSubBlock = popPushNext();
        }
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
