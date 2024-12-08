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
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.FST.Arc.BitTable;

import java.io.IOException;

/**
 * Can next() and advance() through the terms in an FST
 */
abstract class FSTEnum<T> {
    protected final FST<T> fst;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected FST.Arc<T>[] arcs = new FST.Arc[10];
    // outputs are cumulative
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected T[] output = (T[]) new Object[10];

    protected final T NO_OUTPUT;
    protected final FST.BytesReader fstReader;

    protected int upto;
    int targetLength;

    /**
     * doFloor controls the behavior of advance: if it's true doFloor is true, advance positions to
     * the biggest term before target.
     */
    FSTEnum(FST<T> fst) {
        this.fst = fst;
        fstReader = fst.getBytesReader();
        NO_OUTPUT = fst.outputs.getNoOutput();
        fst.getFirstArc(getArc(0));
        output[0] = NO_OUTPUT;
    }

    protected abstract int getTargetLabel();

    protected abstract int getCurrentLabel();

    protected abstract void setCurrentLabel(int label);

    protected abstract void grow();

    /** Rewinds enum state to match the shared prefix between current term and target term */
    private void rewindPrefix() throws IOException {
        if (upto == 0) {
            // System.out.println(" init");
            upto = 1;
            fst.readFirstTargetArc(getArc(0), getArc(1), fstReader);
            return;
        }
        // System.out.println(" rewind upto=" + upto + " vs targetLength=" + targetLength);

        final int currentLimit = upto;
        upto = 1;
        while (upto < currentLimit && upto <= targetLength + 1) {
            final int cmp = getCurrentLabel() - getTargetLabel();
            if (cmp < 0) {
                // seek forward
                // System.out.println(" seek fwd");
                break;
            } else if (cmp > 0) {
                // seek backwards -- reset this arc to the first arc
                final FST.Arc<T> arc = getArc(upto);
                fst.readFirstTargetArc(getArc(upto - 1), arc, fstReader);
                // System.out.println(" seek first arc");
                break;
            }
            upto++;
        }
        // System.out.println(" fall through upto=" + upto);
    }

    protected void doNext() throws IOException {
        // System.out.println("FE: next upto=" + upto);
        if (upto == 0) {
            // System.out.println(" init");
            upto = 1;
            fst.readFirstTargetArc(getArc(0), getArc(1), fstReader);
        } else {
            // pop
            // System.out.println(" check pop curArc target=" + arcs[upto].target + " label=" +
            // arcs[upto].label + " isLast?=" + arcs[upto].isLast());
            while (arcs[upto].isLast()) {
                upto--;
                if (upto == 0) {
                    // System.out.println(" eof");
                    return;
                }
            }
            fst.readNextArc(arcs[upto], fstReader);
        }

        pushFirst();
    }

    // TODO: should we return a status here (SEEK_FOUND / SEEK_NOT_FOUND /
    // SEEK_END)? saves the eq check above?

    /** Seeks to smallest term that's &gt;= target. */
    protected void doSeekCeil() throws IOException {

        // System.out.println(" advance len=" + target.length + " curlen=" + current.length);

        // TODO: possibly caller could/should provide common
        // prefix length? ie this work may be redundant if
        // caller is in fact intersecting against its own
        // automaton

        // System.out.println("FE.seekCeil upto=" + upto);

        // Save time by starting at the end of the shared prefix
        // b/w our current term & the target:
        rewindPrefix();
        // System.out.println(" after rewind upto=" + upto);

        FST.Arc<T> arc = getArc(upto);
        // System.out.println(" init targetLabel=" + targetLabel);

        // Now scan forward, matching the new suffix of the target
        while (arc != null) {
            int targetLabel = getTargetLabel();
            // System.out.println(" cycle upto=" + upto + " arc.label=" + arc.label + " (" + (char)
            // arc.label + ") vs targetLabel=" + targetLabel);
            if (arc.bytesPerArc() != 0 && arc.label() != FST.END_LABEL) {
                // Arcs are in an array
                final FST.BytesReader in = fst.getBytesReader();
                if (arc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING) {
                    arc = doSeekCeilArrayDirectAddressing(arc, targetLabel, in);
                } else {
                    assert arc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH;
                    arc = doSeekCeilArrayPacked(arc, targetLabel, in);
                }
            } else {
                arc = doSeekCeilList(arc, targetLabel);
            }
        }
    }

    private FST.Arc<T> doSeekCeilArrayDirectAddressing(final FST.Arc<T> arc, final int targetLabel, final FST.BytesReader in)
        throws IOException {
        // The array is addressed directly by label, with presence bits to compute the actual arc
        // offset.

        int targetIndex = targetLabel - arc.firstLabel();
        if (targetIndex >= arc.numArcs()) {
            // Target is beyond the last arc, out of label range.
            // Dead end (target is after the last arc);
            // rollback to last fork then push
            upto--;
            while (true) {
                if (upto == 0) {
                    return null;
                }
                final FST.Arc<T> prevArc = getArc(upto);
                // System.out.println(" rollback upto=" + upto + " arc.label=" + prevArc.label + "
                // isLast?=" + prevArc.isLast());
                if (prevArc.isLast() == false) {
                    fst.readNextArc(prevArc, fstReader);
                    pushFirst();
                    return null;
                }
                upto--;
            }
        } else {
            if (targetIndex < 0) {
                targetIndex = -1;
            } else if (BitTable.isBitSet(targetIndex, arc, in)) {
                fst.readArcByDirectAddressing(arc, in, targetIndex);
                assert arc.label() == targetLabel;
                // found -- copy pasta from below
                output[upto] = fst.outputs.add(output[upto - 1], arc.output());
                if (targetLabel == FST.END_LABEL) {
                    return null;
                }
                setCurrentLabel(arc.label());
                incr();
                return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
            }
            // Not found, return the next arc (ceil).
            int ceilIndex = BitTable.nextBitSet(targetIndex, arc, in);
            assert ceilIndex != -1;
            fst.readArcByDirectAddressing(arc, in, ceilIndex);
            assert arc.label() > targetLabel;
            pushFirst();
            return null;
        }
    }

    private FST.Arc<T> doSeekCeilArrayPacked(final FST.Arc<T> arc, final int targetLabel, final FST.BytesReader in) throws IOException {
        // The array is packed -- use binary search to find the target.
        int idx = Util.binarySearch(fst, arc, targetLabel);
        if (idx >= 0) {
            // Match
            fst.readArcByIndex(arc, in, idx);
            assert arc.arcIdx() == idx;
            assert arc.label() == targetLabel : "arc.label=" + arc.label() + " vs targetLabel=" + targetLabel + " mid=" + idx;
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (targetLabel == FST.END_LABEL) {
                return null;
            }
            setCurrentLabel(arc.label());
            incr();
            return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
        }
        idx = -1 - idx;
        if (idx == arc.numArcs()) {
            // Dead end
            fst.readArcByIndex(arc, in, idx - 1);
            assert arc.isLast();
            // Dead end (target is after the last arc);
            // rollback to last fork then push
            upto--;
            while (true) {
                if (upto == 0) {
                    return null;
                }
                final FST.Arc<T> prevArc = getArc(upto);
                // System.out.println(" rollback upto=" + upto + " arc.label=" + prevArc.label + "
                // isLast?=" + prevArc.isLast());
                if (prevArc.isLast() == false) {
                    fst.readNextArc(prevArc, fstReader);
                    pushFirst();
                    return null;
                }
                upto--;
            }
        } else {
            // Ceiling - arc with least higher label
            fst.readArcByIndex(arc, in, idx);
            assert arc.label() > targetLabel;
            pushFirst();
            return null;
        }
    }

    private FST.Arc<T> doSeekCeilList(final FST.Arc<T> arc, final int targetLabel) throws IOException {
        // Arcs are not array'd -- must do linear scan:
        if (arc.label() == targetLabel) {
            // recurse
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (targetLabel == FST.END_LABEL) {
                return null;
            }
            setCurrentLabel(arc.label());
            incr();
            return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
        } else if (arc.label() > targetLabel) {
            pushFirst();
            return null;
        } else if (arc.isLast()) {
            // Dead end (target is after the last arc);
            // rollback to last fork then push
            upto--;
            while (true) {
                if (upto == 0) {
                    return null;
                }
                final FST.Arc<T> prevArc = getArc(upto);
                // System.out.println(" rollback upto=" + upto + " arc.label=" + prevArc.label + "
                // isLast?=" + prevArc.isLast());
                if (prevArc.isLast() == false) {
                    fst.readNextArc(prevArc, fstReader);
                    pushFirst();
                    return null;
                }
                upto--;
            }
        } else {
            // keep scanning
            // System.out.println(" next scan");
            fst.readNextArc(arc, fstReader);
        }
        return arc;
    }

    // Todo: should we return a status here (SEEK_FOUND / SEEK_NOT_FOUND /
    // SEEK_END)? saves the eq check above?
    /** Seeks to largest term that's &lt;= target. */
    void doSeekFloor() throws IOException {

        // TODO: possibly caller could/should provide common
        // prefix length? ie this work may be redundant if
        // caller is in fact intersecting against its own
        // automaton
        // System.out.println("FE: seek floor upto=" + upto);

        // Save CPU by starting at the end of the shared prefix
        // b/w our current term & the target:
        rewindPrefix();

        // System.out.println("FE: after rewind upto=" + upto);

        FST.Arc<T> arc = getArc(upto);

        // System.out.println("FE: init targetLabel=" + targetLabel);

        // Now scan forward, matching the new suffix of the target
        while (arc != null) {
            // System.out.println(" cycle upto=" + upto + " arc.label=" + arc.label + " (" + (char)
            // arc.label + ") targetLabel=" + targetLabel + " isLast?=" + arc.isLast() + " bba=" +
            // arc.bytesPerArc);
            int targetLabel = getTargetLabel();

            if (arc.bytesPerArc() != 0 && arc.label() != FST.END_LABEL) {
                // Arcs are in an array
                final FST.BytesReader in = fst.getBytesReader();
                if (arc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING) {
                    arc = doSeekFloorArrayDirectAddressing(arc, targetLabel, in);
                } else {
                    assert arc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH;
                    arc = doSeekFloorArrayPacked(arc, targetLabel, in);
                }
            } else {
                arc = doSeekFloorList(arc, targetLabel);
            }
        }
    }

    private FST.Arc<T> doSeekFloorArrayDirectAddressing(FST.Arc<T> arc, int targetLabel, FST.BytesReader in) throws IOException {
        // The array is addressed directly by label, with presence bits to compute the actual arc
        // offset.

        int targetIndex = targetLabel - arc.firstLabel();
        if (targetIndex < 0) {
            // Before first arc.
            return backtrackToFloorArc(arc, targetLabel, in);
        } else if (targetIndex >= arc.numArcs()) {
            // After last arc.
            fst.readLastArcByDirectAddressing(arc, in);
            assert arc.label() < targetLabel;
            assert arc.isLast();
            pushLast();
            return null;
        } else {
            // Within label range.
            if (BitTable.isBitSet(targetIndex, arc, in)) {
                fst.readArcByDirectAddressing(arc, in, targetIndex);
                assert arc.label() == targetLabel;
                // found -- copy pasta from below
                output[upto] = fst.outputs.add(output[upto - 1], arc.output());
                if (targetLabel == FST.END_LABEL) {
                    return null;
                }
                setCurrentLabel(arc.label());
                incr();
                return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
            }
            // Scan backwards to find a floor arc.
            int floorIndex = BitTable.previousBitSet(targetIndex, arc, in);
            assert floorIndex != -1;
            fst.readArcByDirectAddressing(arc, in, floorIndex);
            assert arc.label() < targetLabel;
            assert arc.isLast() || fst.readNextArcLabel(arc, in) > targetLabel;
            pushLast();
            return null;
        }
    }

    /**
     * Backtracks until it finds a node which first arc is before our target label.` Then on the node,
     * finds the arc just before the targetLabel.
     *
     * @return null to continue the seek floor recursion loop.
     */
    private FST.Arc<T> backtrackToFloorArc(FST.Arc<T> arc, int targetLabel, final FST.BytesReader in) throws IOException {
        while (true) {
            // First, walk backwards until we find a node which first arc is before our target label.
            fst.readFirstTargetArc(getArc(upto - 1), arc, fstReader);
            if (arc.label() < targetLabel) {
                // Then on this node, find the arc just before the targetLabel.
                if (arc.isLast() == false) {
                    if (arc.bytesPerArc() != 0 && arc.label() != FST.END_LABEL) {
                        if (arc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH) {
                            findNextFloorArcBinarySearch(arc, targetLabel, in);
                        } else {
                            assert arc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING;
                            findNextFloorArcDirectAddressing(arc, targetLabel, in);
                        }
                    } else {
                        while (arc.isLast() == false && fst.readNextArcLabel(arc, in) < targetLabel) {
                            fst.readNextArc(arc, fstReader);
                        }
                    }
                }
                assert arc.label() < targetLabel;
                assert arc.isLast() || fst.readNextArcLabel(arc, in) >= targetLabel;
                pushLast();
                return null;
            }
            upto--;
            if (upto == 0) {
                return null;
            }
            targetLabel = getTargetLabel();
            arc = getArc(upto);
        }
    }

    /**
     * Finds and reads an arc on the current node which label is strictly less than the given label.
     * Skips the first arc, finds next floor arc; or none if the floor arc is the first arc itself (in
     * this case it has already been read).
     *
     * <p>Precondition: the given arc is the first arc of the node.
     */
    private void findNextFloorArcDirectAddressing(FST.Arc<T> arc, int targetLabel, final FST.BytesReader in) throws IOException {
        assert arc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING;
        assert arc.label() != FST.END_LABEL;
        assert arc.label() == arc.firstLabel();
        if (arc.numArcs() > 1) {
            int targetIndex = targetLabel - arc.firstLabel();
            assert targetIndex >= 0;
            if (targetIndex >= arc.numArcs()) {
                // Beyond last arc. Take last arc.
                fst.readLastArcByDirectAddressing(arc, in);
            } else {
                // Take the preceding arc, even if the target is present.
                int floorIndex = BitTable.previousBitSet(targetIndex, arc, in);
                if (floorIndex > 0) {
                    fst.readArcByDirectAddressing(arc, in, floorIndex);
                }
            }
        }
    }

    /** Same as {@link #findNextFloorArcDirectAddressing} for binary search node. */
    private void findNextFloorArcBinarySearch(FST.Arc<T> arc, int targetLabel, FST.BytesReader in) throws IOException {
        assert arc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH;
        assert arc.label() != FST.END_LABEL;
        assert arc.arcIdx() == 0;
        if (arc.numArcs() > 1) {
            int idx = Util.binarySearch(fst, arc, targetLabel);
            assert idx != -1;
            if (idx > 1) {
                fst.readArcByIndex(arc, in, idx - 1);
            } else if (idx < -2) {
                fst.readArcByIndex(arc, in, -2 - idx);
            }
        }
    }

    private FST.Arc<T> doSeekFloorArrayPacked(FST.Arc<T> arc, int targetLabel, final FST.BytesReader in) throws IOException {
        // Arcs are fixed array -- use binary search to find the target.
        int idx = Util.binarySearch(fst, arc, targetLabel);

        if (idx >= 0) {
            // Match -- recurse
            // System.out.println(" match! arcIdx=" + idx);
            fst.readArcByIndex(arc, in, idx);
            assert arc.arcIdx() == idx;
            assert arc.label() == targetLabel : "arc.label=" + arc.label() + " vs targetLabel=" + targetLabel + " mid=" + idx;
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (targetLabel == FST.END_LABEL) {
                return null;
            }
            setCurrentLabel(arc.label());
            incr();
            return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
        } else if (idx == -1) {
            // Before first arc.
            return backtrackToFloorArc(arc, targetLabel, in);
        } else {
            // There is a floor arc; idx will be (-1 - (floor + 1)).
            fst.readArcByIndex(arc, in, -2 - idx);
            assert arc.isLast() || fst.readNextArcLabel(arc, in) > targetLabel;
            assert arc.label() < targetLabel : "arc.label=" + arc.label() + " vs targetLabel=" + targetLabel;
            pushLast();
            return null;
        }
    }

    private FST.Arc<T> doSeekFloorList(FST.Arc<T> arc, int targetLabel) throws IOException {
        if (arc.label() == targetLabel) {
            // Match -- recurse
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (targetLabel == FST.END_LABEL) {
                return null;
            }
            setCurrentLabel(arc.label());
            incr();
            return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
        } else if (arc.label() > targetLabel) {
            // TODO: if each arc could somehow read the arc just
            // before, we can save this re-scan. The ceil case
            // doesn't need this because it reads the next arc
            // instead:
            while (true) {
                // First, walk backwards until we find a first arc
                // that's before our target label:
                fst.readFirstTargetArc(getArc(upto - 1), arc, fstReader);
                if (arc.label() < targetLabel) {
                    // Then, scan forwards to the arc just before
                    // the targetLabel:
                    while (arc.isLast() == false && fst.readNextArcLabel(arc, fstReader) < targetLabel) {
                        fst.readNextArc(arc, fstReader);
                    }
                    pushLast();
                    return null;
                }
                upto--;
                if (upto == 0) {
                    return null;
                }
                targetLabel = getTargetLabel();
                arc = getArc(upto);
            }
        } else if (arc.isLast() == false) {
            // System.out.println(" check next label=" + fst.readNextArcLabel(arc) + " (" + (char)
            // fst.readNextArcLabel(arc) + ")");
            if (fst.readNextArcLabel(arc, fstReader) > targetLabel) {
                pushLast();
                return null;
            } else {
                // keep scanning
                return fst.readNextArc(arc, fstReader);
            }
        } else {
            pushLast();
            return null;
        }
    }

    /** Seeks to exactly target term. */
    boolean doSeekExact() throws IOException {

        // TODO: possibly caller could/should provide common
        // prefix length? ie this work may be redundant if
        // caller is in fact intersecting against its own
        // automaton

        // System.out.println("FE: seek exact upto=" + upto);

        // Save time by starting at the end of the shared prefix
        // b/w our current term & the target:
        rewindPrefix();

        // System.out.println("FE: after rewind upto=" + upto);
        FST.Arc<T> arc = getArc(upto - 1);
        int targetLabel = getTargetLabel();

        final FST.BytesReader fstReader = fst.getBytesReader();

        while (true) {
            // System.out.println(" cycle target=" + (targetLabel == -1 ? "-1" : (char) targetLabel));
            final FST.Arc<T> nextArc = fst.findTargetArc(targetLabel, arc, getArc(upto), fstReader);
            if (nextArc == null) {
                // short circuit
                // upto--;
                // upto = 0;
                fst.readFirstTargetArc(arc, getArc(upto), fstReader);
                // System.out.println(" no match upto=" + upto);
                return false;
            }
            // Match -- recurse:
            output[upto] = fst.outputs.add(output[upto - 1], nextArc.output());
            if (targetLabel == FST.END_LABEL) {
                // System.out.println(" return found; upto=" + upto + " output=" + output[upto] + "
                // nextArc=" + nextArc.isLast());
                return true;
            }
            setCurrentLabel(targetLabel);
            incr();
            targetLabel = getTargetLabel();
            arc = nextArc;
        }
    }

    private void incr() {
        upto++;
        grow();
        if (arcs.length <= upto) {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            final FST.Arc<T>[] newArcs = new FST.Arc[ArrayUtil.oversize(1 + upto, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
            System.arraycopy(arcs, 0, newArcs, 0, arcs.length);
            arcs = newArcs;
        }
        if (output.length <= upto) {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            final T[] newOutput = (T[]) new Object[ArrayUtil.oversize(1 + upto, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
            System.arraycopy(output, 0, newOutput, 0, output.length);
            output = newOutput;
        }
    }

    // Appends current arc, and then recurses from its target,
    // appending first arc all the way to the final node
    private void pushFirst() throws IOException {

        FST.Arc<T> arc = arcs[upto];
        assert arc != null;

        while (true) {
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (arc.label() == FST.END_LABEL) {
                // Final node
                break;
            }
            // System.out.println(" pushFirst label=" + (char) arc.label + " upto=" + upto + " output=" +
            // fst.outputs.outputToString(output[upto]));
            setCurrentLabel(arc.label());
            incr();

            final FST.Arc<T> nextArc = getArc(upto);
            fst.readFirstTargetArc(arc, nextArc, fstReader);
            arc = nextArc;
        }
    }

    // Recurses from current arc, appending last arc all the
    // way to the first final node
    private void pushLast() throws IOException {

        FST.Arc<T> arc = arcs[upto];
        assert arc != null;

        while (true) {
            setCurrentLabel(arc.label());
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (arc.label() == FST.END_LABEL) {
                // Final node
                break;
            }
            incr();

            arc = fst.readLastTargetArc(arc, getArc(upto), fstReader);
        }
    }

    private FST.Arc<T> getArc(int idx) {
        if (arcs[idx] == null) {
            arcs[idx] = new FST.Arc<>();
        }
        return arcs[idx];
    }
}
