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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.FST.Arc;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.FST.Arc.BitTable;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.FST.BytesReader;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Static helper methods.
 *
 */
public final class Util {
    private Util() {}

    /** Looks up the output for this input, or null if the input is not accepted. */
    public static <T> T get(FST<T> fst, IntsRef input) throws IOException {

        // TODO: would be nice not to alloc this on every lookup
        final Arc<T> arc = fst.getFirstArc(new Arc<>());

        final BytesReader fstReader = fst.getBytesReader();

        // Accumulate output as we go
        T output = fst.outputs.getNoOutput();
        for (int i = 0; i < input.length; i++) {
            if (fst.findTargetArc(input.ints[input.offset + i], arc, arc, fstReader) == null) {
                return null;
            }
            output = fst.outputs.add(output, arc.output());
        }

        if (arc.isFinal()) {
            return fst.outputs.add(output, arc.nextFinalOutput());
        } else {
            return null;
        }
    }

    // TODO: maybe a CharsRef version for BYTE2

    /** Looks up the output for this input, or null if the input is not accepted */
    public static <T> T get(FST<T> fst, BytesRef input) throws IOException {
        assert fst.inputType == FST.INPUT_TYPE.BYTE1;

        final BytesReader fstReader = fst.getBytesReader();

        // TODO: would be nice not to alloc this on every lookup
        final Arc<T> arc = fst.getFirstArc(new Arc<>());

        // Accumulate output as we go
        T output = fst.outputs.getNoOutput();
        for (int i = 0; i < input.length; i++) {
            if (fst.findTargetArc(input.bytes[i + input.offset] & 0xFF, arc, arc, fstReader) == null) {
                return null;
            }
            output = fst.outputs.add(output, arc.output());
        }

        if (arc.isFinal()) {
            return fst.outputs.add(output, arc.nextFinalOutput());
        } else {
            return null;
        }
    }

    /**
     * Represents a path in TopNSearcher.
     *
     */
    public static class FSTPath<T> {
        /** Holds the last arc appended to this path */
        public Arc<T> arc;
        /** Holds cost plus any usage-specific output: */
        public T output;

        public final IntsRefBuilder input;
        public final float boost;
        public final CharSequence context;

        // Custom int payload for consumers; the NRT suggester uses this to record if this path has
        // already enumerated a surface form
        public int payload;

        FSTPath(T output, Arc<T> arc, IntsRefBuilder input, float boost, CharSequence context, int payload) {
            this.arc = new Arc<T>().copyFrom(arc);
            this.output = output;
            this.input = input;
            this.boost = boost;
            this.context = context;
            this.payload = payload;
        }

        FSTPath<T> newPath(T output, IntsRefBuilder input) {
            return new FSTPath<>(output, this.arc, input, this.boost, this.context, this.payload);
        }

        @Override
        public String toString() {
            return "input=" + input.get() + " output=" + output + " context=" + context + " boost=" + boost + " payload=" + payload;
        }
    }

    /** Compares first by the provided comparator, and then tie breaks by path.input. */
    private static class TieBreakByInputComparator<T> implements Comparator<FSTPath<T>> {
        private final Comparator<T> comparator;

        TieBreakByInputComparator(Comparator<T> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(FSTPath<T> a, FSTPath<T> b) {
            int cmp = comparator.compare(a.output, b.output);
            if (cmp == 0) {
                return a.input.get().compareTo(b.input.get());
            } else {
                return cmp;
            }
        }
    }

    /** Utility class to find top N shortest paths from start point(s). */
    public static class TopNSearcher<T> {

        private final FST<T> fst;
        private final BytesReader bytesReader;
        private final int topN;
        private final int maxQueueDepth;

        private final Arc<T> scratchArc = new Arc<>();

        private final Comparator<T> comparator;
        private final Comparator<FSTPath<T>> pathComparator;

        TreeSet<FSTPath<T>> queue;

        /**
         * Creates an unbounded TopNSearcher
         *
         * @param fst the {@link FST} to search on
         * @param topN the number of top scoring entries to retrieve
         * @param maxQueueDepth the maximum size of the queue of possible top entries
         * @param comparator the comparator to select the top N
         */
        public TopNSearcher(FST<T> fst, int topN, int maxQueueDepth, Comparator<T> comparator) {
            this(fst, topN, maxQueueDepth, comparator, new TieBreakByInputComparator<>(comparator));
        }

        public TopNSearcher(FST<T> fst, int topN, int maxQueueDepth, Comparator<T> comparator, Comparator<FSTPath<T>> pathComparator) {
            this.fst = fst;
            this.bytesReader = fst.getBytesReader();
            this.topN = topN;
            this.maxQueueDepth = maxQueueDepth;
            this.comparator = comparator;
            this.pathComparator = pathComparator;
            queue = new TreeSet<>(pathComparator);
        }

        // If back plus this arc is competitive then add to queue:
        protected void addIfCompetitive(FSTPath<T> path) {

            assert queue != null;

            T output = fst.outputs.add(path.output, path.arc.output());

            if (queue.size() == maxQueueDepth) {
                FSTPath<T> bottom = queue.last();
                int comp = pathComparator.compare(path, bottom);
                if (comp > 0) {
                    // Doesn't compete
                    return;
                } else if (comp == 0) {
                    // Tie break by alpha sort on the input:
                    path.input.append(path.arc.label());
                    final int cmp = bottom.input.get().compareTo(path.input.get());
                    path.input.setLength(path.input.length() - 1);

                    // We should never see dups:
                    assert cmp != 0;

                    if (cmp < 0) {
                        // Doesn't compete
                        return;
                    }
                }
                // Competes
            }
            // else ... Queue isn't full yet, so any path we hit competes:

            // copy over the current input to the new input
            // and add the arc.label to the end
            IntsRefBuilder newInput = new IntsRefBuilder();
            newInput.copyInts(path.input.get());
            newInput.append(path.arc.label());

            FSTPath<T> newPath = path.newPath(output, newInput);
            if (acceptPartialPath(newPath)) {
                queue.add(newPath);
                if (queue.size() == maxQueueDepth + 1) {
                    queue.pollLast();
                }
            }
        }

        public void addStartPaths(Arc<T> node, T startOutput, boolean allowEmptyString, IntsRefBuilder input) throws IOException {
            addStartPaths(node, startOutput, allowEmptyString, input, 0, null, -1);
        }

        /**
         * Adds all leaving arcs, including 'finished' arc, if the node is final, from this node into
         * the queue.
         */
        public void addStartPaths(
            Arc<T> node,
            T startOutput,
            boolean allowEmptyString,
            IntsRefBuilder input,
            float boost,
            CharSequence context,
            int payload
        ) throws IOException {

            // De-dup NO_OUTPUT since it must be a singleton:
            if (startOutput.equals(fst.outputs.getNoOutput())) {
                startOutput = fst.outputs.getNoOutput();
            }

            FSTPath<T> path = new FSTPath<>(startOutput, node, input, boost, context, payload);
            fst.readFirstTargetArc(node, path.arc, bytesReader);

            // Bootstrap: find the min starting arc
            while (true) {
                if (allowEmptyString || path.arc.label() != FST.END_LABEL) {
                    addIfCompetitive(path);
                }
                if (path.arc.isLast()) {
                    break;
                }
                fst.readNextArc(path.arc, bytesReader);
            }
        }

        public TopResults<T> search() throws IOException {

            final List<Result<T>> results = new ArrayList<>();

            final BytesReader fstReader = fst.getBytesReader();
            final T NO_OUTPUT = fst.outputs.getNoOutput();

            // TODO: we could enable FST to sorting arcs by weight
            // as it freezes... can easily do this on first pass
            // (w/o requiring rewrite)

            // TODO: maybe we should make an FST.INPUT_TYPE.BYTE0.5!?
            // (nibbles)
            int rejectCount = 0;

            // For each top N path:
            while (results.size() < topN) {

                FSTPath<T> path;

                if (queue == null) {
                    // Ran out of paths
                    break;
                }

                // Remove top path since we are now going to
                // pursue it:
                path = queue.pollFirst();

                if (path == null) {
                    // There were less than topN paths available:
                    break;
                }
                // System.out.println("pop path=" + path + " arc=" + path.arc.output);

                if (acceptPartialPath(path) == false) {
                    continue;
                }

                if (path.arc.label() == FST.END_LABEL) {
                    // Empty string!
                    path.input.setLength(path.input.length() - 1);
                    results.add(new Result<>(path.input.get(), path.output));
                    continue;
                }

                if (results.size() == topN - 1 && maxQueueDepth == topN) {
                    // Last path -- don't bother w/ queue anymore:
                    queue = null;
                }

                // We take path and find its "0 output completion",
                // ie, just keep traversing the first arc with
                // NO_OUTPUT that we can find, since this must lead
                // to the minimum path that completes from
                // path.arc.

                // For each input letter:
                while (true) {

                    fst.readFirstTargetArc(path.arc, path.arc, fstReader);

                    // For each arc leaving this node:
                    boolean foundZero = false;
                    boolean arcCopyIsPending = false;
                    while (true) {
                        // tricky: instead of comparing output == 0, we must
                        // express it via the comparator compare(output, 0) == 0
                        if (comparator.compare(NO_OUTPUT, path.arc.output()) == 0) {
                            if (queue == null) {
                                foundZero = true;
                                break;
                            } else if (foundZero == false) {
                                arcCopyIsPending = true;
                                foundZero = true;
                            } else {
                                addIfCompetitive(path);
                            }
                        } else if (queue != null) {
                            addIfCompetitive(path);
                        }
                        if (path.arc.isLast()) {
                            break;
                        }
                        if (arcCopyIsPending) {
                            scratchArc.copyFrom(path.arc);
                            arcCopyIsPending = false;
                        }
                        fst.readNextArc(path.arc, fstReader);
                    }

                    assert foundZero;

                    if (queue != null && arcCopyIsPending == false) {
                        path.arc.copyFrom(scratchArc);
                    }

                    if (path.arc.label() == FST.END_LABEL) {
                        // Add final output:
                        path.output = fst.outputs.add(path.output, path.arc.output());
                        if (acceptResult(path)) {
                            results.add(new Result<>(path.input.get(), path.output));
                        } else {
                            rejectCount++;
                        }
                        break;
                    } else {
                        path.input.append(path.arc.label());
                        path.output = fst.outputs.add(path.output, path.arc.output());
                        if (acceptPartialPath(path) == false) {
                            break;
                        }
                    }
                }
            }
            return new TopResults<>(rejectCount + topN <= maxQueueDepth, results);
        }

        protected boolean acceptResult(FSTPath<T> path) {
            return acceptResult(path.input.get(), path.output);
        }

        /** Override this to prevent considering a path before it's complete */
        protected boolean acceptPartialPath(FSTPath<T> path) {
            return true;
        }

        protected boolean acceptResult(IntsRef input, T output) {
            return true;
        }
    }

    /**
     * Holds a single input (IntsRef) + output, returned by {@link #shortestPaths shortestPaths()}.
     */
    public static final class Result<T> {
        public final IntsRef input;
        public final T output;

        public Result(IntsRef input, T output) {
            this.input = input;
            this.output = output;
        }
    }

    /** Holds the results for a top N search using {@link TopNSearcher} */
    public static final class TopResults<T> implements Iterable<Result<T>> {

        /**
         * <code>true</code> iff this is a complete result ie. if the specified queue size was large
         * enough to find the complete list of results. This might be <code>false</code> if the {@link
         * TopNSearcher} rejected too many results.
         */
        public final boolean isComplete;
        /** The top results */
        public final List<Result<T>> topN;

        TopResults(boolean isComplete, List<Result<T>> topN) {
            this.topN = topN;
            this.isComplete = isComplete;
        }

        @Override
        public Iterator<Result<T>> iterator() {
            return topN.iterator();
        }
    }

    /** Starting from node, find the top N min cost completions to a final node. */
    public static <T> TopResults<T> shortestPaths(
        FST<T> fst,
        Arc<T> fromNode,
        T startOutput,
        Comparator<T> comparator,
        int topN,
        boolean allowEmptyString
    ) throws IOException {

        // All paths are kept, so we can pass topN for
        // maxQueueDepth and the pruning is admissible:
        TopNSearcher<T> searcher = new TopNSearcher<>(fst, topN, topN, comparator);

        // since this search is initialized with a single start node
        // it is okay to start with an empty input path here
        searcher.addStartPaths(fromNode, startOutput, allowEmptyString, new IntsRefBuilder());
        return searcher.search();
    }

    /**
     * Dumps an {@link FST} to a GraphViz's <code>dot</code> language description for visualization.
     * Example of use:
     *
     * <pre class="prettyprint">
     * PrintWriter pw = new PrintWriter(&quot;out.dot&quot;);
     * Util.toDot(fst, pw, true, true);
     * pw.close();
     * </pre>
     *
     * and then, from command line:
     *
     * <pre>
     * dot -Tpng -o out.png out.dot
     * </pre>
     *
     * <p>Note: larger FSTs (a few thousand nodes) won't even render, don't bother.
     *
     * @param sameRank If <code>true</code>, the resulting <code>dot</code> file will try to order
     *     states in layers of breadth-first traversal. This may mess up arcs, but makes the output
     *     FST's structure a bit clearer.
     * @param labelStates If <code>true</code> states will have labels equal to their offsets in their
     *     binary format. Expands the graph considerably.
     * @see <a href="http://www.graphviz.org/">graphviz project</a>
     */
    public static <T> void toDot(FST<T> fst, Writer out, boolean sameRank, boolean labelStates) throws IOException {
        final String expandedNodeColor = "blue";

        // This is the start arc in the automaton (from the epsilon state to the first state
        // with outgoing transitions.
        final Arc<T> startArc = fst.getFirstArc(new Arc<>());

        // A queue of transitions to consider for the next level.
        final List<Arc<T>> thisLevelQueue = new ArrayList<>();

        // A queue of transitions to consider when processing the next level.
        final List<Arc<T>> nextLevelQueue = new ArrayList<>();
        nextLevelQueue.add(startArc);
        // System.out.println("toDot: startArc: " + startArc);

        // A list of states on the same level (for ranking).
        final List<Integer> sameLevelStates = new ArrayList<>();

        // A bitset of already seen states (target offset).
        final BitSet seen = new BitSet();
        seen.set((int) startArc.target());

        // Shape for states.
        final String stateShape = "circle";
        final String finalStateShape = "doublecircle";

        // Emit DOT prologue.
        out.write("digraph FST {\n");
        out.write("  rankdir = LR; splines=true; concentrate=true; ordering=out; ranksep=2.5; \n");

        if (labelStates == false) {
            out.write("  node [shape=circle, width=.2, height=.2, style=filled]\n");
        }

        emitDotState(out, "initial", "point", "white", "");

        final T NO_OUTPUT = fst.outputs.getNoOutput();
        final BytesReader r = fst.getBytesReader();

        // final FST.Arc<T> scratchArc = new FST.Arc<>();

        {
            final String stateColor;
            if (fst.isExpandedTarget(startArc, r)) {
                stateColor = expandedNodeColor;
            } else {
                stateColor = null;
            }

            final boolean isFinal;
            final T finalOutput;
            if (startArc.isFinal()) {
                isFinal = true;
                finalOutput = startArc.nextFinalOutput() == NO_OUTPUT ? null : startArc.nextFinalOutput();
            } else {
                isFinal = false;
                finalOutput = null;
            }

            emitDotState(
                out,
                Long.toString(startArc.target()),
                isFinal ? finalStateShape : stateShape,
                stateColor,
                finalOutput == null ? "" : fst.outputs.outputToString(finalOutput)
            );
        }

        out.write("  initial -> " + startArc.target() + "\n");

        int level = 0;

        while (nextLevelQueue.isEmpty() == false) {
            // we could double buffer here, but it doesn't matter probably.
            // System.out.println("next level=" + level);
            thisLevelQueue.addAll(nextLevelQueue);
            nextLevelQueue.clear();

            level++;
            out.write("\n  // Transitions and states at level: " + level + "\n");
            while (thisLevelQueue.isEmpty() == false) {
                final Arc<T> arc = thisLevelQueue.remove(thisLevelQueue.size() - 1);
                // System.out.println(" pop: " + arc);
                if (FST.targetHasArcs(arc)) {
                    // scan all target arcs
                    // System.out.println(" readFirstTarget...");

                    final long node = arc.target();

                    fst.readFirstRealTargetArc(arc.target(), arc, r);

                    // System.out.println(" firstTarget: " + arc);

                    while (true) {

                        // System.out.println(" cycle arc=" + arc);
                        // Emit the unseen state and add it to the queue for the next level.
                        if (arc.target() >= 0 && seen.get((int) arc.target()) == false) {

                            /*
                            boolean isFinal = false;
                            T finalOutput = null;
                            fst.readFirstTargetArc(arc, scratchArc);
                            if (scratchArc.isFinal() && fst.targetHasArcs(scratchArc)) {
                              // target is final
                              isFinal = true;
                              finalOutput = scratchArc.output == NO_OUTPUT ? null : scratchArc.output;
                              System.out.println("dot hit final label=" + (char) scratchArc.label);
                            }
                            */
                            final String stateColor;
                            if (fst.isExpandedTarget(arc, r)) {
                                stateColor = expandedNodeColor;
                            } else {
                                stateColor = null;
                            }

                            final String finalOutput;
                            if (arc.nextFinalOutput() != null && arc.nextFinalOutput() != NO_OUTPUT) {
                                finalOutput = fst.outputs.outputToString(arc.nextFinalOutput());
                            } else {
                                finalOutput = "";
                            }

                            emitDotState(out, Long.toString(arc.target()), stateShape, stateColor, finalOutput);
                            // To see the node address, use this instead:
                            // emitDotState(out, Integer.toString(arc.target), stateShape, stateColor,
                            // String.valueOf(arc.target));
                            seen.set((int) arc.target());
                            nextLevelQueue.add(new Arc<T>().copyFrom(arc));
                            sameLevelStates.add((int) arc.target());
                        }

                        String outs;
                        if (arc.output() != NO_OUTPUT) {
                            outs = "/" + fst.outputs.outputToString(arc.output());
                        } else {
                            outs = "";
                        }

                        if (FST.targetHasArcs(arc) == false && arc.isFinal() && arc.nextFinalOutput() != NO_OUTPUT) {
                            // Tricky special case: sometimes, due to
                            // pruning, the builder can [sillily] produce
                            // an FST with an arc into the final end state
                            // (-1) but also with a next final output; in
                            // this case we pull that output up onto this
                            // arc
                            outs = outs + "/[" + fst.outputs.outputToString(arc.nextFinalOutput()) + "]";
                        }

                        final String arcColor;
                        if (arc.flag(FST.BIT_TARGET_NEXT)) {
                            arcColor = "red";
                        } else {
                            arcColor = "black";
                        }

                        assert arc.label() != FST.END_LABEL;
                        out.write(
                            "  "
                                + node
                                + " -> "
                                + arc.target()
                                + " [label=\""
                                + printableLabel(arc.label())
                                + outs
                                + "\""
                                + (arc.isFinal() ? " style=\"bold\"" : "")
                                + " color=\""
                                + arcColor
                                + "\"]\n"
                        );

                        // Break the loop if we're on the last arc of this state.
                        if (arc.isLast()) {
                            // System.out.println(" break");
                            break;
                        }
                        fst.readNextRealArc(arc, r);
                    }
                }
            }

            // Emit state ranking information.
            if (sameRank && sameLevelStates.size() > 1) {
                out.write("  {rank=same; ");
                for (int state : sameLevelStates) {
                    out.write(state + "; ");
                }
                out.write(" }\n");
            }
            sameLevelStates.clear();
        }

        // Emit terminating state (always there anyway).
        out.write("  -1 [style=filled, color=black, shape=doublecircle, label=\"\"]\n\n");
        out.write("  {rank=sink; -1 }\n");

        out.write("}\n");
        out.flush();
    }

    /** Emit a single state in the <code>dot</code> language. */
    private static void emitDotState(Writer out, String name, String shape, String color, String label) throws IOException {
        out.write(
            "  "
                + name
                + " ["
                + (shape != null ? "shape=" + shape : "")
                + " "
                + (color != null ? "color=" + color : "")
                + " "
                + (label != null ? "label=\"" + label + "\"" : "label=\"\"")
                + " "
                + "]\n"
        );
    }

    /** Ensures an arc's label is indeed printable (dot uses US-ASCII). */
    private static String printableLabel(int label) {
        // Any ordinary ascii character, except for " or \, are
        // printed as the character; else, as a hex string:
        if (label >= 0x20 && label <= 0x7d && label != 0x22 && label != 0x5c) { // " OR \
            return Character.toString((char) label);
        }
        return "0x" + Integer.toHexString(label);
    }

    /** Just maps each UTF16 unit (char) to the ints in an IntsRef. */
    public static IntsRef toUTF16(CharSequence s, IntsRefBuilder scratch) {
        final int charLimit = s.length();
        scratch.setLength(charLimit);
        scratch.grow(charLimit);
        for (int idx = 0; idx < charLimit; idx++) {
            scratch.setIntAt(idx, s.charAt(idx));
        }
        return scratch.get();
    }

    /**
     * Decodes the Unicode codepoints from the provided CharSequence and places them in the provided
     * scratch IntsRef, which must not be null, returning it.
     */
    public static IntsRef toUTF32(CharSequence s, IntsRefBuilder scratch) {
        int charIdx = 0;
        int intIdx = 0;
        final int charLimit = s.length();
        while (charIdx < charLimit) {
            scratch.grow(intIdx + 1);
            final int utf32 = Character.codePointAt(s, charIdx);
            scratch.setIntAt(intIdx, utf32);
            charIdx += Character.charCount(utf32);
            intIdx++;
        }
        scratch.setLength(intIdx);
        return scratch.get();
    }

    /**
     * Decodes the Unicode codepoints from the provided char[] and places them in the provided scratch
     * IntsRef, which must not be null, returning it.
     */
    public static IntsRef toUTF32(char[] s, int offset, int length, IntsRefBuilder scratch) {
        int charIdx = offset;
        int intIdx = 0;
        final int charLimit = offset + length;
        while (charIdx < charLimit) {
            scratch.grow(intIdx + 1);
            final int utf32 = Character.codePointAt(s, charIdx, charLimit);
            scratch.setIntAt(intIdx, utf32);
            charIdx += Character.charCount(utf32);
            intIdx++;
        }
        scratch.setLength(intIdx);
        return scratch.get();
    }

    /** Just takes unsigned byte values from the BytesRef and converts into an IntsRef. */
    public static IntsRef toIntsRef(BytesRef input, IntsRefBuilder scratch) {
        scratch.clear();
        for (int i = 0; i < input.length; i++) {
            scratch.append(input.bytes[i + input.offset] & 0xFF);
        }
        return scratch.get();
    }

    /** Just converts IntsRef to BytesRef; you must ensure the int values fit into a byte. */
    public static BytesRef toBytesRef(IntsRef input, BytesRefBuilder scratch) {
        scratch.grow(input.length);
        for (int i = 0; i < input.length; i++) {
            int value = input.ints[i + input.offset];
            // NOTE: we allow -128 to 255
            assert value >= Byte.MIN_VALUE && value <= 255 : "value " + value + " doesn't fit into byte";
            scratch.setByteAt(i, (byte) value);
        }
        scratch.setLength(input.length);
        return scratch.get();
    }

    // Uncomment for debugging:

    /*
    public static <T> void dotToFile(FST<T> fst, String filePath) throws IOException {
    Writer w = new OutputStreamWriter(new FileOutputStream(filePath));
    toDot(fst, w, true, true);
    w.close();
    }
    */

    /**
     * Reads the first arc greater or equal than the given label into the provided arc in place and
     * returns it iff found, otherwise return <code>null</code>.
     *
     * @param label the label to ceil on
     * @param fst the fst to operate on
     * @param follow the arc to follow reading the label from
     * @param arc the arc to read into in place
     * @param in the fst's {@link BytesReader}
     */
    public static <T> Arc<T> readCeilArc(int label, FST<T> fst, Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
        if (label == FST.END_LABEL) {
            return FST.readEndArc(follow, arc);
        }
        if (FST.targetHasArcs(follow) == false) {
            return null;
        }
        fst.readFirstTargetArc(follow, arc, in);
        if (arc.bytesPerArc() != 0 && arc.label() != FST.END_LABEL) {
            if (arc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING) {
                // Fixed length arcs in a direct addressing node.
                int targetIndex = label - arc.label();
                if (targetIndex >= arc.numArcs()) {
                    return null;
                } else if (targetIndex < 0) {
                    return arc;
                } else {
                    if (BitTable.isBitSet(targetIndex, arc, in)) {
                        fst.readArcByDirectAddressing(arc, in, targetIndex);
                        assert arc.label() == label;
                    } else {
                        int ceilIndex = BitTable.nextBitSet(targetIndex, arc, in);
                        assert ceilIndex != -1;
                        fst.readArcByDirectAddressing(arc, in, ceilIndex);
                        assert arc.label() > label;
                    }
                    return arc;
                }
            }
            // Fixed length arcs in a binary search node.
            int idx = binarySearch(fst, arc, label);
            if (idx >= 0) {
                return fst.readArcByIndex(arc, in, idx);
            }
            idx = -1 - idx;
            if (idx == arc.numArcs()) {
                // DEAD END!
                return null;
            }
            return fst.readArcByIndex(arc, in, idx);
        }

        // Variable length arcs in a linear scan list,
        // or special arc with label == FST.END_LABEL.
        fst.readFirstRealTargetArc(follow.target(), arc, in);

        while (true) {
            // System.out.println(" non-bs cycle");
            if (arc.label() >= label) {
                // System.out.println(" found!");
                return arc;
            } else if (arc.isLast()) {
                return null;
            } else {
                fst.readNextRealArc(arc, in);
            }
        }
    }

    /**
     * Perform a binary search of Arcs encoded as a packed array
     *
     * @param fst the FST from which to read
     * @param arc the starting arc; sibling arcs greater than this will be searched. Usually the first
     *     arc in the array.
     * @param targetLabel the label to search for
     * @param <T> the output type of the FST
     * @return the index of the Arc having the target label, or if no Arc has the matching label,
     *     {@code -1 - idx)}, where {@code idx} is the index of the Arc with the next highest label,
     *     or the total number of arcs if the target label exceeds the maximum.
     * @throws IOException when the FST reader does
     */
    static <T> int binarySearch(FST<T> fst, Arc<T> arc, int targetLabel) throws IOException {
        assert arc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH
            : "Arc is not encoded as packed array for binary search (nodeFlags=" + arc.nodeFlags() + ")";
        BytesReader in = fst.getBytesReader();
        int low = arc.arcIdx();
        int mid;
        int high = arc.numArcs() - 1;
        while (low <= high) {
            mid = (low + high) >>> 1;
            in.setPosition(arc.posArcsStart());
            in.skipBytes((long) arc.bytesPerArc() * mid + 1);
            final int midLabel = fst.readLabel(in);
            final int cmp = midLabel - targetLabel;
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -1 - low;
    }
}
