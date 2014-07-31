/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.search.suggest.analyzing;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.util.*;

/**
 * Generic TopNSearcher for {@link org.apache.lucene.util.fst.FST}
 *
 * Finds Top <code>topN</code> inputs that completes all the {@link #addStartPaths(FST.Arc, Object, boolean, IntsRefBuilder)}
 * for each input iteratively finds the top accepted <code>nLeaf</code> outputs
 *
 * For every input completion that reaches its <code>leafLabel</code>
 * {@link #onLeafNode(org.apache.lucene.util.IntsRef, Object)} is called once,
 * followed by {@link #collectOutput(Object)} called at most <code>nLeaf</code> +
 * number of rejected outputs. After at least <code>nLeaf</code> outputs are accepted
 * {@link #onFinishNode()} is called
 *
 */
public abstract class TopNSearcher<T> {

    // debug flag for logging
    private static final boolean DEBUG = false;

    private final FST<T> fst;
    private final FST.BytesReader bytesReader;
    private final int topN;
    private final int nLeaf;
    private final int leafLabel;
    private final int maxQueueDepth;

    private final FST.Arc<T> scratchArc = new FST.Arc<>();

    private final Comparator<T> comparator;

    private TreeSet<Util.FSTPath<T>> queue = null;
    private TreeSet<Util.FSTPath<T>> leafQueue = null;

    /**
     * Creates an unbounded TopNSearcher
     *
     * @param fst           the {@link org.apache.lucene.util.fst.FST} to search on
     * @param topN          the number of top scoring inputs to retrieve
     * @param nLeaf         the number of top scoring outputs to retrieve per input
     * @param leafLabel     the input label that marks the beginning of the output leaves
     * @param maxQueueDepth the maximum size of the queue of possible top entries
     * @param comparator    the comparator to select the top N
     */
    public TopNSearcher(FST<T> fst, int topN, int nLeaf, int leafLabel, int maxQueueDepth, Comparator<T> comparator) {
        this.fst = fst;
        this.bytesReader = fst.getBytesReader();
        this.topN = topN;
        this.nLeaf = nLeaf;
        this.leafLabel = leafLabel;
        this.maxQueueDepth = maxQueueDepth;
        this.comparator = comparator;

        queue = new TreeSet<>(new TieBreakByInputComparator<>(comparator));
        leafQueue = new TreeSet<>(new TieBreakByInputComparator<>(comparator));

    }

    //TODO: test and fix pruning (the chosen path is never put in the queue; make sure pruning works there)
    protected boolean prune(T pathCost) {
        return false;
    }

    // If back plus this arc is competitive then add to queue:
    protected void addIfCompetitive(Util.FSTPath<T> path) {

        assert queue != null;

        T cost = fst.outputs.add(path.cost, path.arc.output);
        if (DEBUG) {
            System.out.println("  addIfCompetitive queue.size()=" + queue.size() + " path=" + path.arc.label + " + label=" + path.arc.label);
        }

        if (prune(cost)) {
            return;
        }

        if (queue.size() == maxQueueDepth) {
            Util.FSTPath<T> bottom = queue.last();
            int comp = comparator.compare(cost, bottom.cost);
            if (comp > 0) {
                // Doesn't compete
                return;
            } else if (comp == 0) {
                // Tie break by alpha sort on the input:
                path.input.append(path.arc.label);
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
        } else {
            // Queue isn't full yet, so any path we hit competes:
        }

        // copy over the current input to the new input
        // and add the arc.label to the end
        IntsRefBuilder newInput = new IntsRefBuilder();
        newInput.copyInts(path.input.get());
        if (path.arc.label != leafLabel) {
            newInput.append(path.arc.label);
        }
        final Util.FSTPath<T> newPath = new Util.FSTPath<>(cost, path.arc, newInput);

        queue.add(newPath);

        if (queue.size() == maxQueueDepth + 1) {
            queue.pollLast();
        }
    }

    protected void addLeafPath(Util.FSTPath<T> path) {
        T cost = fst.outputs.add(path.cost, path.arc.output);
        IntsRefBuilder newInput = new IntsRefBuilder();
        newInput.copyInts(path.input.get());
        newInput.append(path.arc.label);
        final Util.FSTPath<T> newPath = new Util.FSTPath<>(cost, path.arc, newInput);

        leafQueue.add(newPath);
    }

    /**
     * Adds all leaving arcs, including 'finished' arc, if
     * the node is final, from this node into the queue.
     */
    public void addStartPaths(FST.Arc<T> node, T startOutput, boolean allowEmptyString, IntsRefBuilder input) throws IOException {

        // De-dup NO_OUTPUT since it must be a singleton:
        if (startOutput.equals(fst.outputs.getNoOutput())) {
            startOutput = fst.outputs.getNoOutput();
        }

        Util.FSTPath<T> path = new Util.FSTPath<>(startOutput, node, input);
        fst.readFirstTargetArc(node, path.arc, bytesReader);

        if (DEBUG) {
            System.out.println("add start paths");
        }

        // Bootstrap: find the min starting arc
        while (true) {
            if (allowEmptyString || path.arc.label != FST.END_LABEL) {
                addIfCompetitive(path);
            }
            if (path.arc.isLast()) {
                break;
            }
            fst.readNextArc(path.arc, bytesReader);
        }
    }


    /**
     * The results for the search can be collected using {@link #onLeafNode(org.apache.lucene.util.IntsRef, Object)},
     * {@link #collectOutput(Object)} and {@link #onFinishNode()}
     *
     * @return <code>true</code> iff this is a complete result ie. if
     * the specified queue size was large enough to find the complete list of results. This might
     * be <code>false</code> if the {@link TopNSearcher} rejected too many results.
     */
    public boolean search() throws IOException {

        int resultCount = 0;

        if (DEBUG) {
            System.out.println("search topN=" + topN);
        }
        final FST.BytesReader fstReader = fst.getBytesReader();
        final T NO_OUTPUT = fst.outputs.getNoOutput();

        // TODO: we could enable FST to sorting arcs by weight
        // as it freezes... can easily do this on first pass
        // (w/o requiring rewrite)

        // TODO: maybe we should make an FST.INPUT_TYPE.BYTE0.5!?
        // (nibbles)
        int rejectCount = 0;

        // For each top N path:
        while (resultCount < topN) {
            if (DEBUG) {
                System.out.println("\nfind next path: queue.size=" + queue.size());
            }

            Util.FSTPath<T> path;

            if (queue == null) {
                // Ran out of paths
                if (DEBUG) {
                    System.out.println("  break queue=null");
                }
                break;
            }

            // Remove top path since we are now going to
            // pursue it:
            path = queue.pollFirst();

            if (path == null) {
                // There were less than topN paths available:
                if (DEBUG) {
                    System.out.println("  break no more paths");
                }
                break;
            }

            if (path.arc.label == FST.END_LABEL) {
                if (DEBUG) {
                    System.out.println("    empty string!  cost=" + path.cost);
                }
                // Empty string!
                //path.input.setLength(path.input.length() - 1);
                continue;
            }

            if (resultCount == topN - 1 && maxQueueDepth == topN) {
                // Last path -- don't bother w/ queue anymore:
                queue = null;
            }

            if (DEBUG) {
                System.out.println("  path: " + path.input.get());
            }

            // We take path and find its "0 output completion",
            // ie, just keep traversing the first arc with
            // NO_OUTPUT that we can find, since this must lead
            // to the minimum path that completes from
            // path.arc.

            if (DEBUG) {
                System.out.println("\n    cycle path: " + path.input.get());
            }

            boolean isLeaf = path.arc.label == leafLabel;
            boolean inLeaf = false;
            int acceptedLeaves = 0;

            if (isLeaf) {
                onLeafNode(path.input.get(), path.cost);
                inLeaf = true;
            }

            // For each input letter:
            while (true) {
                fst.readFirstTargetArc(path.arc, path.arc, fstReader);
                boolean foundZero = false;
                // For each arc leaving this node:
                while (true) {
                    if (DEBUG) {
                        System.out.println("      arc=" + path.input.get().toString() + " cost=" + path.arc.output);
                    }
                    // tricky: instead of comparing output == 0, we must
                    // express it via the comparator compare(output, 0) == 0
                    //TODO: generalize for bytes based scoring (no output would be if there is a exact bytes match with the current context with the right offset)
                    if (comparator.compare(NO_OUTPUT, path.arc.output) == 0) {
                        if (queue == null) {
                            foundZero = true;
                            break;
                        } else if (!foundZero) {
                            scratchArc.copyFrom(path.arc);
                            foundZero = true;
                        } else if (inLeaf) {
                            addLeafPath(path);
                        } else {
                            addIfCompetitive(path);
                        }
                    } else if (inLeaf) {
                        addLeafPath(path);
                    } else if (queue != null) {
                        addIfCompetitive(path);
                    }
                    if (path.arc.isLast()) {
                        break;
                    }
                    fst.readNextArc(path.arc, fstReader);
                }
                assert foundZero;

                if (queue != null) {
                    // TODO: maybe we can save this copyFrom if we
                    // are more clever above... eg on finding the
                    // first NO_OUTPUT arc we'd switch to using
                    // scratchArc
                    path.arc.copyFrom(scratchArc);
                }
                if (inLeaf) {
                    // The path with the lowest cost is final
                    // so can scan the leafQueue for any other paths
                    // that are already final
                    if (path.arc.isFinal()) {
                        T cost = fst.outputs.add(path.cost, path.arc.output);
                        while (path != null && path.arc.isFinal()) {
                            if (!prune(cost)) {
                                if (collectOutput(cost)) {
                                    if (++acceptedLeaves == nLeaf) {
                                        break;
                                    }
                                }
                            }
                            path = leafQueue.pollFirst();
                            if (path != null) {
                                cost = path.cost;
                            }
                        }
                        if (path == null || acceptedLeaves == nLeaf) {
                            if (acceptedLeaves > 0) {
                                resultCount++;
                            } else {
                                rejectCount++;
                            }
                            leafQueue.clear();
                            onFinishNode();
                            break;
                        }
                    } else {
                        path.input.append(path.arc.label);
                        path.cost = fst.outputs.add(path.cost, path.arc.output);
                    }
                } else {
                    isLeaf = path.arc.label == leafLabel;
                    path.cost = fst.outputs.add(path.cost, path.arc.output);
                    if (isLeaf && !inLeaf) {
                        onLeafNode(path.input.get(), path.cost);
                        inLeaf = true;
                    }
                    path.input.append(path.arc.label);
                }
            }
        }
        return rejectCount + topN <= maxQueueDepth;
    }

    /**
     * Called exactly once when a leaf node for a <code>input</code>
     * is found
     * @param input the input up to the leaf node (excluding the leafLabel)
     * @param output the output up to the leaf node
     */
    protected abstract void onLeafNode(IntsRef input, T output);

    /**
     * Called on finding a leaf node output.
     *
     * This will be called at most <code>nLeaf</code> + number of not accepted
     * output.
     *
     * @param output full output at the leaf node
     * @return true if output is accepted, false otherwise
     * @throws IOException
     */
    protected abstract boolean collectOutput(T output) throws IOException;

    /**
     * Called on finishing processing all the output(s) at a leaf node
     * @throws IOException
     */
    protected abstract void onFinishNode() throws IOException;

    /**
     * Compares first by the provided comparator, and then
     * tie breaks by path.input.
     */
    private static class TieBreakByInputComparator<T> implements Comparator<Util.FSTPath<T>> {
        private final Comparator<T> comparator;

        public TieBreakByInputComparator(Comparator<T> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Util.FSTPath<T> a, Util.FSTPath<T> b) {
            int cmp = comparator.compare(a.cost, b.cost);
            if (cmp == 0) {
                return a.input.get().compareTo(b.input.get());
            } else {
                return cmp;
            }
        }
    }
}
