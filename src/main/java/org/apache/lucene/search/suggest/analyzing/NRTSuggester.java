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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.suggest.analyzing.ContextAwareScorer.OutputConfiguration;
import org.apache.lucene.search.suggest.analyzing.SegmentLookup.Context.Filter;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import org.apache.lucene.util.automaton.*;
import org.apache.lucene.util.fst.*;
import org.apache.lucene.util.fst.PairOutputs.Pair;

import java.io.IOException;
import java.util.*;

import static org.apache.lucene.search.suggest.analyzing.NRTSuggester.PayLoadProcessor.parseDocID;
import static org.apache.lucene.search.suggest.analyzing.NRTSuggester.PayLoadProcessor.parseSurfaceForm;

/**
 *
 * NRTSuggester:
 * Performs lookup on an FST returning the top <code>num</code> analyzed forms with the top acceptable <code>nLeaf</code> docIDs
 * for a given prefix scored according to the provided {@link ContextAwareScorer}.
 * The lookup filters out results that correspond to deleted documents in near-real time.
 * see {@link #lookup(LeafReader, Analyzer, CharSequence, int, int, Context, Collector)}.
 *
 * FST Format:
 * Input: analyzed forms of input terms
 * Output: raw input value, weight and docID
 * Score: configurable through {@link ContextAwareScorer} (currently supported scores: Long and BytesRef)
 * @lucene.experimental
 */
public class NRTSuggester<W extends Comparable<W>> extends SegmentLookup<W> {

    static final Map<String, ContextAwareScorer<?>> scoreProvider = new HashMap<>();

    /**
     * Registers a {@link ContextAwareScorer}
     * @param name unique name for the scorer
     * @param scorer the instance
     * @return true if the scorer has been registered, false if the scorer was already registered
     */
    static boolean registerScorer(String name, ContextAwareScorer<?> scorer) {
        if (!scoreProvider.containsKey(name)) {
            scoreProvider.put(name, scorer);
            return true;
        }
        return false;
    }

    /**
     * FST<Weight,Surface>:
     * input is the analyzed form, with a null byte between terms
     * and a {@link NRTSuggesterBuilder#END_BYTE} to denote the
     * end of the input
     * weight is encoded according to {@link ContextAwareScorer#getOutputConfiguration()}
     * surface is the original, unanalyzed form followed by the docID
     */
    private final FST<Pair<W, BytesRef>> fst;

    /**
     * Highest number of analyzed paths we saw for any single
     * input surface form.  For analyzers that never create
     * graphs this will always be 1.
     */
    private final int maxAnalyzedPathsPerLeaf;

    /**
     * Represents the separation between tokens, if
     * {@link #preserveSep} was specified
     */
    private final int sepLabel;

    /**
     * Seperator used between surface form and its docID in the FST output
     */
    private final int payloadSep;

    /**
     * Maximum graph paths to index for a single analyzed
     * surface form.  This only matters if your analyzer
     * makes lots of alternate paths (e.g. contains
     * SynonymFilter).
     */
    private final int endByte;

    private final int holeCharacter;


    /**
     * True if separator between tokens should be preserved.
     */
    private final boolean preserveSep;

    /**
     * Whether position holes should appear in the automaton.
     */
    private final boolean preservePositionIncrements;

    /**
     * Maximum queue depth for {@link TopNSearcher}
     */
    private static final int MAX_TOP_N_QUEUE_SIZE = 1000;

    private static final int MAX_GRAPH_EXPANSIONS = -1;

    private final Scorer scorer;

    private NRTSuggester(ContextAwareScorer<W> contextAwareScorer, FST<Pair<W, BytesRef>> fst,
                         int maxAnalyzedPathsPerLeaf,
                         boolean preserveSep, boolean preservePositionIncrements,
                         int sepLabel, int payloadSep, int endByte, int holeCharacter) {
        this.fst = fst;
        this.maxAnalyzedPathsPerLeaf = maxAnalyzedPathsPerLeaf;
        this.sepLabel = sepLabel;
        this.payloadSep = payloadSep;
        this.endByte = endByte;
        this.holeCharacter = holeCharacter;
        this.preserveSep = preserveSep;
        this.preservePositionIncrements = preservePositionIncrements;
        this.scorer = new Scorer(contextAwareScorer);
        if (sepLabel != NRTSuggesterBuilder.SEP_LABEL) {
            throw new IllegalArgumentException("sepLabel must be " + NRTSuggesterBuilder.SEP_LABEL);
        }
    }

    @Override
    public long ramBytesUsed() {
        return fst == null ? 0 : fst.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    private static double calculateLiveDocRatio(int numDocs, int maxDocs) {
        return (numDocs > 0) ? ((double) numDocs / maxDocs) : -1;
    }

    private class Scorer {
        final ContextAwareScorer<W> contextAwareScorer;
        final OutputConfiguration<W> outputConfiguration;

        public Scorer(ContextAwareScorer<W> scorer) {
            this.contextAwareScorer = scorer;
            this.outputConfiguration = scorer.getOutputConfiguration();
        }

        public boolean prune(W currentContext, Pair<W, BytesRef> pathCost) {
            //TODO: we can add a strict mode, where the surfaceform/input will have to have the same prefix as the key/analyzed query
            return contextAwareScorer.prune(currentContext, outputConfiguration.decode(pathCost.output1));
        }

        public Comparator<Pair<W, BytesRef>> getComparator(W currentContext) {
            final Comparator<W> comparator = contextAwareScorer.comparator(currentContext);
            return new Comparator<Pair<W, BytesRef>>() {
                @Override
                public int compare(Pair<W, BytesRef> o1, Pair<W, BytesRef> o2) {
                    //TODO: we can also sort by the surface form here (for more relevant result w.r.t. input key)
                    return comparator.compare(o1.output1, o2.output1);
                }
            };
        }

        public W score(W currentContext, W result) {
            return contextAwareScorer.score(currentContext, outputConfiguration.decode(result));
        }
    }

    @Override
    public void lookup(final LeafReader reader, final Analyzer queryAnalyzer, final CharSequence key, final int num, final int nLeaf, final Context<W> context, final Collector<W> collector) {

        /* DEBUG
        try {
            PrintWriter pw = new PrintWriter("/tmp/out.dot");
            Util.toDot(fst, pw, true, true);
            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */

        assert num > 0 : "num must be > 0, found num="+num;
        if (fst == null) {
            return;
        }

        if (reader == null) {
            throw new IllegalArgumentException("reader can not be null");
        }

        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) == holeCharacter) {
                throw new IllegalArgumentException("lookup key cannot contain HOLE character U+001E; this character is reserved");
            }
            if (key.charAt(i) == sepLabel) {
                throw new IllegalArgumentException("lookup key cannot contain unit separator character U+001F; this character is reserved");
            }
        }

        final double liveDocsRatio = calculateLiveDocRatio(reader.numDocs(), reader.maxDoc());
        if (liveDocsRatio == -1) {
            return;
        }
        final W scoreContext;
        final Filter filter;

        if (context == null) {
            scoreContext = null;
            filter = null;
        } else {
            scoreContext = context.scoreContext;
            filter = context.filter;
        }

        final Bits liveDocs = reader.getLiveDocs();
        try {

            Automaton lookupAutomaton = toLookupAutomaton(queryAnalyzer, key);
            final List<FSTUtil.Path<Pair<W, BytesRef>>> prefixPaths = FSTUtil.intersectPrefixPaths(lookupAutomaton, fst);

            TopNSearcher<Pair<W,BytesRef>> searcher = new TopNSearcher<Pair<W, BytesRef>>(fst,
                                                                   num, nLeaf, endByte,
                                                                    // TODO: take into account filter cardinality
                                                                   getMaxTopNSearcherQueueSize(num, liveDocsRatio),
                                                                   scorer.getComparator(scoreContext)) {
                private final CharsRefBuilder spare = new CharsRefBuilder();
                private final BytesRefBuilder currentKeyBuilder = new BytesRefBuilder();
                private final List<Collector.ResultMetaData<W>> currentResultMetaDataList = new ArrayList<>(nLeaf);
                private final Set<Integer> seenDocIds = new HashSet<>(nLeaf);

                @Override
                protected boolean prune(Pair<W, BytesRef> pathCost) {
                    return scorer.prune(scoreContext, pathCost);
                }

                @Override
                protected void onLeafNode(IntsRef input, Pair<W, BytesRef> output) {
                    Util.toBytesRef(input, currentKeyBuilder);
                }

                @Override
                protected boolean collectOutput(Pair<W, BytesRef> output) throws IOException {
                    int payloadSepIndex = parseSurfaceForm(output.output2, payloadSep, spare);
                    int docID = parseDocID(output.output2, payloadSepIndex);

                    // filter out deleted docs
                    if (liveDocs != null && !liveDocs.get(docID)) {
                        return false;
                    }

                    // filter by filter context
                    if (filter != null && !filter.bits().get(docID)) {
                        return false;
                    }

                    // compute score
                    // add new surface form
                    if (!seenDocIds.contains(docID)) {
                        W score = scorer.score(scoreContext, output.output1);
                        currentResultMetaDataList.add(new Collector.ResultMetaData<>(spare.toCharsRef(), score, docID));
                        seenDocIds.add(docID);
                        return true;
                    }  else {
                        return false;
                    }
                }

                @Override
                protected void onFinishNode() throws IOException {
                    if (currentResultMetaDataList.size() > 0) {
                        collector.collect(currentKeyBuilder.get().utf8ToString(), new ArrayList<>(currentResultMetaDataList));
                        currentResultMetaDataList.clear();
                    }
                    seenDocIds.clear();
                }

            };

            // TODO: add fuzzy support
            for (FSTUtil.Path<Pair<W,BytesRef>> path : getFullPrefixPaths(prefixPaths, lookupAutomaton, fst)) {
                searcher.addStartPaths(path.fstNode, path.output, true, path.input);
            }

            boolean isComplete = searcher.search();
            // search admissibility is not guaranteed
            // see comment on getMaxTopNSearcherQueueSize
            //assert isComplete;

        } catch (IOException bogus) {
            throw new RuntimeException(bogus);
        }
    }

    /**
     * Simple heuristics to try to avoid over-pruning potential suggestions by the
     * TopNSearcher. Since suggestion entries can be rejected if they belong
     * to a deleted document, the length of the TopNSearcher queue has to
     * be increased by some factor, to account for the filtered out suggestions.
     * This heuristic will try to make the searcher admissible, but the search
     * can still lead to over-pruning
     */
    private int getMaxTopNSearcherQueueSize(int num, final double liveDocRatio) {
        // TODO: we can get away with just num instead, as the leaves of an input are managed by another queue
        int maxQueueSize = num * maxAnalyzedPathsPerLeaf;
        // liveDocRatio can be at most 1.0 (if no docs were deleted)
        assert liveDocRatio <= 1.0d;
        return Math.min(MAX_TOP_N_QUEUE_SIZE, (int) (maxQueueSize / liveDocRatio));
    }

    /**
     * Returns all completion paths to initialize the search.
     */
    protected List<FSTUtil.Path<Pair<W, BytesRef>>> getFullPrefixPaths(List<FSTUtil.Path<Pair<W, BytesRef>>> prefixPaths,
                                                                          Automaton lookupAutomaton,
                                                                          FST<Pair<W, BytesRef>> fst)
            throws IOException {
        return prefixPaths;
    }


    final Automaton toLookupAutomaton(final Analyzer queryAnalyzer, final CharSequence key) throws IOException {
        // TODO: is there a Reader from a CharSequence?
        // Turn tokenstream into automaton:
        Automaton automaton = null;
        TokenStream ts = queryAnalyzer.tokenStream("", key.toString());
        try {
            automaton = TokenUtils.getTokenStreamToAutomaton(preserveSep, preservePositionIncrements, sepLabel).toAutomaton(ts);
        } finally {
            IOUtils.closeWhileHandlingException(ts);
        }

        automaton = replaceSep(automaton, preserveSep, sepLabel);

        // TODO: we can optimize this somewhat by determinizing
        // while we convert

        // This automaton should not blow up during determinize:
        automaton = Operations.determinize(automaton, Integer.MAX_VALUE);
        return automaton;
    }

    /**
     * Loads a {@link NRTSuggester} from {@link org.apache.lucene.store.IndexInput}
     */
    public static <W extends Comparable<W>> NRTSuggester<W> load(IndexInput input) throws IOException {
        final ContextAwareScorer<W> contextAwareScorer = (ContextAwareScorer<W>) scoreProvider.get(input.readString());
        OutputConfiguration<W> outputConfiguration = contextAwareScorer.getOutputConfiguration();
        final FST<Pair<W, BytesRef>> fst = new FST<>(input, new PairOutputs<>(
                outputConfiguration.outputSingleton(), ByteSequenceOutputs.getSingleton()));
        int maxAnalyzedPathsPerLeaf = input.readVInt();
        int options = input.readVInt();
        boolean preserveSep = (options & NRTSuggesterBuilder.SERIALIZE_PRESERVE_SEPARATORS) != 0;
        boolean preservePositionIncrements = (options & NRTSuggesterBuilder.SERIALIZE_PRESERVE_POSITION_INCREMENTS) != 0;
        int sepLabel = input.readVInt();
        int endByte = input.readVInt();
        int payloadSep = input.readVInt();
        int holeCharacter = input.readVInt();

        return new NRTSuggester<>(contextAwareScorer, fst, maxAnalyzedPathsPerLeaf, preserveSep,
                preservePositionIncrements, sepLabel, payloadSep, endByte, holeCharacter);

    }

    // Replaces SEP with epsilon or remaps them if
    // we were asked to preserve them:
    private static Automaton replaceSep(Automaton a, boolean preserveSep, int sepLabel) {

        Automaton result = new Automaton();

        // Copy all states over
        int numStates = a.getNumStates();
        for(int s=0;s<numStates;s++) {
            result.createState();
            result.setAccept(s, a.isAccept(s));
        }

        // Go in reverse topo sort so we know we only have to
        // make one pass:
        Transition t = new Transition();
        int[] topoSortStates = topoSortStates(a);
        for(int i=0;i<topoSortStates.length;i++) {
            int state = topoSortStates[topoSortStates.length-1-i];
            int count = a.initTransition(state, t);
            for(int j=0;j<count;j++) {
                a.getNextTransition(t);
                if (t.min == TokenStreamToAutomaton.POS_SEP) {
                    assert t.max == TokenStreamToAutomaton.POS_SEP;
                    if (preserveSep) {
                        // Remap to SEP_LABEL:
                        result.addTransition(state, t.dest, sepLabel);
                    } else {
                        result.addEpsilon(state, t.dest);
                    }
                } else if (t.min == TokenStreamToAutomaton.HOLE) {
                    assert t.max == TokenStreamToAutomaton.HOLE;

                    // Just remove the hole: there will then be two
                    // SEP tokens next to each other, which will only
                    // match another hole at search time.  Note that
                    // it will also match an empty-string token ... if
                    // that's somehow a problem we can always map HOLE
                    // to a dedicated byte (and escape it in the
                    // input).
                    result.addEpsilon(state, t.dest);
                } else {
                    result.addTransition(state, t.dest, t.min, t.max);
                }
            }
        }

        result.finishState();

        return result;
    }

    private static int[] topoSortStates(Automaton a) {
        int[] states = new int[a.getNumStates()];
        final Set<Integer> visited = new HashSet<>();
        final LinkedList<Integer> worklist = new LinkedList<>();
        worklist.add(0);
        visited.add(0);
        int upto = 0;
        states[upto] = 0;
        upto++;
        Transition t = new Transition();
        while (worklist.size() > 0) {
            int s = worklist.removeFirst();
            int count = a.initTransition(s, t);
            for (int i=0;i<count;i++) {
                a.getNextTransition(t);
                if (!visited.contains(t.dest)) {
                    visited.add(t.dest);
                    worklist.add(t.dest);
                    states[upto++] = t.dest;
                }
            }
        }
        return states;
    }

    /**
     * TODO: think of a better name
     */
    public static class TokenUtils {

        public static Set<IntsRef> toFiniteStrings(TokenStream ts, boolean preserveSep, boolean preservePositionIncrements) throws IOException {
            Automaton automaton = null;
            try {

                // Create corresponding automaton: labels are bytes
                // from each analyzed token, with byte 0 used as
                // separator between tokens:
                automaton = getTokenStreamToAutomaton(preserveSep, preservePositionIncrements, NRTSuggesterBuilder.SEP_LABEL).toAutomaton(ts);
            } finally {
                IOUtils.closeWhileHandlingException(ts);
            }

            automaton = replaceSep(automaton, preserveSep, NRTSuggesterBuilder.SEP_LABEL);

            // TODO: LUCENE-5660 re-enable this once we disallow massive suggestion strings
            // assert SpecialOperations.isFinite(automaton);

            // Get all paths from the automaton (there can be
            // more than one path, eg if the analyzer created a
            // graph using SynFilter or WDF):

            // TODO: we could walk & add simultaneously, so we
            // don't have to alloc [possibly biggish]
            // intermediate HashSet in RAM:

            return Operations.getFiniteStrings(automaton, MAX_GRAPH_EXPANSIONS);
        }


        /**
         * Just escapes the 0xff byte (which we still for SEP).
         */
        private static final class EscapingTokenStreamToAutomaton extends TokenStreamToAutomaton {

            final BytesRefBuilder spare = new BytesRefBuilder();
            private char sepLabel;

            public EscapingTokenStreamToAutomaton(char sepLabel) {
                this.sepLabel = sepLabel;
            }

            @Override
            protected BytesRef changeToken(BytesRef in) {
                int upto = 0;
                for(int i=0;i<in.length;i++) {
                    byte b = in.bytes[in.offset+i];
                    if (b == (byte) sepLabel) {
                        spare.grow(upto+2);
                        spare.setByteAt(upto++, (byte) sepLabel);
                        spare.setByteAt(upto++, b);
                    } else {
                        spare.grow(upto+1);
                        spare.setByteAt(upto++, b);
                    }
                }
                spare.setLength(upto);
                return spare.get();
            }
        }

        public static TokenStreamToAutomaton getTokenStreamToAutomaton(boolean preserveSep, boolean preservePositionIncrements, int sepLabel) {
            final TokenStreamToAutomaton tsta;
            if (preserveSep) {
                tsta = new EscapingTokenStreamToAutomaton((char) sepLabel);
            } else {
                // When we're not preserving sep, we don't steal 0xff
                // byte, so we don't need to do any escaping:
                tsta = new TokenStreamToAutomaton();
            }
            tsta.setPreservePositionIncrements(preservePositionIncrements);
            return tsta;
        }
    }

    /**
     * Helper to encode/decode payload with surface + PAYLOAD_SEP + docID
     */
    static final class PayLoadProcessor {
        final static private int MAX_DOC_ID_LEN_WITH_SEP = 6; // vint takes at most 5 bytes

        static int parseSurfaceForm(final BytesRef output, int payloadSep, CharsRefBuilder spare) {
            int surfaceFormLen = -1;
            for (int i = 0; i < output.length; i++) {
                if (output.bytes[output.offset + i] == payloadSep) {
                    surfaceFormLen = i;
                    break;
                }
            }
            assert surfaceFormLen != -1 : "no payloadSep found, unable to determine surface form";
            spare.copyUTF8Bytes(output.bytes, output.offset, surfaceFormLen);
            return surfaceFormLen;
        }

        static int parseDocID(final BytesRef output, int payloadSepIndex) {
            assert payloadSepIndex != -1 : "payload sep index can not be -1";
            ByteArrayDataInput input = new ByteArrayDataInput(output.bytes, payloadSepIndex + output.offset + 1, output.length - (payloadSepIndex + output.offset));
            return input.readVInt();
        }

        static BytesRef make(final BytesRef surface, int docID, int payloadSep) throws IOException {
            int len = surface.length + MAX_DOC_ID_LEN_WITH_SEP;
            byte[] buffer = new byte[len];
            ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
            output.writeBytes(surface.bytes, surface.length - surface.offset);
            output.writeByte((byte) payloadSep);
            output.writeVInt(docID);
            return new BytesRef(buffer, 0, output.getPosition());
        }
    }
}
