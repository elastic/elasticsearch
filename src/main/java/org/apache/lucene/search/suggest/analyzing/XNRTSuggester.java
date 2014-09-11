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
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.*;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.automaton.*;
import org.apache.lucene.util.fst.*;
import org.apache.lucene.util.fst.PairOutputs.Pair;

import java.io.IOException;
import java.util.*;

/**
 * Currently A fork of {@link org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester}
 * with very limited NRT capabilities.<p>
 * <b>Supported features:</b>
 *  <ul>
 *    <li>Filter out deleted documents (NRT)</li>
 *    <li>Returning arbitrary stored fields as payload (NRT)</li>
 *    <li>Optionally return duplicated output</li>
 *  </ul>
 * <b>NOTE: This is still largely a work in progress.</b>
 * <p/>
 * <b>TODO:</b>
 *  <ul>
 *   <li> Flexible Scoring/Weighting: Allow factoring in external factors (lookup key, surface form) to weighting suggestions</li>
 *   <li> General Refactoring</li>
 *   </ul>
 *
 * @lucene.experimental
 */
public class XNRTSuggester extends XLookup {

    /**
     * FST<Weight,Surface>:
     * input is the analyzed form, with a null byte between terms
     * weights are encoded as costs: (Integer.MAX_VALUE-weight)
     * surface is the original, unanalyzed form.
     */
    private FST<Pair<Long, BytesRef>> fst = null;

    /**
     * Analyzer that will be used for analyzing suggestions at
     * index time.
     *
     * TODO:: cleanup (indexAnalyzer is not used in Lookup anymore)
     */
    private final Analyzer indexAnalyzer;

    /**
     * Analyzer that will be used for analyzing suggestions at
     * query time.
     */
    private final Analyzer queryAnalyzer;

    /**
     * True if separator between tokens should be preserved.
     */
    private final boolean preserveSep;

    /**
     * Include this flag in the options parameter to {@link
     * #XAnalyzingSuggester(Analyzer, Analyzer, int, int, int, boolean, FST, boolean, int, int, int, int, int)} to preserve
     * token separators when matching.
     */
    public static final int PRESERVE_SEP = 2;

    /**
     * Represents the separation between tokens, if
     * PRESERVE_SEP was specified
     */
    public static final int SEP_LABEL = '\u001F';

    /**
     * Marks end of the analyzed input and start of dedup
     * byte.
     */
    public static final int END_BYTE = 0x0;

    /**
     * Maximum number of dup surface forms (different surface
     * forms for the same analyzed form).
     *
     * TODO: cleanup
     */
    private final int maxSurfaceFormsPerAnalyzedForm;

    /**
     * Maximum graph paths to index for a single analyzed
     * surface form.  This only matters if your analyzer
     * makes lots of alternate paths (e.g. contains
     * SynonymFilter).
     */
    private final int maxGraphExpansions;

    /**
     * Highest number of analyzed paths we saw for any single
     * input surface form.  For analyzers that never create
     * graphs this will always be 1.
     */
    private int maxAnalyzedPathsForOneInput;

    private boolean hasPayloads;

    private final int sepLabel;
    private final int payloadSep;
    private final int endByte;
    private final int holeCharacter;

    public static final int PAYLOAD_SEP = '\u001F';
    public static final int HOLE_CHARACTER = '\u001E';
    private static final int MAX_TOP_N_QUEUE_SIZE = 1000;
    private final Automaton queryPrefix;

    /**
     * Whether position holes should appear in the automaton.
     */
    private boolean preservePositionIncrements;

    /**
     * Calls {@link #XAnalyzingSuggester(Analyzer, Analyzer, int, int, int, boolean, FST, boolean, int, int, int, int, int)
     * AnalyzingSuggester(analyzer, analyzer, PRESERVE_SEP, 256, -1)}
     */
    public XNRTSuggester(Analyzer analyzer) {
        this(analyzer, null, analyzer, PRESERVE_SEP, 256, -1, true, null, false, 0, SEP_LABEL, PAYLOAD_SEP, END_BYTE, HOLE_CHARACTER);
    }

    /**
     * Calls {@link #XAnalyzingSuggester(Analyzer, Analyzer, int, int, int, boolean, FST, boolean, int, int, int, int, int)
     * AnalyzingSuggester(indexAnalyzer, queryAnalyzer, PRESERVE_SEP, 256, -1)}
     */
    public XNRTSuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer) {
        this(indexAnalyzer, null, queryAnalyzer, PRESERVE_SEP, 256, -1, true, null, false, 0, SEP_LABEL, PAYLOAD_SEP, END_BYTE, HOLE_CHARACTER);
    }

    /**
     * Creates a new suggester.
     *
     * @param indexAnalyzer                  Analyzer that will be used for
     *                                       analyzing suggestions while building the index.
     * @param queryAnalyzer                  Analyzer that will be used for
     *                                       analyzing query text during lookup
     * @param options                        see {@link #PRESERVE_SEP}
     * @param maxSurfaceFormsPerAnalyzedForm Maximum number of
     *                                       surface forms to keep for a single analyzed form.
     *                                       When there are too many surface forms we discard the
     *                                       lowest weighted ones.
     * @param maxGraphExpansions             Maximum number of graph paths
     *                                       to expand from the analyzed form.  Set this to -1 for
     *                                       no limit.
     */
    public XNRTSuggester(Analyzer indexAnalyzer, Automaton queryPrefix, Analyzer queryAnalyzer, int options, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions,
                         boolean preservePositionIncrements, FST<Pair<Long, BytesRef>> fst, boolean hasPayloads, int maxAnalyzedPathsForOneInput,
                         int sepLabel, int payloadSep, int endByte, int holeCharacter) {
        // SIMON EDIT: I added fst, hasPayloads and maxAnalyzedPathsForOneInput
        this.indexAnalyzer = indexAnalyzer;
        this.queryAnalyzer = queryAnalyzer;
        this.fst = fst;
        this.hasPayloads = hasPayloads;
        //TODO:: simplify (exact_first no longer used)
        if ((options & ~(PRESERVE_SEP)) != 0) {
            throw new IllegalArgumentException("options should only contain PRESERVE_SEP; got " + options);
        }
        this.preserveSep = (options & PRESERVE_SEP) != 0;

        // TODO: remove query prefix support, once filter can be supported
        this.queryPrefix = queryPrefix;

        // NOTE: this is just an implementation limitation; if
        // somehow this is a problem we could fix it by using
        // more than one byte to disambiguate ... but 256 seems
        // like it should be way more then enough.
        if (maxSurfaceFormsPerAnalyzedForm <= 0 || maxSurfaceFormsPerAnalyzedForm > 256) {
            throw new IllegalArgumentException("maxSurfaceFormsPerAnalyzedForm must be > 0 and < 256 (got: " + maxSurfaceFormsPerAnalyzedForm + ")");
        }
        this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;

        if (maxGraphExpansions < 1 && maxGraphExpansions != -1) {
            throw new IllegalArgumentException("maxGraphExpansions must -1 (no limit) or > 0 (got: " + maxGraphExpansions + ")");
        }
        this.maxGraphExpansions = maxGraphExpansions;
        this.maxAnalyzedPathsForOneInput = maxAnalyzedPathsForOneInput;
        this.preservePositionIncrements = preservePositionIncrements;
        this.sepLabel = sepLabel;
        this.payloadSep = payloadSep;
        this.endByte = endByte;
        this.holeCharacter = holeCharacter;
    }

    /**
     * Returns byte size of the underlying FST.
     */
    public long ramBytesUsed() {
        return fst == null ? 0 : fst.ramBytesUsed();
    }

    // Replaces SEP with epsilon or remaps them if
    // we were asked to preserve them:
    private Automaton replaceSep(Automaton a) {

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
                        result.addTransition(state, t.dest, SEP_LABEL);
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
    protected Automaton convertAutomaton(Automaton a) {
        if (queryPrefix != null) {
            a = Operations.concatenate(Arrays.asList(queryPrefix, a));
            Operations.determinize(a);
        }
        return a;
    }

    private int[] topoSortStates(Automaton a) {
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
     * Just escapes the 0xff byte (which we still for SEP).
     */
    private static final class  EscapingTokenStreamToAutomaton extends TokenStreamToAutomaton {

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

    public TokenStreamToAutomaton getTokenStreamToAutomaton() {
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

    private Result getLookupResult(CharsRef term, Long output1, BytesRef payload, List<Result.XStoredField> storedFields) {
        return new Result(term.toString(), decodeWeight(output1), payload, storedFields);
    }

    private static List<Result.XStoredField> getPayloadFields(int docID, Set<String> payloadFieldNames, final AtomicReader reader) throws IOException {
        if (payloadFieldNames != null) {
            final Document document = reader.document(docID, payloadFieldNames);
            return Result.getStoredFieldsFromDocument(document, payloadFieldNames);
        }
        return null;
    }

    private static double calculateLiveDocRatio(int numDocs, int maxDocs) {
        return (numDocs > 0) ? ((double) numDocs / maxDocs) : -1;
    }

    @Override
    public List<Result> lookup(final CharSequence key, final int num, final AtomicReader reader, final Set<String> payloadFields, final boolean duplicateSurfaceForm) {
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
            return Collections.emptyList();
        }

        final double liveDocsRatio = (reader != null) ? calculateLiveDocRatio(reader.numDocs(), reader.maxDoc()) : 1.0d;
        if (liveDocsRatio == -1) {
            return Collections.emptyList();
        }

        final Bits liveDocs = (reader != null) ? reader.getLiveDocs() : null;

        if (payloadFields != null && reader == null) {
            throw new IllegalArgumentException("can't retrieve payloads if reader=null");
        }
        //System.out.println("lookup key=" + key + " num=" + num);
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) == holeCharacter) {
                throw new IllegalArgumentException("lookup key cannot contain HOLE character U+001E; this character is reserved");
            }
            if (key.charAt(i) == sepLabel) {
                throw new IllegalArgumentException("lookup key cannot contain unit separator character U+001F; this character is reserved");
            }
        }
        try {

            Automaton lookupAutomaton = toLookupAutomaton(key);

            // Intersect automaton w/ suggest wFST and get all
            // prefix starting nodes & their outputs:
            //final PathIntersector intersector = getPathIntersector(lookupAutomaton, fst);

            //System.out.println("  prefixPaths: " + prefixPaths.size());

            final List<Result> results = new ArrayList<>();

            List<FSTUtil.Path<Pair<Long,BytesRef>>> prefixPaths = FSTUtil.intersectPrefixPaths(convertAutomaton(lookupAutomaton), fst);


            XUtil.TopNSearcher<Pair<Long,BytesRef>> searcher;
            searcher = new XUtil.TopNSearcher<Pair<Long,BytesRef>>(fst,
                                                                   num,
                                                                   getMaxTopNSearcherQueueSize(num, liveDocsRatio),
                                                                   weightComparator) {
                private final Set<BytesRef> seen = new HashSet<>();
                private CharsRefBuilder spare = new CharsRefBuilder();

                @Override
                protected boolean collectResult(IntsRef input, Pair<Long, BytesRef> output) throws IOException {

                    XPayLoadProcessor.PayloadMetaData metaData = XPayLoadProcessor.parse(output.output2, hasPayloads, payloadSep, spare);
                    if (liveDocs != null && metaData.hasDocID()) {
                        if (!liveDocs.get(metaData.docID)) {
                            return false;
                        }
                    }
                    // Dedup: when the input analyzes to a graph we
                    // can get duplicate surface forms:
                    if (!duplicateSurfaceForm && seen.contains(metaData.surfaceForm)) {
                        return false;
                    }
                    seen.add(metaData.surfaceForm);

                    Result result = getLookupResult(spare.get(), output.output1, metaData.payload,
                                                           getPayloadFields(metaData.docID, payloadFields, reader));
                    results.add(result);
                    return true;
                }
            };

            prefixPaths = getFullPrefixPaths(prefixPaths, lookupAutomaton, fst);

            for (FSTUtil.Path<Pair<Long,BytesRef>> path : prefixPaths) {
                searcher.addStartPaths(path.fstNode, path.output, true, path.input);
            }

            // TODO: for fuzzy case would be nice to return

            boolean isComplete = searcher.search();
            // search admissibility is not guaranteed
            // see comment on getMaxTopNSearcherQueueSize
            //assert isComplete;
            return results;
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
        int maxQueueSize = num * maxAnalyzedPathsForOneInput;
        // liveDocRatio can be at most 1.0 (if no docs were deleted)
        assert liveDocRatio <= 1.0d;
        return Math.min(MAX_TOP_N_QUEUE_SIZE, (int) (maxQueueSize / liveDocRatio));
    }

    /**
     * Returns all completion paths to initialize the search.
     */
    protected List<FSTUtil.Path<Pair<Long, BytesRef>>> getFullPrefixPaths(List<FSTUtil.Path<Pair<Long, BytesRef>>> prefixPaths,
                                                                          Automaton lookupAutomaton,
                                                                          FST<Pair<Long, BytesRef>> fst)
            throws IOException {
        return prefixPaths;
    }

    public final Set<IntsRef> toFiniteStrings(final TokenStreamToAutomaton ts2a, final TokenStream ts) throws IOException {
        Automaton automaton = null;
        try {

            // Create corresponding automaton: labels are bytes
            // from each analyzed token, with byte 0 used as
            // separator between tokens:
            automaton = ts2a.toAutomaton(ts);
        } finally {
            IOUtils.closeWhileHandlingException(ts);
        }

        automaton = replaceSep(automaton);
        automaton = convertAutomaton(automaton);

        // TODO: LUCENE-5660 re-enable this once we disallow massive suggestion strings
        // assert SpecialOperations.isFinite(automaton);

        // Get all paths from the automaton (there can be
        // more than one path, eg if the analyzer created a
        // graph using SynFilter or WDF):

        // TODO: we could walk & add simultaneously, so we
        // don't have to alloc [possibly biggish]
        // intermediate HashSet in RAM:

        return Operations.getFiniteStrings(automaton, maxGraphExpansions);
    }

    final Automaton toLookupAutomaton(final CharSequence key) throws IOException {
        // TODO: is there a Reader from a CharSequence?
        // Turn tokenstream into automaton:
        Automaton automaton = null;
        TokenStream ts = queryAnalyzer.tokenStream("", key.toString());
        try {
            automaton = getTokenStreamToAutomaton().toAutomaton(ts);
        } finally {
            IOUtils.closeWhileHandlingException(ts);
        }

        automaton = replaceSep(automaton);

        // TODO: we can optimize this somewhat by determinizing
        // while we convert
        automaton = Operations.determinize(automaton);
        return automaton;
    }



    /**
     * Returns the weight associated with an input string,
     * or null if it does not exist.
     */
    public Object get(CharSequence key) {
        throw new UnsupportedOperationException();
    }

    /**
     * cost -> weight
     */
    public static int decodeWeight(long encoded) {
        return (int) (Integer.MAX_VALUE - encoded);
    }

    /**
     * weight -> cost
     */
    public static int encodeWeight(long value) {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException("cannot encode value: " + value);
        }
        return Integer.MAX_VALUE - (int) value;
    }

    static final Comparator<Pair<Long, BytesRef>> weightComparator = new Comparator<Pair<Long, BytesRef>>() {
        @Override
        public int compare(Pair<Long, BytesRef> left, Pair<Long, BytesRef> right) {
            return left.output1.compareTo(right.output1);
        }
    };

    public static class XBuilder {
        private Builder<Pair<Long, BytesRef>> builder;
        private int maxSurfaceFormsPerAnalyzedForm;
        private IntsRefBuilder scratchInts = new IntsRefBuilder();
        private final PairOutputs<Long, BytesRef> outputs;
        private boolean hasPayloads;
        private BytesRefBuilder analyzed = new BytesRefBuilder();
        private SurfaceFormPriorityQueue surfaceFormPriorityQueue;
        private int payloadSep;
        private int docID;

        public XBuilder(int maxSurfaceFormsPerAnalyzedForm, boolean hasPayloads, int payloadSep) {
            this.payloadSep = payloadSep;
            this.outputs = new PairOutputs<>(PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton());
            this.builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
            this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;
            this.hasPayloads = hasPayloads;
            this.surfaceFormPriorityQueue = new SurfaceFormPriorityQueue(maxSurfaceFormsPerAnalyzedForm);
        }

        public void startTerm(BytesRef analyzed) {
            this.analyzed.grow(analyzed.length + 2);
            this.analyzed.copyBytes(analyzed);
        }

        public void setDocID(final int docID) {
            this.docID = docID;
        }

        private final static class SurfaceFormAndPayload implements Comparable<SurfaceFormAndPayload> {
            int docID;
            BytesRef payload;
            long weight;

            public SurfaceFormAndPayload(int docID, BytesRef payload, long cost) {
                super();
                this.docID = docID;
                this.payload = payload;
                this.weight = cost;
            }

            @Override
            public int compareTo(SurfaceFormAndPayload o) {
                int res = compare(weight, o.weight);
                if (res == 0) {
                    return payload.compareTo(o.payload);
                }
                return res;
            }

            public static int compare(long x, long y) {
                return (x < y) ? -1 : ((x == y) ? 0 : 1);
            }
        }

        private final static class SurfaceFormPriorityQueue extends PriorityQueue<SurfaceFormAndPayload> {

            public SurfaceFormPriorityQueue(int maxSize) {
                super(maxSize);
            }

            @Override
            protected boolean lessThan(SurfaceFormAndPayload a, SurfaceFormAndPayload b) {
                return a.weight < b.weight;
            }

            /**
             * Returns results in descending order followed by sorting <code>SurfaceFormAndPayloads</code>
             * according to
             * {@link SurfaceFormAndPayload#compareTo(SurfaceFormAndPayload)}
             */
            public SurfaceFormAndPayload[] getResults() {
                int size = size();
                SurfaceFormAndPayload[] res = new SurfaceFormAndPayload[size];
                for (int i = size - 1; i >= 0; i--) {
                    res[i] = pop();
                }
                ArrayUtil.timSort(res, 0, size);
                return res;
            }
        }

        public int addSurface(BytesRef surface, BytesRef payload, long cost) throws IOException {
            long encodedWeight = cost == -1 ? cost : encodeWeight(cost);
            BytesRef payloadRef = XPayLoadProcessor.make(surface, payload, docID, payloadSep, hasPayloads);
            surfaceFormPriorityQueue.insertWithOverflow(new SurfaceFormAndPayload(this.docID, payloadRef, encodedWeight));
            return surfaceFormPriorityQueue.size();
        }

        public void finishTerm(long defaultWeight) throws IOException {
            SurfaceFormAndPayload[] surfaceFormAndPayloads = surfaceFormPriorityQueue.getResults();
            assert surfaceFormAndPayloads.length <= maxSurfaceFormsPerAnalyzedForm;
            int deduplicator = 0;
            analyzed.append((byte) END_BYTE);
            analyzed.setLength(analyzed.length() + 1);
            for (int i = 0; i < surfaceFormAndPayloads.length; i++) {
                analyzed.setByteAt(analyzed.length() - 1,  (byte) deduplicator++);
                Util.toIntsRef(analyzed.get(), scratchInts);
                SurfaceFormAndPayload candidate = surfaceFormAndPayloads[i];
                long cost = candidate.weight == -1 ? encodeWeight(Math.min(Integer.MAX_VALUE, defaultWeight)) : candidate.weight;
                builder.add(scratchInts.get(), outputs.newPair(cost, candidate.payload));
            }
            surfaceFormPriorityQueue.clear();
        }

        public FST<Pair<Long, BytesRef>> build() throws IOException {
            return builder.finish();
        }
    }

    /**
     * Helper to encode/decode payload with surface + PAYLOAD_SEP + payload + PAYLOAD_SEP + docID
     */
    static final class XPayLoadProcessor {
        final static private int MAX_DOC_ID_LEN_WITH_SEP = 6; // vint takes at most 5 bytes

        static class PayloadMetaData {
            BytesRef payload;
            BytesRef surfaceForm;
            int docID = -1;

            PayloadMetaData(BytesRef surfaceForm, BytesRef payload, int docID) {
                this.payload = payload;
                this.surfaceForm = surfaceForm;
                this.docID = docID;
            }

            public boolean hasDocID() {
                return docID != -1;
            }
        }

        private static int getDocIDSepIndex(final BytesRef output, final int payloadSep, final int payloadSepIndex) {
            int docSepIndex = -1;
            int start = (payloadSepIndex < output.length - MAX_DOC_ID_LEN_WITH_SEP) ? output.length - MAX_DOC_ID_LEN_WITH_SEP : payloadSepIndex + 1;
            // have to iterate forward, or else it will break if docID = PAYLOAD_SEP (31)
            for (int i = start; i < output.length; i++) {
                if (output.bytes[output.offset + i] == payloadSep) {
                    docSepIndex = i;
                    break;
                }
            }
            return docSepIndex;
        }

        static int getDocIDSepIndex(final BytesRef output, final boolean hasPayloads, final int payloadSep) {
            int docSepIndex = -1;
            if (hasPayloads) {
                int sepIndex = getPayloadSepIndex(output, payloadSep);
                docSepIndex = getDocIDSepIndex(output, payloadSep, sepIndex);
            } else {
                int start = (output.length - MAX_DOC_ID_LEN_WITH_SEP < 0) ? 0 : output.length - MAX_DOC_ID_LEN_WITH_SEP;
                for (int i = start; i < output.length; i++) {
                    if (output.bytes[output.offset + i] == payloadSep) {
                        docSepIndex = i;
                        break;
                    }
                }
            }
            return docSepIndex;
        }

        static int getPayloadSepIndex(final BytesRef output, final int payloadSep) {
            int sepIndex = -1;
            for (int i = 0; i < output.length; i++) {
                if (output.bytes[output.offset + i] == payloadSep) {
                    sepIndex = i;
                    break;
                }
            }
            return sepIndex;
        }

        static PayloadMetaData parse(final BytesRef output, final boolean hasPayloads, final int payloadSep, CharsRefBuilder spare) {
            BytesRef payload = null;
            BytesRef surfaceForm;
            int docID = -1;

            boolean docIDExists = false;
            int docSepIndex = -1;
            if (hasPayloads) {
                int sepIndex = getPayloadSepIndex(output, payloadSep);
                assert sepIndex != -1;
                docSepIndex = getDocIDSepIndex(output, payloadSep, sepIndex);
                spare.grow(sepIndex);
                docIDExists = docSepIndex != -1;
                int payloadLen = output.length - sepIndex - 1;
                if (docIDExists) {
                    payloadLen = docSepIndex - sepIndex - 1;
                }
                spare.copyUTF8Bytes(output.bytes, output.offset, sepIndex);
                payload = new BytesRef(payloadLen);
                System.arraycopy(output.bytes, sepIndex + 1, payload.bytes, 0, payloadLen);
                payload.length = payloadLen;
                surfaceForm = new BytesRef(spare.get());
            } else {
                docSepIndex = getDocIDSepIndex(output, false, payloadSep);
                docIDExists = docSepIndex != -1;
                int len = (docIDExists) ? docSepIndex : output.length;
                spare.grow(len);
                spare.copyUTF8Bytes(output.bytes, output.offset, len);
                surfaceForm = new BytesRef(spare.get());
            }

            if (docIDExists) {
                ByteArrayDataInput input = new ByteArrayDataInput(output.bytes, docSepIndex + 1, output.length - docSepIndex);
                docID = input.readVInt();
            }
            return new PayloadMetaData(surfaceForm, payload, docID);
        }

        static BytesRef make(BytesRef surface, final BytesRef payload, final int docID, final int payloadSep, final boolean hasPayloads) throws IOException {
            int len = surface.length + ((hasPayloads) ? 1 + payload.length : 0) + MAX_DOC_ID_LEN_WITH_SEP;
            byte[] buffer = new byte[len];
            ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
            output.writeBytes(surface.bytes, surface.length - surface.offset);
            if (hasPayloads) {
                output.writeByte((byte) payloadSep);
                output.writeBytes(payload.bytes, payload.length - payload.offset);
            }
            output.writeByte((byte) payloadSep);
            output.writeVInt(docID);

            return new BytesRef(buffer, 0, output.getPosition());
        }
    }
}
