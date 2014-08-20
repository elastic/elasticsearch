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
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.*;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.*;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.automaton.*;
import org.apache.lucene.util.fst.*;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.apache.lucene.util.fst.Util.Result;
import org.apache.lucene.util.fst.Util.TopResults;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.*;
import java.util.*;

/**
 * Currently A fork of {@link org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester}
 * with very limited NRT capabilities.
 * Only Supported NRT feature is deleted document filtering from suggestions.
 * NOTE: This is still largely a work in progress.
 *
 * TODO:
 *   - Support optional surface form deduplication
 *   - Flexible Scoring/Weighting:
 *     - allow factoring in external factors (lookup key, surface form) to weighting suggestions
 *     - support context using outputs
 *   - Support returning arbitrary StoredFields from documents of resulting suggestions
 *   - General Refactoring
 *   
 * @lucene.experimental
 */
public class XNRTSuggester extends Lookup {

  /**
   * FST<Weight,Surface>: 
   *  input is the analyzed form, with a null byte between terms
   *  weights are encoded as costs: (Integer.MAX_VALUE-weight)
   *  surface is the original, unanalyzed form.
   */
  private FST<Pair<Long,BytesRef>> fst = null;
  
  /** 
   * Analyzer that will be used for analyzing suggestions at
   * index time.
   */
  private final Analyzer indexAnalyzer;

  /** 
   * Analyzer that will be used for analyzing suggestions at
   * query time.
   */
  private final Analyzer queryAnalyzer;
  
  /** 
   * True if exact match suggestions should always be returned first.
   */
  private final boolean exactFirst;
  
  /** 
   * True if separator between tokens should be preserved.
   */
  private final boolean preserveSep;

  /** Include this flag in the options parameter to {@link
   *  #XAnalyzingSuggester(Analyzer,Analyzer,int,int,int,boolean,FST,boolean,int,int,int,int,int)} to always
   *  return the exact match first, regardless of score.  This
   *  has no performance impact but could result in
   *  low-quality suggestions. */
  public static final int EXACT_FIRST = 1;

  /** Include this flag in the options parameter to {@link
   *  #XAnalyzingSuggester(Analyzer,Analyzer,int,int,int,boolean,FST,boolean,int,int,int,int,int)} to preserve
   *  token separators when matching. */
  public static final int PRESERVE_SEP = 2;

  /** Represents the separation between tokens, if
   *  PRESERVE_SEP was specified */
  public static final int SEP_LABEL = '\u001F';

  /** Marks end of the analyzed input and start of dedup
   *  byte. */
  public static final int END_BYTE = 0x0;

  /** Maximum number of dup surface forms (different surface
   *  forms for the same analyzed form). */
  private final int maxSurfaceFormsPerAnalyzedForm;

  /** Maximum graph paths to index for a single analyzed
   *  surface form.  This only matters if your analyzer
   *  makes lots of alternate paths (e.g. contains
   *  SynonymFilter). */
  private final int maxGraphExpansions;

  /** Highest number of analyzed paths we saw for any single
   *  input surface form.  For analyzers that never create
   *  graphs this will always be 1. */
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

  /** Whether position holes should appear in the automaton. */
  private boolean preservePositionIncrements;

    /**
   * Calls {@link #XAnalyzingSuggester(Analyzer,Analyzer,int,int,int,boolean,FST,boolean,int,int,int,int,int)
   * AnalyzingSuggester(analyzer, analyzer, EXACT_FIRST |
   * PRESERVE_SEP, 256, -1)}
   */
  public XNRTSuggester(Analyzer analyzer) {
    this(analyzer, null, analyzer, EXACT_FIRST | PRESERVE_SEP, 256, -1, true, null, false, 0, SEP_LABEL, PAYLOAD_SEP, END_BYTE, HOLE_CHARACTER);
  }

  /**
   * Calls {@link #XAnalyzingSuggester(Analyzer,Analyzer,int,int,int,boolean,FST,boolean,int,int,int,int,int)
   * AnalyzingSuggester(indexAnalyzer, queryAnalyzer, EXACT_FIRST |
   * PRESERVE_SEP, 256, -1)}
   */
  public XNRTSuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer) {
    this(indexAnalyzer, null, queryAnalyzer, EXACT_FIRST | PRESERVE_SEP, 256, -1, true, null, false, 0, SEP_LABEL, PAYLOAD_SEP, END_BYTE, HOLE_CHARACTER);
  }

  /**
   * Creates a new suggester.
   * 
   * @param indexAnalyzer Analyzer that will be used for
   *   analyzing suggestions while building the index.
   * @param queryAnalyzer Analyzer that will be used for
   *   analyzing query text during lookup
   * @param options see {@link #EXACT_FIRST}, {@link #PRESERVE_SEP}
   * @param maxSurfaceFormsPerAnalyzedForm Maximum number of
   *   surface forms to keep for a single analyzed form.
   *   When there are too many surface forms we discard the
   *   lowest weighted ones.
   * @param maxGraphExpansions Maximum number of graph paths
   *   to expand from the analyzed form.  Set this to -1 for
   *   no limit.
   */
  public XNRTSuggester(Analyzer indexAnalyzer, Automaton queryPrefix, Analyzer queryAnalyzer, int options, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions,
                             boolean preservePositionIncrements, FST<Pair<Long, BytesRef>> fst, boolean hasPayloads, int maxAnalyzedPathsForOneInput,
                             int sepLabel, int payloadSep, int endByte, int holeCharacter) {
      // SIMON EDIT: I added fst, hasPayloads and maxAnalyzedPathsForOneInput 
    this.indexAnalyzer = indexAnalyzer;
    this.queryAnalyzer = queryAnalyzer;
    this.fst = fst;
    this.hasPayloads = hasPayloads;
    if ((options & ~(EXACT_FIRST | PRESERVE_SEP)) != 0) {
      throw new IllegalArgumentException("options should only contain EXACT_FIRST and PRESERVE_SEP; got " + options);
    }
    this.exactFirst = (options & EXACT_FIRST) != 0;
    this.preserveSep = (options & PRESERVE_SEP) != 0;

    // FLORIAN EDIT: I added <code>queryPrefix</code> for context dependent suggestions
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

  /** Returns byte size of the underlying FST. */
  public long ramBytesUsed() {
    return fst == null ? 0 : fst.ramBytesUsed();
  }

  private static void copyDestTransitions(State from, State to, List<Transition> transitions) {
    if (to.isAccept()) {
      from.setAccept(true);
    }
    for(Transition t : to.getTransitions()) {
      transitions.add(t);
    }
  }

  // Replaces SEP with epsilon or remaps them if
  // we were asked to preserve them:
  private static void replaceSep(Automaton a, boolean preserveSep, int replaceSep) {

    State[] states = a.getNumberedStates();

    // Go in reverse topo sort so we know we only have to
    // make one pass:
    for(int stateNumber=states.length-1;stateNumber >=0;stateNumber--) {
      final State state = states[stateNumber];
      List<Transition> newTransitions = new ArrayList<>();
      for(Transition t : state.getTransitions()) {
        assert t.getMin() == t.getMax();
        if (t.getMin() == TokenStreamToAutomaton.POS_SEP) {
          if (preserveSep) {
            // Remap to SEP_LABEL:
            newTransitions.add(new Transition(replaceSep, t.getDest()));
          } else {
            copyDestTransitions(state, t.getDest(), newTransitions);
            a.setDeterministic(false);
          }
        } else if (t.getMin() == TokenStreamToAutomaton.HOLE) {

          // Just remove the hole: there will then be two
          // SEP tokens next to each other, which will only
          // match another hole at search time.  Note that
          // it will also match an empty-string token ... if
          // that's somehow a problem we can always map HOLE
          // to a dedicated byte (and escape it in the
          // input).
          copyDestTransitions(state, t.getDest(), newTransitions);
          a.setDeterministic(false);
        } else {
          newTransitions.add(t);
        }
      }
      state.setTransitions(newTransitions.toArray(new Transition[newTransitions.size()]));
    }
  }

  protected Automaton convertAutomaton(Automaton a) {
    if (queryPrefix != null) {
      a = Automaton.concatenate(Arrays.asList(queryPrefix, a));
      BasicOperations.determinize(a);
    }
    return a;
  }

  /** Just escapes the 0xff byte (which we still for SEP). */
  private static final class  EscapingTokenStreamToAutomaton extends TokenStreamToAutomaton {

    final BytesRef spare = new BytesRef();
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
          if (spare.bytes.length == upto) {
            spare.grow(upto+2);
          }
          spare.bytes[upto++] = (byte) sepLabel;
          spare.bytes[upto++] = b;
        } else {
          if (spare.bytes.length == upto) {
            spare.grow(upto+1);
          }
          spare.bytes[upto++] = b;
        }
      }
      spare.offset = 0;
      spare.length = upto;
      return spare;
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
  
  @Override
  public void build(InputIterator iterator) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean store(OutputStream output) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean load(InputStream input) throws IOException {
    throw new UnsupportedOperationException();
  }

  private LookupResult getLookupResult(Long output1, BytesRef output2, CharsRef spare) {
    XPayLoadProcessor.PayloadMetaData metaData = XPayLoadProcessor.parse(output2, hasPayloads, payloadSep, spare);
    return new LookupResult(spare.toString(), decodeWeight(output1), metaData.payload);
  }

  private boolean sameSurfaceForm(BytesRef key, BytesRef output2) {
    if (hasPayloads) {
      // output2 has at least PAYLOAD_SEP byte:
      if (key.length >= output2.length) {
        return false;
      }
      for(int i=0;i<key.length;i++) {
        if (key.bytes[key.offset+i] != output2.bytes[output2.offset+i]) {
          return false;
        }
      }
      return output2.bytes[output2.offset + key.length] == payloadSep;
    } else {
      int docIDSepIndex = XPayLoadProcessor.getDocIDSepIndex(output2, false, payloadSep);
      if (docIDSepIndex != -1) {
        BytesRef spare = new BytesRef(output2.bytes, output2.offset, output2.length - 1 - docIDSepIndex);
        return key.bytesEquals(spare);
      }
      return key.bytesEquals(output2);
    }
  }

  @Override
  public List<LookupResult> lookup(final CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) {
      return lookup(key, contexts, onlyMorePopular, num, null, 1.0d);
  }

  public List<LookupResult> lookup(final CharSequence key, final int num, final AtomicReader reader) {
      double liveDocRatio = 1.0d;
      if (reader.numDocs() > 0) {
        liveDocRatio = (double) reader.numDocs() / reader.maxDoc();
      } else {
        return Collections.emptyList();
      }
      return lookup(key, null, false, num, reader.getLiveDocs(), liveDocRatio);
  }

  private List<LookupResult> lookup(final CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num, final Bits liveDocs, final double liveDocsRatio) {
    assert num > 0;

    if (onlyMorePopular) {
      throw new IllegalArgumentException("this suggester only works with onlyMorePopular=false");
    }
    if (fst == null) {
      return Collections.emptyList();
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
    final BytesRef utf8Key = new BytesRef(key);
    try {

      Automaton lookupAutomaton = toLookupAutomaton(key);

      final CharsRef spare = new CharsRef();

      //System.out.println("  now intersect exactFirst=" + exactFirst);
    
      // Intersect automaton w/ suggest wFST and get all
      // prefix starting nodes & their outputs:
      //final PathIntersector intersector = getPathIntersector(lookupAutomaton, fst);

      //System.out.println("  prefixPaths: " + prefixPaths.size());

      BytesReader bytesReader = fst.getBytesReader();

      FST.Arc<Pair<Long,BytesRef>> scratchArc = new FST.Arc<>();

      final List<LookupResult> results = new ArrayList<>();

      List<FSTUtil.Path<Pair<Long,BytesRef>>> prefixPaths = FSTUtil.intersectPrefixPaths(convertAutomaton(lookupAutomaton), fst);

      if (exactFirst) {

        int count = 0;
        for (FSTUtil.Path<Pair<Long,BytesRef>> path : prefixPaths) {
          if (fst.findTargetArc(endByte, path.fstNode, scratchArc, bytesReader) != null) {
            // This node has END_BYTE arc leaving, meaning it's an
            // "exact" match:
            count++;
          }
        }

        // Searcher just to find the single exact only
        // match, if present:
        Util.TopNSearcher<Pair<Long,BytesRef>> searcher;
        searcher = new Util.TopNSearcher<Pair<Long, BytesRef>>(fst, count * maxSurfaceFormsPerAnalyzedForm, getMaxTopNSearcherQueueSize(count, liveDocsRatio), weightComparator) {

          @Override
          protected boolean acceptResult(IntsRef input, Pair<Long,BytesRef> output) {
            XPayLoadProcessor.PayloadMetaData metaData = XPayLoadProcessor.parse(output.output2, hasPayloads, payloadSep, spare);
            if (liveDocs != null && metaData.hasDocID()) {
              if (!liveDocs.get(metaData.docID)) {
                  return false;
              }
            }
            return true;
          }
        };

        // NOTE: we could almost get away with only using
        // the first start node.  The only catch is if
        // maxSurfaceFormsPerAnalyzedForm had kicked in and
        // pruned our exact match from one of these nodes
        // ...:
        for (FSTUtil.Path<Pair<Long,BytesRef>> path : prefixPaths) {
          if (fst.findTargetArc(endByte, path.fstNode, scratchArc, bytesReader) != null) {
            // This node has END_BYTE arc leaving, meaning it's an
            // "exact" match:
            searcher.addStartPaths(scratchArc, fst.outputs.add(path.output, scratchArc.output), false, path.input);
          }
        }

        Util.TopResults<Pair<Long,BytesRef>> completions = searcher.search();

        // NOTE: this is rather inefficient: we enumerate
        // every matching "exactly the same analyzed form"
        // path, and then do linear scan to see if one of
        // these exactly matches the input.  It should be
        // possible (though hairy) to do something similar
        // to getByOutput, since the surface form is encoded
        // into the FST output, so we more efficiently hone
        // in on the exact surface-form match.  Still, I
        // suspect very little time is spent in this linear
        // seach: it's bounded by how many prefix start
        // nodes we have and the
        // maxSurfaceFormsPerAnalyzedForm:
        for(Result<Pair<Long,BytesRef>> completion : completions) {
          BytesRef output2 = completion.output.output2;
          if (sameSurfaceForm(utf8Key, output2)) {
            results.add(getLookupResult(completion.output.output1, output2, spare));
            break;
          }
        }

        if (results.size() == num) {
          // That was quick:
          return results;
        }
      }

      Util.TopNSearcher<Pair<Long,BytesRef>> searcher;
      searcher = new Util.TopNSearcher<Pair<Long,BytesRef>>(fst,
                                                            num - results.size(),
                                                            getMaxTopNSearcherQueueSize(num, liveDocsRatio),
                                                            weightComparator) {
        private final Set<BytesRef> seen = new HashSet<>();
        private CharsRef spare = new CharsRef();

        @Override
        protected boolean acceptResult(IntsRef input, Pair<Long,BytesRef> output) {

          XPayLoadProcessor.PayloadMetaData metaData = XPayLoadProcessor.parse(output.output2, hasPayloads, payloadSep, spare);
          if (liveDocs != null && metaData.hasDocID()) {
            if (!liveDocs.get(metaData.docID)) {
              return false;
            }
          }
          // Dedup: when the input analyzes to a graph we
          // can get duplicate surface forms:
          if (seen.contains(metaData.surfaceForm)) {
              return false;
          }
          seen.add(metaData.surfaceForm);

          if (!exactFirst) {
            return true;
          } else {
            // In exactFirst mode, don't accept any paths
            // matching the surface form since that will
            // create duplicate results:
            if (metaData.surfaceForm.bytesEquals(utf8Key)) {
              // We found exact match, which means we should
              // have already found it in the first search:
              assert results.size() == 1;
              return false;
            } else {
              return true;
            }
          }
        }
      };

      prefixPaths = getFullPrefixPaths(prefixPaths, lookupAutomaton, fst);
      
      for (FSTUtil.Path<Pair<Long,BytesRef>> path : prefixPaths) {
        searcher.addStartPaths(path.fstNode, path.output, true, path.input);
      }

      TopResults<Pair<Long,BytesRef>> completions = searcher.search();
      // search admissibility is not guaranteed
      // see comment on getMaxTopNSearcherQueueSize
      //assert completions.isComplete;

      for(Result<Pair<Long,BytesRef>> completion : completions) {

        LookupResult result = getLookupResult(completion.output.output1, completion.output.output2, spare);

        // TODO: for fuzzy case would be nice to return
        // how many edits were required

        //System.out.println("    result=" + result);
        results.add(result);

        if (results.size() == num) {
          // In the exactFirst=true case the search may
          // produce one extra path
          break;
        }
      }

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

  @Override
  public boolean store(DataOutput output) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean load(DataInput input) throws IOException {
    throw new UnsupportedOperationException();
  }

    /** Returns all completion paths to initialize the search. */
  protected List<FSTUtil.Path<Pair<Long,BytesRef>>> getFullPrefixPaths(List<FSTUtil.Path<Pair<Long,BytesRef>>> prefixPaths,
                                                                       Automaton lookupAutomaton,
                                                                       FST<Pair<Long,BytesRef>> fst)
    throws IOException {
    return prefixPaths;
  }

  public final Set<IntsRef> toFiniteStrings(final TokenStreamToAutomaton ts2a, TokenStream ts) throws IOException {
      // Analyze surface form:

      // Create corresponding automaton: labels are bytes
      // from each analyzed token, with byte 0 used as
      // separator between tokens:
      Automaton automaton = ts2a.toAutomaton(ts);
      ts.close();

      replaceSep(automaton, preserveSep, sepLabel);

      // Get all paths from the automaton (there can be
      // more than one path, eg if the analyzer created a
      // graph using SynFilter or WDF):

      // TODO: we could walk & add simultaneously, so we
      // don't have to alloc [possibly biggish]
      // intermediate HashSet in RAM:
      return SpecialOperations.getFiniteStrings(automaton, maxGraphExpansions);
  }

  final Automaton toLookupAutomaton(final CharSequence key) throws IOException {
    // Turn tokenstream into automaton:
    TokenStream ts = queryAnalyzer.tokenStream("", key.toString());
    Automaton automaton = (getTokenStreamToAutomaton()).toAutomaton(ts);
    ts.close();

    // TODO: we could use the end offset to "guess"
    // whether the final token was a partial token; this
    // would only be a heuristic ... but maybe an OK one.
    // This way we could eg differentiate "net" from "net ",
    // which we can't today...

    replaceSep(automaton, preserveSep, sepLabel);

    // TODO: we can optimize this somewhat by determinizing
    // while we convert
    BasicOperations.determinize(automaton);
    return automaton;
  }
  
  

  /**
   * Returns the weight associated with an input string,
   * or null if it does not exist.
   */
  public Object get(CharSequence key) {
    throw new UnsupportedOperationException();
  }
  
  /** cost -> weight */
  public static int decodeWeight(long encoded) {
    return (int)(Integer.MAX_VALUE - encoded);
  }
  
  /** weight -> cost */
  public static int encodeWeight(long value) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + value);
    }
    return Integer.MAX_VALUE - (int)value;
  }
   
  static final Comparator<Pair<Long,BytesRef>> weightComparator = new Comparator<Pair<Long,BytesRef>> () {
    @Override
    public int compare(Pair<Long,BytesRef> left, Pair<Long,BytesRef> right) {
      return left.output1.compareTo(right.output1);
    }
  };

    public static class XBuilder {
        private Builder<Pair<Long, BytesRef>> builder;
        private int maxSurfaceFormsPerAnalyzedForm;
        private IntsRef scratchInts = new IntsRef();
        private final PairOutputs<Long, BytesRef> outputs;
        private boolean hasPayloads;
        private BytesRef analyzed = new BytesRef();
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
            this.analyzed.copyBytes(analyzed);
            this.analyzed.grow(analyzed.length + 2);
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
            analyzed.bytes[analyzed.offset + analyzed.length] = END_BYTE;
            analyzed.length += 2;
            for (int i = 0; i < surfaceFormAndPayloads.length; i++) {
                analyzed.bytes[analyzed.offset + analyzed.length - 1] = (byte) deduplicator++;
                Util.toIntsRef(analyzed, scratchInts);
                SurfaceFormAndPayload candidate = surfaceFormAndPayloads[i];
                long cost = candidate.weight == -1 ? encodeWeight(Math.min(Integer.MAX_VALUE, defaultWeight)) : candidate.weight;
                builder.add(scratchInts, outputs.newPair(cost, candidate.payload));
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

      static PayloadMetaData parse(final BytesRef output, final boolean hasPayloads, final int payloadSep, CharsRef spare) {
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
              UnicodeUtil.UTF8toUTF16(output.bytes, output.offset, sepIndex, spare);
              payload = new BytesRef(payloadLen);
              System.arraycopy(output.bytes, sepIndex + 1, payload.bytes, 0, payloadLen);
              payload.length = payloadLen;
              surfaceForm = new BytesRef(spare);
          } else {
              docSepIndex = getDocIDSepIndex(output, false, payloadSep);
              docIDExists = docSepIndex != -1;
              int len = (docIDExists) ? docSepIndex : output.length;
              spare.grow(len);
              UnicodeUtil.UTF8toUTF16(output.bytes, output.offset, len, spare);
              surfaceForm = new BytesRef(spare);
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
