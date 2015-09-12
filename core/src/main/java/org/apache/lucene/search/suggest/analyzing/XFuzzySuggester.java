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
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.*;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

/**
 * Implements a fuzzy {@link AnalyzingSuggester}. The similarity measurement is
 * based on the Damerau-Levenshtein (optimal string alignment) algorithm, though
 * you can explicitly choose classic Levenshtein by passing <code>false</code>
 * for the <code>transpositions</code> parameter.
 * <p>
 * At most, this query will match terms up to
 * {@value org.apache.lucene.util.automaton.LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}
 * edits. Higher distances are not supported.  Note that the
 * fuzzy distance is measured in "byte space" on the bytes
 * returned by the {@link org.apache.lucene.analysis.TokenStream}'s {@link
 * org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute}, usually UTF8.  By default
 * the analyzed bytes must be at least 3 {@link
 * #DEFAULT_MIN_FUZZY_LENGTH} bytes before any edits are
 * considered.  Furthermore, the first 1 {@link
 * #DEFAULT_NON_FUZZY_PREFIX} byte is not allowed to be
 * edited.  We allow up to 1 (@link
 * #DEFAULT_MAX_EDITS} edit.
 * If {@link #unicodeAware} parameter in the constructor is set to true, maxEdits,
 * minFuzzyLength, transpositions and nonFuzzyPrefix are measured in Unicode code
 * points (actual letters) instead of bytes.*
 *
 * <p>
 * NOTE: This suggester does not boost suggestions that
 * required no edits over suggestions that did require
 * edits.  This is a known limitation.
 *
 * <p>
 * Note: complex query analyzers can have a significant impact on the lookup
 * performance. It's recommended to not use analyzers that drop or inject terms
 * like synonyms to keep the complexity of the prefix intersection low for good
 * lookup performance. At index time, complex analyzers can safely be used.
 * </p>
 *
 * @lucene.experimental
 */
public final class XFuzzySuggester extends XAnalyzingSuggester {
    private final int maxEdits;
    private final boolean transpositions;
    private final int nonFuzzyPrefix;
    private final int minFuzzyLength;
    private final boolean unicodeAware;

    /**
     *  Measure maxEdits, minFuzzyLength, transpositions and nonFuzzyPrefix
     *  parameters in Unicode code points (actual letters)
     *  instead of bytes.
     */
    public static final boolean DEFAULT_UNICODE_AWARE = false;

    /**
     * The default minimum length of the key passed to {@link
     * #lookup} before any edits are allowed.
     */
    public static final int DEFAULT_MIN_FUZZY_LENGTH = 3;

    /**
     * The default prefix length where edits are not allowed.
     */
    public static final int DEFAULT_NON_FUZZY_PREFIX = 1;

    /**
     * The default maximum number of edits for fuzzy
     * suggestions.
     */
    public static final int DEFAULT_MAX_EDITS = 1;

    /**
     * The default transposition value passed to {@link org.apache.lucene.util.automaton.LevenshteinAutomata}
     */
    public static final boolean DEFAULT_TRANSPOSITIONS = true;

    /**
     * Creates a {@link FuzzySuggester} instance initialized with default values.
     *
     * @param analyzer the analyzer used for this suggester
     */
    public XFuzzySuggester(Analyzer analyzer) {
        this(analyzer, analyzer);
    }

    /**
     * Creates a {@link FuzzySuggester} instance with an index & a query analyzer initialized with default values.
     *
     * @param indexAnalyzer
     *           Analyzer that will be used for analyzing suggestions while building the index.
     * @param queryAnalyzer
     *           Analyzer that will be used for analyzing query text during lookup
     */
    public XFuzzySuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer) {
        this(indexAnalyzer, null, queryAnalyzer, EXACT_FIRST | PRESERVE_SEP, 256, -1, DEFAULT_MAX_EDITS, DEFAULT_TRANSPOSITIONS,
                DEFAULT_NON_FUZZY_PREFIX, DEFAULT_MIN_FUZZY_LENGTH, DEFAULT_UNICODE_AWARE, null, false, 0, SEP_LABEL, PAYLOAD_SEP, END_BYTE, HOLE_CHARACTER);

    }

    /**
     * Creates a {@link FuzzySuggester} instance.
     *
     * @param indexAnalyzer Analyzer that will be used for
     *        analyzing suggestions while building the index.
     * @param queryAnalyzer Analyzer that will be used for
     *        analyzing query text during lookup
     * @param options see {@link #EXACT_FIRST}, {@link #PRESERVE_SEP}
     * @param maxSurfaceFormsPerAnalyzedForm Maximum number of
     *        surface forms to keep for a single analyzed form.
     *        When there are too many surface forms we discard the
     *        lowest weighted ones.
     * @param maxGraphExpansions Maximum number of graph paths
     *        to expand from the analyzed form.  Set this to -1 for
     *        no limit.
     * @param maxEdits must be >= 0 and <= {@link org.apache.lucene.util.automaton.LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE} .
     * @param transpositions <code>true</code> if transpositions should be treated as a primitive
     *        edit operation. If this is false, comparisons will implement the classic
     *        Levenshtein algorithm.
     * @param nonFuzzyPrefix length of common (non-fuzzy) prefix (see default {@link #DEFAULT_NON_FUZZY_PREFIX}
     * @param minFuzzyLength minimum length of lookup key before any edits are allowed (see default {@link #DEFAULT_MIN_FUZZY_LENGTH})
     * @param sepLabel separation label
     * @param payloadSep payload separator byte
     * @param endByte end byte marker byte
     */
    public XFuzzySuggester(Analyzer indexAnalyzer, Automaton queryPrefix, Analyzer queryAnalyzer, int options, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions,
                           int maxEdits, boolean transpositions, int nonFuzzyPrefix, int minFuzzyLength, boolean unicodeAware,
                           FST<PairOutputs.Pair<Long, BytesRef>> fst, boolean hasPayloads, int maxAnalyzedPathsForOneInput,
                           int sepLabel, int payloadSep, int endByte, int holeCharacter) {
        super(indexAnalyzer, queryPrefix, queryAnalyzer, options, maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions, true, fst, hasPayloads, maxAnalyzedPathsForOneInput, sepLabel, payloadSep, endByte, holeCharacter);
        if (maxEdits < 0 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
            throw new IllegalArgumentException("maxEdits must be between 0 and " + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE);
        }
        if (nonFuzzyPrefix < 0) {
            throw new IllegalArgumentException("nonFuzzyPrefix must not be >= 0 (got " + nonFuzzyPrefix + ")");
        }
        if (minFuzzyLength < 0) {
            throw new IllegalArgumentException("minFuzzyLength must not be >= 0 (got " + minFuzzyLength + ")");
        }

        this.maxEdits = maxEdits;
        this.transpositions = transpositions;
        this.nonFuzzyPrefix = nonFuzzyPrefix;
        this.minFuzzyLength = minFuzzyLength;
        this.unicodeAware = unicodeAware;
    }

    @Override
    protected List<FSTUtil.Path<PairOutputs.Pair<Long,BytesRef>>> getFullPrefixPaths(List<FSTUtil.Path<PairOutputs.Pair<Long,BytesRef>>> prefixPaths,
                                                                                     Automaton lookupAutomaton,
                                                                                     FST<PairOutputs.Pair<Long,BytesRef>> fst)
            throws IOException {

        // TODO: right now there's no penalty for fuzzy/edits,
        // ie a completion whose prefix matched exactly what the
        // user typed gets no boost over completions that
        // required an edit, which get no boost over completions
        // requiring two edits.  I suspect a multiplicative
        // factor is appropriate (eg, say a fuzzy match must be at
        // least 2X better weight than the non-fuzzy match to
        // "compete") ... in which case I think the wFST needs
        // to be log weights or something ...

        Automaton levA = convertAutomaton(toLevenshteinAutomata(lookupAutomaton));
    /*
      Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"), "UTF-8");
      w.write(levA.toDot());
      w.close();
      System.out.println("Wrote LevA to out.dot");
    */
        return FSTUtil.intersectPrefixPaths(levA, fst);
    }

    @Override
    protected Automaton convertAutomaton(Automaton a) {
      if (unicodeAware) {
        // FLORIAN EDIT: get converted Automaton from superclass
        Automaton utf8automaton = new UTF32ToUTF8().convert(super.convertAutomaton(a));
        // This automaton should not blow up during determinize:
        utf8automaton = Operations.determinize(utf8automaton, Integer.MAX_VALUE);
        return utf8automaton;
      } else {
        return super.convertAutomaton(a);
      }
    }

    @Override
    public TokenStreamToAutomaton getTokenStreamToAutomaton() {
      final TokenStreamToAutomaton tsta = super.getTokenStreamToAutomaton();
      tsta.setUnicodeArcs(unicodeAware);
      return tsta;
    }

    Automaton toLevenshteinAutomata(Automaton automaton) {
        List<Automaton> subs = new ArrayList<>();
        FiniteStringsIterator finiteStrings = new FiniteStringsIterator(automaton);
        for (IntsRef string; (string = finiteStrings.next()) != null;) {
          if (string.length <= nonFuzzyPrefix || string.length < minFuzzyLength) {
            subs.add(Automata.makeString(string.ints, string.offset, string.length));
          } else {
            int ints[] = new int[string.length-nonFuzzyPrefix];
            System.arraycopy(string.ints, string.offset+nonFuzzyPrefix, ints, 0, ints.length);
            // TODO: maybe add alphaMin to LevenshteinAutomata,
            // and pass 1 instead of 0?  We probably don't want
            // to allow the trailing dedup bytes to be
            // edited... but then 0 byte is "in general" allowed
            // on input (but not in UTF8).
            LevenshteinAutomata lev = new LevenshteinAutomata(ints, unicodeAware ? Character.MAX_CODE_POINT : 255, transpositions);
            subs.add(lev.toAutomaton(maxEdits, UnicodeUtil.newString(string.ints, string.offset, nonFuzzyPrefix)));
          }
        }

        if (subs.isEmpty()) {
          // automaton is empty, there is no accepted paths through it
          return Automata.makeEmpty(); // matches nothing
        } else if (subs.size() == 1) {
          // no synonyms or anything: just a single path through the tokenstream
          return subs.get(0);
        } else {
          // multiple paths: this is really scary! is it slow?
          // maybe we should not do this and throw UOE?
          Automaton a = Operations.union(subs);
          // TODO: we could call toLevenshteinAutomata() before det? 
          // this only happens if you have multiple paths anyway (e.g. synonyms)
          return Operations.determinize(a, DEFAULT_MAX_DETERMINIZED_STATES);
        }
      }
}
