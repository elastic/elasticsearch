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

package org.apache.lucene.analysis.synonym;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockGraphTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TokenStreamToTermAutomatonQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SynonymGraphFilterTests extends BaseTokenStreamTestCase {

    /**
     * Set a side effect by {@link #getAnalyzer}.
     */
    private SynonymGraphFilter synFilter;

    // LUCENE-6664
    public static void assertAnalyzesToPositions(Analyzer a, String input, String[] output, String[] types, int[] posIncrements, int[]
        posLengths) throws IOException {
        assertAnalyzesTo(a, input, output, null, null, types, posIncrements, posLengths);
    }

    public void testBasicKeepOrigOneOutput() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x", true);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a b", new String[]{"c", "x", "a", "b"}, new int[]{0, 2, 2, 4}, new int[]{1, 5, 3, 5}, new String[]{"word",
            "SYNONYM", "word", "word"}, new int[]{1, 1, 0, 1}, new int[]{1, 2, 1, 1});
        a.close();
    }

    public void testMixedKeepOrig() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x", true);
        add(b, "e f", "y", false);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a b c e f g", new String[]{"c", "x", "a", "b", "c", "y", "g"}, new int[]{0, 2, 2, 4, 6, 8, 12}, new
            int[]{1, 5, 3, 5, 7, 11, 13}, new String[]{"word", "SYNONYM", "word", "word", "word", "SYNONYM", "word"}, new
            int[]{1, 1, 0,
            1, 1, 1, 1}, new int[]{1, 2, 1, 1, 1, 1, 1});
        a.close();
    }

    public void testNoParseAfterBuffer() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "b a", "x", true);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "b b b", new String[]{"b", "b", "b"}, new int[]{0, 2, 4}, new int[]{1, 3, 5}, new String[]{"word", "word",
            "word"}, new int[]{1, 1, 1}, new int[]{1, 1, 1});
        a.close();
    }

    public void testOneInputMultipleOutputKeepOrig() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x", true);
        add(b, "a b", "y", true);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a b c", new String[]{"c", "x", "y", "a", "b", "c"}, new int[]{0, 2, 2, 2, 4, 6}, new int[]{1, 5, 5, 3, 5,
            7}, new String[]{"word", "SYNONYM", "SYNONYM", "word", "word", "word"}, new int[]{1, 1, 0, 0, 1, 1, 1, 1}, new
            int[]{1, 2, 2,
            1, 1, 1, 1, 1});
        a.close();
    }

    /**
     * parse a syn file with bad syntax
     */
    public void testInvalidAnalyzesToNothingOutput() throws Exception {
        String testFile = "a => 1";
        Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, false);
        SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
        try {
            parser.parse(new StringReader(testFile));
            fail("didn't get expected exception");
        } catch (ParseException expected) {
            // expected exc
        }
        analyzer.close();
    }

    /**
     * parse a syn file with bad syntax
     */
    public void testInvalidDoubleMap() throws Exception {
        String testFile = "a => b => c";
        Analyzer analyzer = new MockAnalyzer(random());
        SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
        try {
            parser.parse(new StringReader(testFile));
            fail("didn't get expected exception");
        } catch (ParseException expected) {
            // expected exc
        }
        analyzer.close();
    }

    public void testMoreThanOneLookAhead() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b c d", "x", true);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "a b c e", new String[]{"a", "b", "c", "e"}, new int[]{0, 2, 4, 6}, new int[]{1, 3, 5, 7}, new
            String[]{"word", "word", "word", "word"}, new int[]{1, 1, 1, 1}, new int[]{1, 1, 1, 1});
        a.close();
    }

    public void testLookaheadAfterParse() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "b b", "x", true);
        add(b, "b", "y", true);

        Analyzer a = getAnalyzer(b, true);

        assertAnalyzesTo(a, "b a b b", new String[]{"y", "b", "a", "x", "b", "b"}, new int[]{0, 0, 2, 4, 4, 6}, new int[]{1, 1, 3, 7, 5,
            7}, null, new int[]{1, 0, 1, 1, 0, 1}, new int[]{1, 1, 1, 2, 1, 1}, true);
    }

    public void testLookaheadSecondParse() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "b b b", "x", true);
        add(b, "b", "y", true);

        Analyzer a = getAnalyzer(b, true);

        assertAnalyzesTo(a, "b b", new String[]{"y", "b", "y", "b"}, new int[]{0, 0, 2, 2}, new int[]{1, 1, 3, 3}, null, new int[]{1, 0,
            1, 0}, new int[]{1, 1, 1, 1}, true);
    }

    public void testOneInputMultipleOutputNoKeepOrig() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x", false);
        add(b, "a b", "y", false);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a b c", new String[]{"c", "x", "y", "c"}, new int[]{0, 2, 2, 6}, new int[]{1, 5, 5, 7}, new
            String[]{"word", "SYNONYM", "SYNONYM", "word"}, new int[]{1, 1, 0, 1}, new int[]{1, 1, 1, 1});
        a.close();
    }

    public void testOneInputMultipleOutputMixedKeepOrig() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x", true);
        add(b, "a b", "y", false);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a b c", new String[]{"c", "x", "y", "a", "b", "c"}, new int[]{0, 2, 2, 2, 4, 6}, new int[]{1, 5, 5, 3, 5,
            7}, new String[]{"word", "SYNONYM", "SYNONYM", "word", "word", "word"}, new int[]{1, 1, 0, 0, 1, 1, 1, 1}, new
            int[]{1, 2, 2,
            1, 1, 1, 1, 1});
        a.close();
    }

    public void testSynAtEnd() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x", true);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c d e a b", new String[]{"c", "d", "e", "x", "a", "b"}, new int[]{0, 2, 4, 6, 6, 8}, new int[]{1, 3, 5, 9,
            7, 9}, new String[]{"word", "word", "word", "SYNONYM", "word", "word"}, new int[]{1, 1, 1, 1, 0, 1}, new int[]{1, 1, 1,
            2, 1,
            1});
        a.close();
    }

    public void testTwoSynsInARow() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a", "x", false);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a a b", new String[]{"c", "x", "x", "b"}, new int[]{0, 2, 4, 6}, new int[]{1, 3, 5, 7}, new
            String[]{"word", "SYNONYM", "SYNONYM", "word"}, new int[]{1, 1, 1, 1}, new int[]{1, 1, 1, 1});
        a.close();
    }

    public void testBasicKeepOrigTwoOutputs() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x y", true);
        add(b, "a b", "m n o", true);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a b d", new String[]{"c", "x", "m", "a", "y", "n", "o", "b", "d"}, new int[]{0, 2, 2, 2, 2, 2, 2, 4, 6},
            new int[]{1, 5, 5, 3, 5, 5, 5, 5, 7}, new String[]{"word", "SYNONYM", "SYNONYM", "word", "SYNONYM",
                "SYNONYM", "SYNONYM",
                "word", "word"}, new int[]{1, 1, 0, 0, 1, 1, 1, 1, 1}, new int[]{1, 1, 2, 4, 4, 1, 2, 1, 1});
        a.close();
    }

    public void testNoCaptureIfNoMatch() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x y", true);

        Analyzer a = getAnalyzer(b, true);

        assertAnalyzesTo(a, "c d d", new String[]{"c", "d", "d"}, new int[]{0, 2, 4}, new int[]{1, 3, 5}, new String[]{"word", "word",
            "word"}, new int[]{1, 1, 1}, new int[]{1, 1, 1});
        assertEquals(0, synFilter.getCaptureCount());
        a.close();
    }

    public void testBasicNotKeepOrigOneOutput() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x", false);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a b", new String[]{"c", "x"}, new int[]{0, 2}, new int[]{1, 5}, new String[]{"word", "SYNONYM"}, new
            int[]{1, 1}, new int[]{1, 1});
        a.close();
    }

    public void testBasicNoKeepOrigTwoOutputs() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x y", false);
        add(b, "a b", "m n o", false);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a b d", new String[]{"c", "x", "m", "y", "n", "o", "d"}, new int[]{0, 2, 2, 2, 2, 2, 6}, new int[]{1, 5,
            5, 5, 5, 5, 7}, new String[]{"word", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM",
            "word"}, new int[]{1, 1, 0, 1, 1,
            1, 1}, new int[]{1, 1, 2, 3, 1, 1, 1});
        a.close();
    }

    public void testIgnoreCase() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x y", false);
        add(b, "a b", "m n o", false);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c A B D", new String[]{"c", "x", "m", "y", "n", "o", "D"}, new int[]{0, 2, 2, 2, 2, 2, 6}, new int[]{1, 5,
            5, 5, 5, 5, 7}, new String[]{"word", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM",
            "word"}, new int[]{1, 1, 0, 1, 1,
            1, 1}, new int[]{1, 1, 2, 3, 1, 1, 1});
        a.close();
    }

    public void testDoNotIgnoreCase() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x y", false);
        add(b, "a b", "m n o", false);

        Analyzer a = getAnalyzer(b, false);
        assertAnalyzesTo(a, "c A B D", new String[]{"c", "A", "B", "D"}, new int[]{0, 2, 4, 6}, new int[]{1, 3, 5, 7}, new
            String[]{"word", "word", "word", "word"}, new int[]{1, 1, 1, 1}, new int[]{1, 1, 1, 1});
        a.close();
    }

    public void testBufferedFinish1() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b c", "m n o", false);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a b", new String[]{"c", "a", "b"}, new int[]{0, 2, 4}, new int[]{1, 3, 5}, new String[]{"word", "word",
            "word"}, new int[]{1, 1, 1}, new int[]{1, 1, 1});
        a.close();
    }

    public void testBufferedFinish2() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "m n o", false);
        add(b, "d e", "m n o", false);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "c a d", new String[]{"c", "a", "d"}, new int[]{0, 2, 4}, new int[]{1, 3, 5}, new String[]{"word", "word",
            "word"}, new int[]{1, 1, 1}, new int[]{1, 1, 1});
        a.close();
    }

    public void testCanReuse() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b", "x", true);
        Analyzer a = getAnalyzer(b, true);
        for (int i = 0; i < 10; i++) {
            assertAnalyzesTo(a, "c a b", new String[]{"c", "x", "a", "b"}, new int[]{0, 2, 2, 4}, new int[]{1, 5, 3, 5}, new
                String[]{"word", "SYNONYM", "word", "word"}, new int[]{1, 1, 0, 1}, new int[]{1, 2, 1, 1});
        }
        a.close();
    }

    /**
     * Multiple input tokens map to a single output token
     */
    public void testManyToOne() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b c", "z", true);

        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "a b c d", new String[]{"z", "a", "b", "c", "d"}, new int[]{0, 0, 2, 4, 6}, new int[]{5, 1, 3, 5, 7}, new
            String[]{"SYNONYM", "word", "word", "word", "word"}, new int[]{1, 0, 1, 1, 1}, new int[]{3, 1, 1, 1, 1});
        a.close();
    }

    public void testBufferAfterMatch() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "a b c d", "x", true);
        add(b, "a b", "y", false);

        // The 'c' token has to be buffered because SynGraphFilter
        // needs to know whether a b c d -> x matches:
        Analyzer a = getAnalyzer(b, true);
        assertAnalyzesTo(a, "f a b c e", new String[]{"f", "y", "c", "e"}, new int[]{0, 2, 6, 8}, new int[]{1, 5, 7, 9}, new
            String[]{"word", "SYNONYM", "word", "word"}, new int[]{1, 1, 1, 1}, new int[]{1, 1, 1, 1});
        a.close();
    }

    public void testZeroSyns() throws Exception {
        Tokenizer tokenizer = new MockTokenizer();
        tokenizer.setReader(new StringReader("aa bb"));
        try {
            new SynonymGraphFilter(tokenizer, new SynonymMap.Builder(true).build(), true);
            fail("did not hit expected exception");
        } catch (IllegalArgumentException iae) {
            // expected
            assertEquals("fst must be non-null", iae.getMessage());
        }
    }

    // Needs TermAutomatonQuery, which is in sandbox still:
    public void testAccurateGraphQuery1() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(newTextField("field", "wtf happened", Field.Store.NO));
        w.addDocument(doc);
        IndexReader r = w.getReader();
        w.close();

        IndexSearcher s = newSearcher(r);

        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "what the fudge", "wtf", true);

        SynonymMap map = b.build();

        TokenStreamToTermAutomatonQuery ts2q = new TokenStreamToTermAutomatonQuery();


        TokenStream in = new CannedTokenStream(0, 23, token("what", 1, 1, 0, 4), token("the", 1, 1, 5, 8), token("fudge", 1, 1, 9, 14),
            token("happened", 1, 1, 15, 23));

        assertEquals(1, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

        in = new CannedTokenStream(0, 12, token("wtf", 1, 1, 0, 3), token("happened", 1, 1, 4, 12));

        assertEquals(1, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

        // "what happened" should NOT match:
        in = new CannedTokenStream(0, 13, token("what", 1, 1, 0, 4), token("happened", 1, 1, 5, 13));
        assertEquals(0, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

        IOUtils.close(r, dir);
    }


    /**
     * If we expand synonyms at search time, the results are correct.
     */
    // Needs TermAutomatonQuery, which is in sandbox still:
    public void testAccurateGraphQuery2() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(newTextField("field", "say wtf happened", Field.Store.NO));
        w.addDocument(doc);
        IndexReader r = w.getReader();
        w.close();

        IndexSearcher s = newSearcher(r);

        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "fudge", "chocolate", true);
        add(b, "what the fudge", "wtf", true);
        add(b, "what the", "wut", true);
        add(b, "say", "say what", true);

        SynonymMap map = b.build();

        TokenStream in = new CannedTokenStream(0, 26, token("say", 1, 1, 0, 3), token("what", 1, 1, 3, 7), token("the", 1, 1, 8, 11),
            token("fudge", 1, 1, 12, 17), token("happened", 1, 1, 18, 26));

        TokenStreamToTermAutomatonQuery ts2q = new TokenStreamToTermAutomatonQuery();

        assertEquals(1, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

        // "what happened" should NOT match:
        in = new CannedTokenStream(0, 13, token("what", 1, 1, 0, 4), token("happened", 1, 1, 5, 13));
        assertEquals(0, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

        IOUtils.close(r, dir);
    }


    // Needs TermAutomatonQuery, which is in sandbox still:
    public void testAccurateGraphQuery3() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(newTextField("field", "say what the fudge happened", Field.Store.NO));
        w.addDocument(doc);
        IndexReader r = w.getReader();
        w.close();

        IndexSearcher s = newSearcher(r);

        SynonymMap.Builder b = new SynonymMap.Builder(true);
        add(b, "wtf", "what the fudge", true);

        SynonymMap map = b.build();

        TokenStream in = new CannedTokenStream(0, 15, token("say", 1, 1, 0, 3), token("wtf", 1, 1, 3, 6), token("happened", 1, 1, 7, 15));

        TokenStreamToTermAutomatonQuery ts2q = new TokenStreamToTermAutomatonQuery();

        assertEquals(1, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

        // "what happened" should NOT match:
        in = new CannedTokenStream(0, 13, token("what", 1, 1, 0, 4), token("happened", 1, 1, 5, 13));
        assertEquals(0, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

        IOUtils.close(r, dir);
    }

    private static Token token(String term, int posInc, int posLength, int startOffset, int endOffset) {
        final Token t = new Token(term, startOffset, endOffset);
        t.setPositionIncrement(posInc);
        t.setPositionLength(posLength);
        return t;
    }

    private String randomNonEmptyString() {
        while (true) {
            String s = TestUtil.randomUnicodeString(random()).trim();
            //String s = TestUtil.randomSimpleString(random()).trim();
            if (s.length() != 0 && s.indexOf('\u0000') == -1) {
                return s;
            }
        }
    }

    // Adds MockGraphTokenFilter after SynFilter:
    public void testRandomGraphAfter() throws Exception {
        final int numIters = atLeast(3);
        for (int i = 0; i < numIters; i++) {
            SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
            final int numEntries = atLeast(10);
            for (int j = 0; j < numEntries; j++) {
                add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
            }
            final SynonymMap map = b.build();
            final boolean ignoreCase = random().nextBoolean();

            final Analyzer analyzer = new Analyzer() {
                @Override
                protected TokenStreamComponents createComponents(String fieldName) {
                    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
                    TokenStream syns = new SynonymGraphFilter(tokenizer, map, ignoreCase);
                    TokenStream graph = new MockGraphTokenFilter(random(), syns);
                    return new TokenStreamComponents(tokenizer, graph);
                }
            };

            checkRandomData(random(), analyzer, 100);
            analyzer.close();
        }
    }

    public void testEmptyStringInput() throws IOException {
        final int numIters = atLeast(10);
        for (int i = 0; i < numIters; i++) {
            SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
            final int numEntries = atLeast(10);
            for (int j = 0; j < numEntries; j++) {
                add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
            }
            final boolean ignoreCase = random().nextBoolean();

            Analyzer analyzer = getAnalyzer(b, ignoreCase);

            checkAnalysisConsistency(random(), analyzer, random().nextBoolean(), "");
            analyzer.close();
        }
    }

    /**
     * simple random test, doesn't verify correctness.
     * does verify it doesnt throw exceptions, or that the stream doesn't misbehave
     */
    public void testRandom2() throws Exception {
        final int numIters = atLeast(3);
        for (int i = 0; i < numIters; i++) {
            SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
            final int numEntries = atLeast(10);
            for (int j = 0; j < numEntries; j++) {
                add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
            }
            final boolean ignoreCase = random().nextBoolean();

            Analyzer analyzer = getAnalyzer(b, ignoreCase);
            checkRandomData(random(), analyzer, 100);
            analyzer.close();
        }
    }

    /**
     * simple random test like testRandom2, but for larger docs
     */
    public void testRandomHuge() throws Exception {
        final int numIters = atLeast(3);
        for (int i = 0; i < numIters; i++) {
            SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
            final int numEntries = atLeast(10);
            //if (VERBOSE) {
            //System.out.println("TEST: iter=" + i + " numEntries=" + numEntries);
            //}
            for (int j = 0; j < numEntries; j++) {
                add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
            }
            final boolean ignoreCase = random().nextBoolean();

            Analyzer analyzer = getAnalyzer(b, ignoreCase);
            checkRandomData(random(), analyzer, 100, 1024);
            analyzer.close();
        }
    }

    public void testEmptyTerm() throws IOException {
        final int numIters = atLeast(10);
        for (int i = 0; i < numIters; i++) {
            SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
            final int numEntries = atLeast(10);
            for (int j = 0; j < numEntries; j++) {
                add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
            }
            final boolean ignoreCase = random().nextBoolean();

            final Analyzer analyzer = getAnalyzer(b, ignoreCase);

            checkAnalysisConsistency(random(), analyzer, random().nextBoolean(), "");
            analyzer.close();
        }
    }

    public void testBuilderDedup() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        final boolean keepOrig = false;
        add(b, "a b", "ab", keepOrig);
        add(b, "a b", "ab", keepOrig);
        add(b, "a b", "ab", keepOrig);
        Analyzer a = getAnalyzer(b, true);

        assertAnalyzesTo(a, "a b", new String[]{"ab"}, new int[]{1});
        a.close();
    }

    public void testBuilderNoDedup() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(false);
        final boolean keepOrig = false;
        add(b, "a b", "ab", keepOrig);
        add(b, "a b", "ab", keepOrig);
        add(b, "a b", "ab", keepOrig);
        Analyzer a = getAnalyzer(b, true);

        assertAnalyzesTo(a, "a b", new String[]{"ab", "ab", "ab"}, new int[]{1, 0, 0});
        a.close();
    }

    public void testRecursion1() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        final boolean keepOrig = false;
        add(b, "zoo", "zoo", keepOrig);
        Analyzer a = getAnalyzer(b, true);

        assertAnalyzesTo(a, "zoo zoo $ zoo", new String[]{"zoo", "zoo", "$", "zoo"}, new int[]{1, 1, 1, 1});
        a.close();
    }

    public void testRecursion2() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        final boolean keepOrig = false;
        add(b, "zoo", "zoo", keepOrig);
        add(b, "zoo", "zoo zoo", keepOrig);
        Analyzer a = getAnalyzer(b, true);

        // verify("zoo zoo $ zoo", "zoo/zoo zoo/zoo/zoo $/zoo zoo/zoo zoo");
        assertAnalyzesTo(a, "zoo zoo $ zoo", new String[]{"zoo", "zoo", "zoo", "zoo", "zoo", "zoo", "$", "zoo", "zoo", "zoo"}, new
            int[]{1, 0, 1, 1, 0, 1, 1, 1, 0, 1});
        a.close();
    }

    public void testKeepOrig() throws Exception {
        SynonymMap.Builder b = new SynonymMap.Builder(true);
        final boolean keepOrig = true;
        add(b, "a b", "ab", keepOrig);
        add(b, "a c", "ac", keepOrig);
        add(b, "a", "aa", keepOrig);
        add(b, "b", "bb", keepOrig);
        add(b, "z x c v", "zxcv", keepOrig);
        add(b, "x c", "xc", keepOrig);
        Analyzer a = getAnalyzer(b, true);

        assertAnalyzesTo(a, "$", new String[]{"$"}, new int[]{1});
        assertAnalyzesTo(a, "a", new String[]{"aa", "a"}, new int[]{1, 0});
        assertAnalyzesTo(a, "a", new String[]{"aa", "a"}, new int[]{1, 0});
        assertAnalyzesTo(a, "$ a", new String[]{"$", "aa", "a"}, new int[]{1, 1, 0});
        assertAnalyzesTo(a, "a $", new String[]{"aa", "a", "$"}, new int[]{1, 0, 1});
        assertAnalyzesTo(a, "$ a !", new String[]{"$", "aa", "a", "!"}, new int[]{1, 1, 0, 1});
        assertAnalyzesTo(a, "a a", new String[]{"aa", "a", "aa", "a"}, new int[]{1, 0, 1, 0});
        assertAnalyzesTo(a, "b", new String[]{"bb", "b"}, new int[]{1, 0});
        assertAnalyzesTo(a, "z x c v", new String[]{"zxcv", "z", "x", "c", "v"}, new int[]{1, 0, 1, 1, 1});
        assertAnalyzesTo(a, "z x c $", new String[]{"z", "xc", "x", "c", "$"}, new int[]{1, 1, 0, 1, 1});
        a.close();
    }

    private Analyzer getAnalyzer(SynonymMap.Builder b, final boolean ignoreCase) throws IOException {
        final SynonymMap map = b.build();
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                // Make a local variable so testRandomHuge doesn't share it across threads!
                SynonymGraphFilter synFilter = new SynonymGraphFilter(tokenizer, map, ignoreCase);
                SynonymGraphFilterTests.this.synFilter = synFilter;
                return new TokenStreamComponents(tokenizer, synFilter);
            }
        };
    }

    private void add(SynonymMap.Builder b, String input, String output, boolean keepOrig) {
        if (VERBOSE) {
            //System.out.println("  add input=" + input + " output=" + output + " keepOrig=" + keepOrig);
        }
        CharsRefBuilder inputCharsRef = new CharsRefBuilder();
        SynonymMap.Builder.join(input.split(" +"), inputCharsRef);

        CharsRefBuilder outputCharsRef = new CharsRefBuilder();
        SynonymMap.Builder.join(output.split(" +"), outputCharsRef);

        b.add(inputCharsRef.get(), outputCharsRef.get(), keepOrig);
    }

    private char[] randomBinaryChars(int minLen, int maxLen, double bias, char base) {
        int len = TestUtil.nextInt(random(), minLen, maxLen);
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            char ch;
            if (random().nextDouble() < bias) {
                ch = base;
            } else {
                ch = (char) (base + 1);
            }
            chars[i] = ch;
        }

        return chars;
    }

    private static String toTokenString(char[] chars) {
        StringBuilder b = new StringBuilder();
        for (char c : chars) {
            if (b.length() > 0) {
                b.append(' ');
            }
            b.append(c);
        }
        return b.toString();
    }

    private static class OneSyn {
        char[] in;
        char[] out;
        boolean keepOrig;

        @Override
        public String toString() {
            return toTokenString(in) + " --> " + toTokenString(out) + " (keepOrig=" + keepOrig + ")";
        }
    }

    public void testRandomSyns() throws Exception {
        int synCount = atLeast(10);
        double bias = random().nextDouble();
        boolean dedup = random().nextBoolean();

        SynonymMap.Builder b = new SynonymMap.Builder(dedup);
        List<OneSyn> syns = new ArrayList<>();
        // Makes random syns from random a / b tokens, mapping to random x / y tokens
        //if (VERBOSE) {
        //    System.out.println("TEST: make " + synCount + " syns");
        //    System.out.println("  bias for a over b=" + bias);
        //    System.out.println("  dedup=" + dedup);
        //    System.out.println("  sausage=" + sausage);
        //}

        int maxSynLength = 0;

        for (int i = 0; i < synCount; i++) {
            OneSyn syn = new OneSyn();
            syn.in = randomBinaryChars(1, 5, bias, 'a');
            syn.out = randomBinaryChars(1, 5, 0.5, 'x');
            syn.keepOrig = random().nextBoolean();
            syns.add(syn);

            maxSynLength = Math.max(maxSynLength, syn.in.length);

            //if (VERBOSE) {
            //    System.out.println("  " + syn);
            //}
            add(b, toTokenString(syn.in), toTokenString(syn.out), syn.keepOrig);
        }

        // Only used w/ VERBOSE:
        Analyzer aNoSausageed;
        if (VERBOSE) {
            aNoSausageed = getAnalyzer(b, true);
        } else {
            aNoSausageed = null;
        }

        Analyzer a = getAnalyzer(b, true);
        int iters = atLeast(20);
        for (int iter = 0; iter < iters; iter++) {

            String doc = toTokenString(randomBinaryChars(50, 100, bias, 'a'));
            //String doc = toTokenString(randomBinaryChars(10, 50, bias, 'a'));

            //if (VERBOSE) {
            //    System.out.println("TEST: iter=" + iter + " doc=" + doc);
            //}
            Automaton expected = slowSynFilter(doc, syns);
            if (VERBOSE) {
                //System.out.println("  expected:\n" + expected.toDot());
            }
            Automaton actual = toAutomaton(a.tokenStream("field", new StringReader(doc)));
            //if (VERBOSE) {
            //    System.out.println("  actual:\n" + actual.toDot());
            //}

            assertTrue("maxLookaheadUsed=" + synFilter.getMaxLookaheadUsed() + " maxSynLength=" + maxSynLength, synFilter
                .getMaxLookaheadUsed() <= maxSynLength);

            checkAnalysisConsistency(random(), a, random().nextBoolean(), doc);
            // We can easily have a non-deterministic automaton at this point, e.g. if
            // more than one syn matched at given point, or if the syn mapped to an
            // output token that also happens to be in the input:
            try {
                actual = Operations.determinize(actual, 50000);
            } catch (TooComplexToDeterminizeException tctde) {
                // Unfortunately the syns can easily create difficult-to-determinize graphs:
                assertTrue(approxEquals(actual, expected));
                continue;
            }

            try {
                expected = Operations.determinize(expected, 50000);
            } catch (TooComplexToDeterminizeException tctde) {
                // Unfortunately the syns can easily create difficult-to-determinize graphs:
                assertTrue(approxEquals(actual, expected));
                continue;
            }

            assertTrue(approxEquals(actual, expected));
            assertTrue(Operations.sameLanguage(actual, expected));
        }

        a.close();
    }

    /**
     * Only used when true equality is too costly to check!
     */
    private boolean approxEquals(Automaton actual, Automaton expected) {
        // Don't collapse these into one line else the thread stack won't say which direction failed!:
        boolean b1 = approxSubsetOf(actual, expected);
        boolean b2 = approxSubsetOf(expected, actual);
        return b1 && b2;
    }

    private boolean approxSubsetOf(Automaton a1, Automaton a2) {
        AutomatonTestUtil.RandomAcceptedStrings ras = new AutomatonTestUtil.RandomAcceptedStrings(a1);
        for (int i = 0; i < 2000; i++) {
            int[] ints = ras.getRandomAcceptedString(random());
            IntsRef path = new IntsRef(ints, 0, ints.length);
            if (accepts(a2, path) == false) {
                throw new RuntimeException("a2 does not accept " + path);
            }
        }

        // Presumed true
        return true;
    }

    /**
     * Like {@link Operations#run} except the incoming automaton is allowed to be non-deterministic.
     */
    private static boolean accepts(Automaton a, IntsRef path) {
        Set<Integer> states = new HashSet<>();
        states.add(0);
        Transition t = new Transition();
        for (int i = 0; i < path.length; i++) {
            int digit = path.ints[path.offset + i];
            Set<Integer> nextStates = new HashSet<>();
            for (int state : states) {
                int count = a.initTransition(state, t);
                for (int j = 0; j < count; j++) {
                    a.getNextTransition(t);
                    if (digit >= t.min && digit <= t.max) {
                        nextStates.add(t.dest);
                    }
                }
            }
            states = nextStates;
            if (states.isEmpty()) {
                return false;
            }
        }

        for (int state : states) {
            if (a.isAccept(state)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Stupid, slow brute-force, yet hopefully bug-free, synonym filter.
     */
    private Automaton slowSynFilter(String doc, List<OneSyn> syns) {
        String[] tokens = doc.split(" +");
        //if (VERBOSE) {
        //    System.out.println("  doc has " + tokens.length + " tokens");
        //}
        int i = 0;
        Automaton.Builder a = new Automaton.Builder();
        int lastState = a.createState();
        while (i < tokens.length) {
            // Consider all possible syn matches starting at this point:
            assert tokens[i].length() == 1;
            //if (VERBOSE) {
            //    System.out.println("    i=" + i);
            //}

            List<OneSyn> matches = new ArrayList<>();
            for (OneSyn syn : syns) {
                if (i + syn.in.length <= tokens.length) {
                    boolean match = true;
                    for (int j = 0; j < syn.in.length; j++) {
                        if (tokens[i + j].charAt(0) != syn.in[j]) {
                            match = false;
                            break;
                        }
                    }

                    if (match) {
                        if (matches.isEmpty() == false) {
                            if (syn.in.length < matches.get(0).in.length) {
                                // Greedy matching: we already found longer syns matching here
                                continue;
                            } else if (syn.in.length > matches.get(0).in.length) {
                                // Greedy matching: all previous matches were shorter, so we drop them
                                matches.clear();
                            } else {
                                // Keep the current matches: we allow multiple synonyms matching the same input string
                            }
                        }

                        matches.add(syn);
                    }
                }
            }

            int nextState = a.createState();

            if (matches.isEmpty() == false) {
                // We have match(es) starting at this token
                //if (VERBOSE) {
                //    System.out.println("  matches @ i=" + i + ": " + matches);
                //}
                // We keepOrig if any of the matches said to:
                boolean keepOrig = false;
                for (OneSyn syn : matches) {
                    keepOrig |= syn.keepOrig;
                }

                if (keepOrig) {
                    // Add path for the original tokens
                    addSidePath(a, lastState, nextState, matches.get(0).in);
                }

                for (OneSyn syn : matches) {
                    addSidePath(a, lastState, nextState, syn.out);
                }

                i += matches.get(0).in.length;
            } else {
                a.addTransition(lastState, nextState, tokens[i].charAt(0));
                i++;
            }

            lastState = nextState;
        }

        a.setAccept(lastState, true);

        return topoSort(a.finish());
    }

    /**
     * Just creates a side path from startState to endState with the provided tokens.
     */
    private static void addSidePath(Automaton.Builder a, int startState, int endState, char[] tokens) {
        int lastState = startState;
        for (int i = 0; i < tokens.length; i++) {
            int nextState;
            if (i == tokens.length - 1) {
                nextState = endState;
            } else {
                nextState = a.createState();
            }

            a.addTransition(lastState, nextState, tokens[i]);

            lastState = nextState;
        }
    }

    private Automaton toAutomaton(TokenStream ts) throws IOException {
        PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
        PositionLengthAttribute posLenAtt = ts.addAttribute(PositionLengthAttribute.class);
        CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        Automaton a = new Automaton();
        int srcNode = -1;
        int destNode = -1;
        int state = a.createState();
        while (ts.incrementToken()) {
            assert termAtt.length() == 1;
            char c = termAtt.charAt(0);
            int posInc = posIncAtt.getPositionIncrement();
            if (posInc != 0) {
                srcNode += posInc;
                while (state < srcNode) {
                    state = a.createState();
                }
            }
            destNode = srcNode + posLenAtt.getPositionLength();
            while (state < destNode) {
                state = a.createState();
            }
            a.addTransition(srcNode, destNode, c);
        }
        ts.end();
        ts.close();
        a.finishState();
        a.setAccept(destNode, true);
        return a;
    }

    /**
     * Renumbers nodes according to their topo sort
     */
    private Automaton topoSort(Automaton in) {
        int[] newToOld = Operations.topoSortStates(in);
        int[] oldToNew = new int[newToOld.length];

        Automaton.Builder a = new Automaton.Builder();
        //System.out.println("remap:");
        for (int i = 0; i < newToOld.length; i++) {
            a.createState();
            oldToNew[newToOld[i]] = i;
            //System.out.println("  " + newToOld[i] + " -> " + i);
            if (in.isAccept(newToOld[i])) {
                a.setAccept(i, true);
                //System.out.println("    **");
            }
        }

        Transition t = new Transition();
        for (int i = 0; i < newToOld.length; i++) {
            int count = in.initTransition(newToOld[i], t);
            for (int j = 0; j < count; j++) {
                in.getNextTransition(t);
                a.addTransition(i, oldToNew[t.dest], t.min, t.max);
            }
        }

        return a.finish();
    }

    /**
     * Helper method to validate all strings that can be generated from a token stream. Uses {@link
     * TokenStreamToAutomaton} to create an automaton. Asserts the finite strings of the automaton
     * are all and only the given valid strings.
     *
     * @param analyzer        analyzer containing the SynonymFilter under test.
     * @param text            text to be analyzed.
     * @param expectedStrings all expected finite strings.
     */
    public void assertAllStrings(Analyzer analyzer, String text, String[] expectedStrings) throws IOException {
        TokenStream tokenStream = analyzer.tokenStream("dummy", text);
        try {
            Automaton automaton = new TokenStreamToAutomaton().toAutomaton(tokenStream);
            Set<IntsRef> finiteStrings = AutomatonTestUtil.getFiniteStringsRecursive(automaton, -1);

            assertEquals("Invalid resulting strings count. Expected " + expectedStrings.length + " was " + finiteStrings.size(),
                expectedStrings.length, finiteStrings.size());

            Set<String> expectedStringsSet = new HashSet<>(Arrays.asList(expectedStrings));

            BytesRefBuilder scratchBytesRefBuilder = new BytesRefBuilder();
            for (IntsRef ir : finiteStrings) {
                String s = Util.toBytesRef(ir, scratchBytesRefBuilder).utf8ToString().replace((char) TokenStreamToAutomaton.POS_SEP, ' ');
                assertTrue("Unexpected string found: " + s, expectedStringsSet.contains(s));
            }
        } finally {
            tokenStream.close();
        }
    }
}
