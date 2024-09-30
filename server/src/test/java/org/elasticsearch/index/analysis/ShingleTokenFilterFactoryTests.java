/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.instanceOf;

@ThreadLeakScope(Scope.NONE)
public class ShingleTokenFilterFactoryTests extends ESTokenStreamTestCase {
    private static final String RESOURCE = "/org/elasticsearch/index/analysis/shingle_analysis.json";

    public void testDefault() throws IOException {
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("shingle");
        String source = "the quick brown fox";
        String[] expected = new String[] { "the", "the quick", "quick", "quick brown", "brown", "brown fox", "fox" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testInverseMapping() throws IOException {
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("shingle_inverse");
        assertThat(tokenFilter, instanceOf(ShingleTokenFilterFactory.class));
        String source = "the quick brown fox";
        String[] expected = new String[] { "the_quick_brown", "quick_brown_fox" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testInverseMappingNoShingles() throws IOException {
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("shingle_inverse");
        assertThat(tokenFilter, instanceOf(ShingleTokenFilterFactory.class));
        String source = "the quick";
        String[] expected = new String[] { "the", "quick" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testFillerToken() throws IOException {
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("shingle_filler");
        String source = "simon the sorcerer";
        String[] expected = new String[] { "simon FILLER", "simon FILLER sorcerer", "FILLER sorcerer" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        TokenStream stream = new StopFilter(tokenizer, StopFilter.makeStopSet("the"));
        assertTokenStreamContents(tokenFilter.create(stream), expected);
    }

    public void testDisableGraph() throws IOException {
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory shingleFiller = analysis.tokenFilter.get("shingle_filler");
        TokenFilterFactory shingleInverse = analysis.tokenFilter.get("shingle_inverse");

        String source = "hello world";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        try (TokenStream stream = shingleFiller.create(tokenizer)) {
            // This config uses different size of shingles so graph analysis is disabled
            assertTrue(stream.hasAttribute(DisableGraphAttribute.class));
        }

        tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        try (TokenStream stream = shingleInverse.create(tokenizer)) {
            // This config uses a single size of shingles so graph analysis is enabled
            assertFalse(stream.hasAttribute(DisableGraphAttribute.class));
        }
    }

    /*`
    * test that throws an error when trying to get a ShingleTokenFilter where difference between max_shingle_size and min_shingle_size
    * is greater than the allowed value of max_shingle_diff
     */
    public void testMaxShingleDiffException() throws Exception {
        String RESOURCE2 = "/org/elasticsearch/index/analysis/shingle_analysis2.json";
        int maxAllowedShingleDiff = 3;
        int shingleDiff = 8;
        try {
            ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(createTempDir(), RESOURCE2);
            analysis.tokenFilter.get("shingle");
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals(
                "In Shingle TokenFilter the difference between max_shingle_size and min_shingle_size (and +1 if outputting unigrams)"
                    + " must be less than or equal to: ["
                    + maxAllowedShingleDiff
                    + "] but was ["
                    + shingleDiff
                    + "]. This limit"
                    + " can be set by changing the ["
                    + IndexSettings.MAX_SHINGLE_DIFF_SETTING.getKey()
                    + "] index level setting.",
                ex.getMessage()
            );
        }
    }
}
