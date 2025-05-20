/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class UniqueTokenFilterTests extends ESTestCase {
    public void testSimple() throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(t, new UniqueTokenFilter(t));
            }
        };

        TokenStream test = analyzer.tokenStream("test", "this test with test");
        test.reset();
        CharTermAttribute termAttribute = test.addAttribute(CharTermAttribute.class);
        PositionIncrementAttribute positionIncrement = test.addAttribute(PositionIncrementAttribute.class);
        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("this"));
        assertEquals(1, positionIncrement.getPositionIncrement());

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("test"));
        assertEquals(1, positionIncrement.getPositionIncrement());

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("with"));
        assertEquals(1, positionIncrement.getPositionIncrement());

        assertThat(test.incrementToken(), equalTo(false));
    }

    public void testOnlyOnSamePosition() throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(t, new UniqueTokenFilter(t, true));
            }
        };

        TokenStream test = analyzer.tokenStream("test", "this test with test");
        test.reset();
        CharTermAttribute termAttribute = test.addAttribute(CharTermAttribute.class);
        PositionIncrementAttribute positionIncrement = test.addAttribute(PositionIncrementAttribute.class);
        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("this"));
        assertEquals(1, positionIncrement.getPositionIncrement());

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("test"));
        assertEquals(1, positionIncrement.getPositionIncrement());

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("with"));
        assertEquals(1, positionIncrement.getPositionIncrement());

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("test"));
        assertEquals(1, positionIncrement.getPositionIncrement());

        assertThat(test.incrementToken(), equalTo(false));
    }

    public void testPositionIncrement() throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                TokenFilter filters = new EdgeNGramTokenFilter(tokenizer, 1, 3, false);
                filters = new UniqueTokenFilter(filters);
                return new TokenStreamComponents(tokenizer, filters);
            }
        };

        assertAnalyzesTo(
            analyzer,
            "foo bar bro bar bro baz",
            new String[] { "f", "fo", "foo", "b", "ba", "bar", "br", "bro", "baz" },
            new int[] { 0, 0, 0, 4, 4, 4, 8, 8, 20 },
            new int[] { 3, 3, 3, 7, 7, 7, 11, 11, 23 },
            new int[] { 1, 0, 0, 1, 0, 0, 1, 0, 3 }
        );
        analyzer.close();
    }

    /**
     * For bwc reasons we need to return the legacy filter for indices create before 7.7
     */
    public void testOldVersionGetXUniqueTokenFilter() throws IOException {

        Settings settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.UNIQUE_TOKEN_FILTER_POS_FIX)
            )
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try (CommonAnalysisPlugin plugin = new CommonAnalysisPlugin()) {

            TokenFilterFactory tff = plugin.getTokenFilters().get("unique").get(idxSettings, null, "unique", settings);
            TokenStream ts = tff.create(new TokenStream() {

                @Override
                public boolean incrementToken() throws IOException {
                    return false;
                }
            });
            assertThat(ts, instanceOf(XUniqueTokenFilter.class));
        }
    }

    public void testNewVersionGetUniqueTokenFilter() throws IOException {

        Settings settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.UNIQUE_TOKEN_FILTER_POS_FIX, IndexVersion.current())
            )
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try (CommonAnalysisPlugin plugin = new CommonAnalysisPlugin()) {

            TokenFilterFactory tff = plugin.getTokenFilters().get("unique").get(idxSettings, null, "unique", settings);
            TokenStream ts = tff.create(new TokenStream() {

                @Override
                public boolean incrementToken() throws IOException {
                    return false;
                }
            });
            assertThat(ts, instanceOf(UniqueTokenFilter.class));
        }
    }
}
