/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.instanceOf;

public class NGramTokenizerFactoryTests extends ESTokenStreamTestCase {
    public void testParseTokenChars() {
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        final IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);
        for (String tokenChars : Arrays.asList("letter", " digit ", "punctuation", "DIGIT", "CoNtRoL", "dash_punctuation")) {
            final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2)
                .put("max_gram", 3)
                .put("token_chars", tokenChars)
                .build();
            new NGramTokenizerFactory(indexProperties, null, name, settings).create();
            // no exception
        }
        {
            final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2)
                .put("max_gram", 3)
                .put("token_chars", "DIRECTIONALITY_UNDEFINED")
                .build();
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> new NGramTokenizerFactory(indexProperties, null, name, settings).create()
            );
            assertEquals("Unknown token type: 'directionality_undefined'", ex.getMessage().substring(0, 46));
            assertTrue(ex.getMessage().contains("custom"));
        }
        {
            final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2)
                .put("max_gram", 3)
                .put("token_chars", "custom")
                .put("custom_token_chars", "_-")
                .build();
            new NGramTokenizerFactory(indexProperties, null, name, settings).create();
            // no exception
        }
        {
            final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2)
                .put("max_gram", 3)
                .put("token_chars", "custom")
                .build();
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> new NGramTokenizerFactory(indexProperties, null, name, settings).create()
            );
            assertEquals("Token type: 'custom' requires setting `custom_token_chars`", ex.getMessage());
        }
    }

    public void testNoTokenChars() throws IOException {
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 2).build();

        final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2)
            .put("max_gram", 4)
            .putList("token_chars", new String[0])
            .build();
        Tokenizer tokenizer = new NGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings)
            .create();
        tokenizer.setReader(new StringReader("1.34"));
        assertTokenStreamContents(tokenizer, new String[] { "1.", "1.3", "1.34", ".3", ".34", "34" });
    }

    public void testCustomTokenChars() throws IOException {
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 2).build();

        final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2)
            .put("max_gram", 3)
            .putList("token_chars", "letter", "custom")
            .put("custom_token_chars", "_-")
            .build();
        Tokenizer tokenizer = new NGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings)
            .create();
        tokenizer.setReader(new StringReader("Abc -gh _jk =lm"));
        assertTokenStreamContents(tokenizer, new String[] { "Ab", "Abc", "bc", "-g", "-gh", "gh", "_j", "_jk", "jk", "lm" });
    }

    public void testPreTokenization() throws IOException {
        // Make sure that pretokenization works well and that it can be used even with token chars which are supplementary characters
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit").build();
        Tokenizer tokenizer = new NGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings)
            .create();
        tokenizer.setReader(new StringReader("Åbc déf g\uD801\uDC00f "));
        assertTokenStreamContents(
            tokenizer,
            new String[] { "Åb", "Åbc", "bc", "dé", "déf", "éf", "g\uD801\uDC00", "g\uD801\uDC00f", "\uD801\uDC00f" }
        );
        settings = newAnalysisSettingsBuilder().put("min_gram", 2)
            .put("max_gram", 3)
            .put("token_chars", "letter,digit,punctuation,whitespace,symbol")
            .build();
        tokenizer = new NGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings).create();
        tokenizer.setReader(new StringReader(" a!$ 9"));
        assertTokenStreamContents(tokenizer, new String[] { " a", " a!", "a!", "a!$", "!$", "!$ ", "$ ", "$ 9", " 9" });
    }

    public void testPreTokenizationEdge() throws IOException {
        // Make sure that pretokenization works well and that it can be used even with token chars which are supplementary characters
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3).put("token_chars", "letter,digit").build();
        Tokenizer tokenizer = new EdgeNGramTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            name,
            settings
        ).create();
        tokenizer.setReader(new StringReader("Åbc déf g\uD801\uDC00f "));
        assertTokenStreamContents(tokenizer, new String[] { "Åb", "Åbc", "dé", "déf", "g\uD801\uDC00", "g\uD801\uDC00f" });
        settings = newAnalysisSettingsBuilder().put("min_gram", 2)
            .put("max_gram", 3)
            .put("token_chars", "letter,digit,punctuation,whitespace,symbol")
            .build();
        tokenizer = new EdgeNGramTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings), null, name, settings)
            .create();
        tokenizer.setReader(new StringReader(" a!$ 9"));
        assertTokenStreamContents(tokenizer, new String[] { " a", " a!" });
    }

    public void testBackwardsCompatibilityEdgeNgramTokenFilter() throws Exception {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            final Index index = new Index("test", "_na_");
            final String name = "ngr";
            IndexVersion v = IndexVersionUtils.randomVersion(random());
            Builder builder = newAnalysisSettingsBuilder().put("min_gram", 2).put("max_gram", 3);
            boolean reverse = random().nextBoolean();
            if (reverse) {
                builder.put("side", "back");
            }
            Settings settings = builder.build();
            Settings indexSettings = newAnalysisSettingsBuilder().put(IndexMetadata.SETTING_VERSION_CREATED, v).build();
            Tokenizer tokenizer = new MockTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream edgeNGramTokenFilter = new EdgeNGramTokenFilterFactory(
                IndexSettingsModule.newIndexSettings(index, indexSettings),
                null,
                name,
                settings
            ).create(tokenizer);
            if (reverse) {
                assertThat(edgeNGramTokenFilter, instanceOf(ReverseStringFilter.class));
            } else {
                assertThat(edgeNGramTokenFilter, instanceOf(EdgeNGramTokenFilter.class));
            }
        }
        assertWarnings(
            "The [side] parameter is deprecated and will be removed. Use a [reverse] before and after the [edge_ngram] instead."
        );
    }

    /*`
    * test that throws an error when trying to get a NGramTokenizer where difference between max_gram and min_gram
    * is greater than the allowed value of max_ngram_diff
     */
    public void testMaxNGramDiffException() throws Exception {
        final Index index = new Index("test", "_na_");
        final String name = "ngr";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);

        int maxAllowedNgramDiff = indexProperties.getMaxNgramDiff();
        int ngramDiff = maxAllowedNgramDiff + 1;
        int min_gram = 2;
        int max_gram = min_gram + ngramDiff;

        final Settings settings = newAnalysisSettingsBuilder().put("min_gram", min_gram).put("max_gram", max_gram).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new NGramTokenizerFactory(indexProperties, null, name, settings).create()
        );
        assertEquals(
            "The difference between max_gram and min_gram in NGram Tokenizer must be less than or equal to: ["
                + maxAllowedNgramDiff
                + "] but was ["
                + ngramDiff
                + "]. This limit can be set by changing the ["
                + IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey()
                + "] index level setting.",
            ex.getMessage()
        );
    }
}
