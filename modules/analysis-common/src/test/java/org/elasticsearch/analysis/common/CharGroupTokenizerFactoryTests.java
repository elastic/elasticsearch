/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

public class CharGroupTokenizerFactoryTests extends ESTokenStreamTestCase {

    public void testParseTokenChars() {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);
        final String name = "cg";
        for (String[] conf : Arrays.asList(
                new String[] { "\\v" },
                new String[] { "\\u00245" },
                new String[] { "commas" },
                new String[] { "a", "b", "c", "\\$" })) {
            final Settings settings = newAnalysisSettingsBuilder().putList("tokenize_on_chars", conf).build();
            expectThrows(RuntimeException.class, () -> new CharGroupTokenizerFactory(indexProperties, null, name, settings).create());
        }

        for (String[] conf : Arrays.asList(
                new String[0],
                new String[] { "\\n" },
                new String[] { "\\u0024" },
                new String[] { "whitespace" },
                new String[] { "a", "b", "c" },
                new String[] { "a", "b", "c", "\\r" },
                new String[] { "\\r" },
                new String[] { "f", "o", "o", "symbol" })) {
            final Settings settings = newAnalysisSettingsBuilder().putList("tokenize_on_chars", Arrays.asList(conf)).build();
            new CharGroupTokenizerFactory(indexProperties, null, name, settings).create();
            // no exception
        }
    }

    public void testMaxTokenLength() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);
        final String name = "cg";

        String[] conf = new String[] {"-"};

        final Settings defaultLengthSettings = newAnalysisSettingsBuilder()
            .putList("tokenize_on_chars", conf)
            .build();
        CharTokenizer tokenizer = (CharTokenizer) new CharGroupTokenizerFactory(indexProperties, null, name, defaultLengthSettings)
            .create();
        String textWithVeryLongToken = RandomStrings.randomAsciiAlphanumOfLength(random(), 256).concat("-trailing");
        try (Reader reader = new StringReader(textWithVeryLongToken)) {
            tokenizer.setReader(reader);
            assertTokenStreamContents(tokenizer, new String[] { textWithVeryLongToken.substring(0, 255),
                textWithVeryLongToken.substring(255, 256), "trailing"});
        }

        final Settings analysisSettings = newAnalysisSettingsBuilder()
            .putList("tokenize_on_chars", conf)
            .put("max_token_length", 2)
            .build();
        tokenizer = (CharTokenizer) new CharGroupTokenizerFactory(indexProperties, null, name, analysisSettings).create();
        try (Reader reader = new StringReader("one-two-three")) {
            tokenizer.setReader(reader);
            assertTokenStreamContents(tokenizer, new String[] { "on", "e", "tw", "o", "th", "re", "e" });
        }

        final Settings tooLongLengthSettings = newAnalysisSettingsBuilder()
            .putList("tokenize_on_chars", conf)
            .put("max_token_length", 1024 * 1024 + 1)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new CharGroupTokenizerFactory(indexProperties, null, name, tooLongLengthSettings).create());
        assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 1048577", e.getMessage());

        final Settings negativeLengthSettings = newAnalysisSettingsBuilder()
            .putList("tokenize_on_chars", conf)
            .put("max_token_length", -1)
            .build();
        e = expectThrows(IllegalArgumentException.class,
            () -> new CharGroupTokenizerFactory(indexProperties, null, name, negativeLengthSettings).create());
        assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: -1", e.getMessage());
    }

    public void testTokenization() throws IOException {
        final Index index = new Index("test", "_na_");
        final String name = "cg";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        final Settings settings = newAnalysisSettingsBuilder().putList("tokenize_on_chars", "whitespace", ":", "\\u0024").build();
        Tokenizer tokenizer = new CharGroupTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings),
                null, name, settings).create();
        tokenizer.setReader(new StringReader("foo bar $34 test:test2"));
        assertTokenStreamContents(tokenizer, new String[] {"foo", "bar", "34", "test", "test2"});
    }
}
