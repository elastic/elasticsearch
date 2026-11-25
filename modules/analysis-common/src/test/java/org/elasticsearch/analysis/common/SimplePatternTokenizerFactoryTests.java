/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.io.StringReader;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class SimplePatternTokenizerFactoryTests extends ESTokenStreamTestCase {

    public void testNonDeterministicPatterns() throws IOException {
        assertTokens("ab|ac", "ab ac ad", "ab", "ac");
        assertTokens("[0-9]{2}/[0-9]{2}/[0-9]{4}|[0-9]{4}-[0-9]{2}-[0-9]{2}", "on 05/11/2026 or 2026-11-05", "05/11/2026", "2026-11-05");
    }

    public void testLongestMatchWins() throws IOException {
        assertTokens("[a-z]+|[a-z]+\\.[a-z]+", "foo.bar baz", "foo.bar", "baz");
    }

    /**
     * A pattern that is too complex to determinize must only fail when a tokenizer is actually
     * created, so that defining such a tokenizer without using it in an analyzer stays harmless.
     */
    public void testTooComplexPatternFailsOnlyOnCreate() {
        SimplePatternTokenizerFactory factory = newFactory("(a|b)*a(a|b){20}");
        expectThrows(TooComplexToDeterminizeException.class, factory::create);
    }

    private void assertTokens(String pattern, String text, String... expected) throws IOException {
        Tokenizer tokenizer = newFactory(pattern).create();
        tokenizer.setReader(new StringReader(text));
        assertTokenStreamContents(tokenizer, expected);
    }

    private SimplePatternTokenizerFactory newFactory(String pattern) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(new Index("test", "_na_"), newAnalysisSettingsBuilder().build());
        Settings settings = Settings.builder().put("pattern", pattern).build();
        return new SimplePatternTokenizerFactory(indexSettings, null, "simple_pattern", settings);
    }
}
