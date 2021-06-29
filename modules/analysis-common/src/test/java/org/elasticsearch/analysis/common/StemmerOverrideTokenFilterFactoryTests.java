/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Locale;

public class StemmerOverrideTokenFilterFactoryTests extends ESTokenStreamTestCase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public static TokenFilterFactory create(String... rules) throws IOException {
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put("index.analysis.filter.my_stemmer_override.type", "stemmer_override")
                .putList("index.analysis.filter.my_stemmer_override.rules", rules)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build(),
            new CommonAnalysisPlugin());

        return analysis.tokenFilter.get("my_stemmer_override");
    }

    public void testRuleError() {
        for (String rule : List.of(
            "",        // empty
            "a",       // no arrow
            "a=>b=>c", // multiple arrows
            "=>a=>b",  // multiple arrows
            "a=>",     // no override
            "a=>b,c",  // multiple overrides
            "=>a",     // no keys
            "a,=>b"    // empty key
        )) {
            expectThrows(RuntimeException.class, String.format(
                Locale.ROOT, "Should fail for invalid rule: '%s'", rule
            ), () -> create(rule));
        }
    }

    public void testRulesOk() throws IOException {
        TokenFilterFactory tokenFilterFactory = create(
            "a => 1",
            "b,c => 2"
        );
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("a b c"));
        assertTokenStreamContents(tokenFilterFactory.create(tokenizer), new String[]{"1", "2", "2"});
    }
}
