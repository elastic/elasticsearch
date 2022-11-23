/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugin.analysis.stempel.AnalysisStempelPlugin;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;

public class SimplePolishTokenFilterTests extends ESTestCase {
    public void testBasicUsage() throws Exception {
        testToken("kwiaty", "kwć");
        testToken("canona", "ć");
        testToken("wirtualna", "wirtualny");
        testToken("polska", "polski");

        testAnalyzer("wirtualna polska", "wirtualny", "polski");
    }

    private void testToken(String source, String expected) throws IOException {
        Index index = new Index("test", "_na_");
        Settings settings = Settings.builder().put("index.analysis.filter.myStemmer.type", "polish_stem").build();
        TestAnalysis analysis = createTestAnalysis(index, settings, new AnalysisStempelPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myStemmer");

        Tokenizer tokenizer = new KeywordTokenizer();
        tokenizer.setReader(new StringReader(source));
        TokenStream ts = filterFactory.create(tokenizer);

        CharTermAttribute term1 = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        assertThat(ts.incrementToken(), equalTo(true));

        assertThat(term1.toString(), equalTo(expected));
    }

    private void testAnalyzer(String source, String... expected_terms) throws IOException {
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisStempelPlugin());

        Analyzer analyzer = analysis.indexAnalyzers.get("polish").analyzer();

        TokenStream ts = analyzer.tokenStream("test", source);

        CharTermAttribute term1 = ts.addAttribute(CharTermAttribute.class);
        ts.reset();

        for (String expected : expected_terms) {
            assertThat(ts.incrementToken(), equalTo(true));
            assertThat(term1.toString(), equalTo(expected));
        }
    }

}
