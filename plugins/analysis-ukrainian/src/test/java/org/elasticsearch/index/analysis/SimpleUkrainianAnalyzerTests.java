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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugin.analysis.ukrainian.AnalysisUkrainianPlugin;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SimpleUkrainianAnalyzerTests extends ESTestCase {

    public void testBasicUsage() throws Exception {
        testAnalyzer("чергу", "черга");
        testAnalyzer("рухається", "рухатися");
        testAnalyzer("колу", "кола", "коло", "кіл");
        testAnalyzer("Ця п'єса у свою чергу рухається по колу.", "п'єса", "черга", "рухатися", "кола", "коло", "кіл");
    }

    private static void testAnalyzer(String source, String... expected_terms) throws IOException {
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisUkrainianPlugin());
        Analyzer analyzer = analysis.indexAnalyzers.get("ukrainian").analyzer();
        TokenStream ts = analyzer.tokenStream("test", source);
        CharTermAttribute term1 = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        for (String expected : expected_terms) {
            assertThat(ts.incrementToken(), equalTo(true));
            assertThat(term1.toString(), equalTo(expected));
        }
        assertThat(ts.incrementToken(), equalTo(false));
    }

}
