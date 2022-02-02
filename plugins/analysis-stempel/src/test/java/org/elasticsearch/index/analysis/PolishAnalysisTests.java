/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.pl.PolishStemTokenFilterFactory;
import org.elasticsearch.plugin.analysis.stempel.AnalysisStempelPlugin;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class PolishAnalysisTests extends ESTestCase {
    public void testDefaultsPolishAnalysis() throws IOException {
        final TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisStempelPlugin());
        TokenFilterFactory tokenizerFactory = analysis.tokenFilter.get("polish_stem");
        MatcherAssert.assertThat(tokenizerFactory, instanceOf(PolishStemTokenFilterFactory.class));

        Analyzer analyzer = analysis.indexAnalyzers.get("polish").analyzer();
        MatcherAssert.assertThat(analyzer, instanceOf(PolishAnalyzer.class));
    }
}
