/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.example.analysis.lucene.CharSkippingTokenizer;
import org.elasticsearch.example.analysis.lucene.ReplaceCharToNumber;
import org.elasticsearch.example.analysis.lucene.SkipStartingWithDigitTokenFilter;
import org.elasticsearch.plugin.analysis.AnalyzerFactory;
import org.elasticsearch.plugin.NamedComponent;
import org.elasticsearch.plugin.Inject;

import java.util.List;

@NamedComponent( "example_analyzer_factory")
public class CustomAnalyzerFactory implements AnalyzerFactory {
    private final ExampleAnalysisSettings settings;

    @Inject
    public CustomAnalyzerFactory(ExampleAnalysisSettings settings) {
        this.settings = settings;
    }

    @Override
    public Analyzer create() {
        return new CustomAnalyzer(settings);
    }

    static class CustomAnalyzer extends Analyzer {

        private final ExampleAnalysisSettings settings;

        public CustomAnalyzer(ExampleAnalysisSettings settings) {
            this.settings = settings;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            var tokenizerListOfChars = settings.singleCharsToSkipInTokenizer().isEmpty() ? List.of("_") : settings.singleCharsToSkipInTokenizer();
            var tokenizer = new CharSkippingTokenizer(tokenizerListOfChars);

            long tokenFilterNumber = settings.analyzerUseTokenListOfChars() ? settings.digitToSkipInTokenFilter() : -1;
            var tokenFilter = new SkipStartingWithDigitTokenFilter(tokenizer, tokenFilterNumber);
            return new TokenStreamComponents(
                r -> tokenizer.setReader(new ReplaceCharToNumber(r, settings.oldCharToReplaceInCharFilter(), settings.newNumberToReplaceInCharFilter())),
                tokenFilter
            );
        }
    }
}

