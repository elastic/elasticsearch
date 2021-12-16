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
import org.apache.lucene.analysis.no.NorwegianNormalizationFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.analysis.*;

import java.util.List;

public class DemoNormalizerIteratorFactory extends AbstractAnalysisIteratorFactory {

    private final Analyzer analyzer;

    public DemoNormalizerIteratorFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new KeywordTokenizer();
                TokenStream tokenStream = new NorwegianNormalizationFilter(tokenizer);

                return new TokenStreamComponents(tokenizer, tokenStream);
            }
        };
    }

    @Override
    public PortableAnalyzeIterator newInstance(List<AnalyzeToken> tokens, AnalyzeState prevState) {
        return new StableLuceneNormalizerIterator(
            analyzer,
            tokens,
            prevState,
            new AnalyzeSettings(100, 1));
    }
}
