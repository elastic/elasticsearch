/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis.pl;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.analysis.AbstractAnalysisIteratorFactory;
import org.elasticsearch.plugins.analysis.AnalyzeState;
import org.elasticsearch.plugins.analysis.SimpleAnalyzeIterator;
import org.elasticsearch.plugins.analysis.StableLuceneAnalyzeIterator;

public class DemoIteratorFactory extends AbstractAnalysisIteratorFactory {

    private final Analyzer analyzer;

    public DemoIteratorFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                TokenStream tokenStream = new ElasticWordOnlyTokenFilter(tokenizer);

                return new TokenStreamComponents(tokenizer, tokenStream);
            }
        };
    }

    @Override
    public SimpleAnalyzeIterator newInstance(String text, AnalyzeState prevState) {
        return new StableLuceneAnalyzeIterator(analyzer, analyzer.tokenStream(null, text), prevState);
    }

    private class ElasticWordOnlyTokenFilter extends FilteringTokenFilter {
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        public ElasticWordOnlyTokenFilter(TokenStream in) {
            super(in);
        }

        @Override
        protected boolean accept() {
            return termAtt.toString().equalsIgnoreCase("elastic");
        }
    }

}
