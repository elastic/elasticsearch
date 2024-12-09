/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

public class RomanianAnalyzerProvider extends AbstractIndexAnalyzerProvider<StopwordAnalyzerBase> {

    private final StopwordAnalyzerBase analyzer;

    RomanianAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name, settings);
        CharArraySet stopwords = Analysis.parseStopWords(env, settings, RomanianAnalyzer.getDefaultStopSet());
        CharArraySet stemExclusionSet = Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET);
        if (indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)) {
            // since Lucene 10, this analyzer a modern unicode form and normalizes cedilla forms to forms with commas
            analyzer = new RomanianAnalyzer(stopwords, stemExclusionSet);
        } else {
            // for older index versions we need the old behaviour without normalization
            analyzer = new StopwordAnalyzerBase(Analysis.parseStopWords(env, settings, RomanianAnalyzer.getDefaultStopSet())) {

                protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
                    final Tokenizer source = new StandardTokenizer();
                    TokenStream result = new LowerCaseFilter(source);
                    result = new StopFilter(result, stopwords);
                    if (stemExclusionSet.isEmpty() == false) {
                        result = new SetKeywordMarkerFilter(result, stemExclusionSet);
                    }
                    result = new SnowballFilter(result, new LegacyRomanianStemmer());
                    return new TokenStreamComponents(source, result);
                }

                protected TokenStream normalize(String fieldName, TokenStream in) {
                    return new LowerCaseFilter(in);
                }
            };

        }
    }

    @Override
    public StopwordAnalyzerBase get() {
        return this.analyzer;
    }
}
