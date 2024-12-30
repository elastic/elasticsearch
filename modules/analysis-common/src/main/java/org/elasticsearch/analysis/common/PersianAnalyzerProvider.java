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
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fa.PersianCharFilter;
import org.apache.lucene.analysis.fa.PersianNormalizationFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

import java.io.Reader;

public class PersianAnalyzerProvider extends AbstractIndexAnalyzerProvider<StopwordAnalyzerBase> {

    private final StopwordAnalyzerBase analyzer;

    PersianAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        if (indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)) {
            // since Lucene 10 this analyzer contains stemming by default
            analyzer = new PersianAnalyzer(Analysis.parseStopWords(env, settings, PersianAnalyzer.getDefaultStopSet()));
        } else {
            // for older index versions we need the old analyzer behaviour without stemming
            analyzer = new StopwordAnalyzerBase(Analysis.parseStopWords(env, settings, PersianAnalyzer.getDefaultStopSet())) {

                protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
                    final Tokenizer source = new StandardTokenizer();
                    TokenStream result = new LowerCaseFilter(source);
                    result = new DecimalDigitFilter(result);
                    result = new ArabicNormalizationFilter(result);
                    /* additional persian-specific normalization */
                    result = new PersianNormalizationFilter(result);
                    /*
                     * the order here is important: the stopword list is normalized with the
                     * above!
                     */
                    return new TokenStreamComponents(source, new StopFilter(result, stopwords));
                }

                protected TokenStream normalize(String fieldName, TokenStream in) {
                    TokenStream result = new LowerCaseFilter(in);
                    result = new DecimalDigitFilter(result);
                    result = new ArabicNormalizationFilter(result);
                    /* additional persian-specific normalization */
                    result = new PersianNormalizationFilter(result);
                    return result;
                }

                protected Reader initReader(String fieldName, Reader reader) {
                    return new PersianCharFilter(reader);
                }
            };
        }
    }

    @Override
    public StopwordAnalyzerBase get() {
        return this.analyzer;
    }
}
