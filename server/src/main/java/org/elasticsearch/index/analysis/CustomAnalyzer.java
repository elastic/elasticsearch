/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.Reader;

public final class CustomAnalyzer extends Analyzer implements AnalyzerComponentsProvider {

    private final AnalyzerComponents components;
    private final int positionIncrementGap;
    private final int offsetGap;
    private final AnalysisMode analysisMode;

    public CustomAnalyzer(TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters,
            TokenFilterFactory[] tokenFilters) {
        this(tokenizerFactory, charFilters, tokenFilters, 0, -1);
    }

    public CustomAnalyzer(TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters,
            TokenFilterFactory[] tokenFilters, int positionIncrementGap, int offsetGap) {
        this.components = new AnalyzerComponents(tokenizerFactory, charFilters, tokenFilters);
        this.positionIncrementGap = positionIncrementGap;
        this.offsetGap = offsetGap;
        // merge and transfer token filter analysis modes with analyzer
        AnalysisMode mode = AnalysisMode.ALL;
        for (TokenFilterFactory f : tokenFilters) {
            mode = mode.merge(f.getAnalysisMode());
        }
        this.analysisMode = mode;
    }

    public TokenizerFactory tokenizerFactory() {
        return this.components.getTokenizerFactory();
    }

    public TokenFilterFactory[] tokenFilters() {
        return this.components.getTokenFilters();
    }

    public CharFilterFactory[] charFilters() {
        return this.components.getCharFilters();
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return this.positionIncrementGap;
    }

    @Override
    public int getOffsetGap(String field) {
        if (offsetGap < 0) {
            return super.getOffsetGap(field);
        }
        return this.offsetGap;
    }

    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }

    @Override
    public AnalyzerComponents getComponents() {
        return this.components;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = this.tokenizerFactory().create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : tokenFilters()) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        CharFilterFactory[] charFilters = charFilters();
        if (CollectionUtils.isEmpty(charFilters) == false) {
            for (CharFilterFactory charFilter : charFilters) {
                reader = charFilter.create(reader);
            }
        }
        return reader;
    }

    @Override
    protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        for (CharFilterFactory charFilter : charFilters()) {
            reader = charFilter.normalize(reader);
        }
        return reader;
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        TokenStream result = in;
        for (TokenFilterFactory filter : tokenFilters()) {
            result = filter.normalize(result);
        }
        return result;
    }
}
