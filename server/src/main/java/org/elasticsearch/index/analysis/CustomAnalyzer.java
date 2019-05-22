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
import org.elasticsearch.index.analysis.CustomAnalyzerProvider.AnalyzerComponents;

import java.io.Reader;

public class CustomAnalyzer extends Analyzer {

    private final AnalysisMode analysisMode;
    protected volatile AnalyzerComponents components;

    public CustomAnalyzer(String tokenizerName, TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters,
                          TokenFilterFactory[] tokenFilters) {
        this(new AnalyzerComponents(tokenizerName, tokenizerFactory, charFilters, tokenFilters, 0, -1), GLOBAL_REUSE_STRATEGY);
    }

    CustomAnalyzer(AnalyzerComponents components, ReuseStrategy reuseStrategy) {
        super(reuseStrategy);
        this.components = components;
        this.analysisMode = calculateAnalysisMode(components);
    }

    /**
     * TODO: We should not expose functions that return objects from the <code>current</code>,
     * only the full {@link AnalyzerComponents} should be returned
     */

    /**
     * The name of the tokenizer as configured by the user.
     */
    public String getTokenizerName() {
        return components.tokenizerName;
    }

    public TokenizerFactory tokenizerFactory() {
        return components.tokenizerFactory;
    }

    public TokenFilterFactory[] tokenFilters() {
        return components.tokenFilters;
    }

    public CharFilterFactory[] charFilters() {
        return components.charFilters;
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return components.positionIncrementGap;
    }

    protected AnalyzerComponents getComponents() {
        return this.components;
    }

    @Override
    public int getOffsetGap(String field) {
        if (this.components.offsetGap < 0) {
            return super.getOffsetGap(field);
        }
        return this.components.offsetGap;
    }

    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final AnalyzerComponents components = getComponents();
        Tokenizer tokenizer = components.tokenizerFactory.create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : components.tokenFilters) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        final AnalyzerComponents components = getComponents();
        if (components.charFilters != null && components.charFilters.length > 0) {
            for (CharFilterFactory charFilter : components.charFilters) {
                reader = charFilter.create(reader);
            }
        }
        return reader;
    }

    @Override
    protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        final AnalyzerComponents components = getComponents();
      for (CharFilterFactory charFilter : components.charFilters) {
          reader = charFilter.normalize(reader);
      }
      return reader;
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        final AnalyzerComponents components = getComponents();
        TokenStream result = in;
        for (TokenFilterFactory filter : components.tokenFilters) {
            result = filter.normalize(result);
        }
        return result;
    }

    private static AnalysisMode calculateAnalysisMode(AnalyzerComponents components) {
        // merge and transfer token filter analysis modes with analyzer
        AnalysisMode mode = AnalysisMode.ALL;
        for (TokenFilterFactory f : components.tokenFilters) {
            mode = mode.merge(f.getAnalysisMode());
        }
        return mode;
    }

    /**
     * Factory method that either returns a plain {@link CustomAnalyzer} if the components used for creation are supporting index and search
     * time use, or a {@link ReloadableCustomAnalyzer} if the components are intended for search time use only.
     */
    static CustomAnalyzer create(AnalyzerComponents components) {
        AnalysisMode mode = calculateAnalysisMode(components);
        if (mode.equals(AnalysisMode.SEARCH_TIME)) {
            return new ReloadableCustomAnalyzer(components);
        } else {
            return new CustomAnalyzer(components, GLOBAL_REUSE_STRATEGY);
        }
    }
}
