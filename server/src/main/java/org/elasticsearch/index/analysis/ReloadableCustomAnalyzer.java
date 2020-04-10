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
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.common.settings.Settings;

import java.io.Reader;
import java.util.Map;

public final class ReloadableCustomAnalyzer extends Analyzer implements AnalyzerComponentsProvider {

    private volatile AnalyzerComponents components;

    private CloseableThreadLocal<AnalyzerComponents> storedComponents = new CloseableThreadLocal<>();

    private final int positionIncrementGap;

    private final int offsetGap;

    /**
     * An alternative {@link ReuseStrategy} that allows swapping the stored analyzer components when they change.
     * This is used to change e.g. token filters in search time analyzers.
     */
    private static final ReuseStrategy UPDATE_STRATEGY = new ReuseStrategy() {
        @Override
        public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
            ReloadableCustomAnalyzer custom = (ReloadableCustomAnalyzer) analyzer;
            AnalyzerComponents components = custom.getComponents();
            AnalyzerComponents storedComponents = custom.getStoredComponents();
            if (storedComponents == null || components != storedComponents) {
                custom.setStoredComponents(components);
                return null;
            }
            TokenStreamComponents tokenStream = (TokenStreamComponents) getStoredValue(analyzer);
            assert tokenStream != null;
            return tokenStream;
        }

        @Override
        public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents tokenStream) {
            setStoredValue(analyzer, tokenStream);
        }
    };

    ReloadableCustomAnalyzer(AnalyzerComponents components, int positionIncrementGap, int offsetGap) {
        super(UPDATE_STRATEGY);
        if (components.analysisMode().equals(AnalysisMode.SEARCH_TIME) == false) {
            throw new IllegalArgumentException(
                    "ReloadableCustomAnalyzer must only be initialized with analysis components in AnalysisMode.SEARCH_TIME mode");
        }
        this.components = components;
        this.positionIncrementGap = positionIncrementGap;
        this.offsetGap = offsetGap;
    }

    @Override
    public AnalyzerComponents getComponents() {
        return this.components;
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return this.positionIncrementGap;
    }

    @Override
    public int getOffsetGap(String field) {
        if (this.offsetGap < 0) {
            return super.getOffsetGap(field);
        }
        return this.offsetGap;
    }

    public AnalysisMode getAnalysisMode() {
        return this.components.analysisMode();
    }

    @Override
    protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        final AnalyzerComponents components = getComponents();
        for (CharFilterFactory charFilter : components.getCharFilters()) {
            reader = charFilter.normalize(reader);
        }
        return reader;
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        final AnalyzerComponents components = getComponents();
        TokenStream result = in;
        for (TokenFilterFactory filter : components.getTokenFilters()) {
            result = filter.normalize(result);
        }
        return result;
    }

    public synchronized void reload(String name,
                                    Settings settings,
                                    final Map<String, TokenizerFactory> tokenizers,
                                    final Map<String, CharFilterFactory> charFilters,
                                    final Map<String, TokenFilterFactory> tokenFilters) {
        AnalyzerComponents components = AnalyzerComponents.createComponents(name, settings, tokenizers, charFilters, tokenFilters);
        this.components = components;
    }

    @Override
    public void close() {
        super.close();
        storedComponents.close();
    }

    private void setStoredComponents(AnalyzerComponents components) {
        storedComponents.set(components);
    }

    private AnalyzerComponents getStoredComponents() {
        return storedComponents.get();
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final AnalyzerComponents components = getStoredComponents();
        Tokenizer tokenizer = components.getTokenizerFactory().create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : components.getTokenFilters()) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        final AnalyzerComponents components = getStoredComponents();
        if (components.getCharFilters() != null && components.getCharFilters().length > 0) {
            for (CharFilterFactory charFilter : components.getCharFilters()) {
                reader = charFilter.create(reader);
            }
        }
        return reader;
    }
}
