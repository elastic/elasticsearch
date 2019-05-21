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
import org.elasticsearch.index.analysis.CustomAnalyzerProvider.AnalyzerComponents;

import java.io.Reader;
import java.util.Map;

public final class CustomAnalyzer extends Analyzer {
    private CloseableThreadLocal<AnalyzerComponents> storedComponents = new CloseableThreadLocal<>();
    private volatile AnalyzerComponents current;

    private final AnalysisMode analysisMode;

    private static final ReuseStrategy UPDATE_STRATEGY = new ReuseStrategy() {
        @Override
        public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
            CustomAnalyzer custom = (CustomAnalyzer) analyzer;
            AnalyzerComponents components = custom.getStoredComponents();
            if (components == null || custom.shouldReload(components)) {
                custom.setStoredComponents(custom.current);
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

    public CustomAnalyzer(String tokenizerName, TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters,
                          TokenFilterFactory[] tokenFilters) {
        this(new AnalyzerComponents(tokenizerName, tokenizerFactory, charFilters, tokenFilters, -1, -1));
    }

    public CustomAnalyzer(String tokenizerName, TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters,
                          TokenFilterFactory[] tokenFilters, int positionIncrementGap, int offsetGap) {
       this(new AnalyzerComponents(tokenizerName, tokenizerFactory, charFilters, tokenFilters, positionIncrementGap, offsetGap));
    }

    public CustomAnalyzer(AnalyzerComponents components) {
        super(UPDATE_STRATEGY);
        this.current = components;
        // merge and transfer token filter analysis modes with analyzer
        AnalysisMode mode = AnalysisMode.ALL;
        for (TokenFilterFactory f : current.tokenFilters) {
            mode = mode.merge(f.getAnalysisMode());
        }
        this.analysisMode = mode;
    }

    /**
     * TODO: We should not expose functions that return objects from the <code>current</code>,
     * only the full {@link AnalyzerComponents} should be returned
     */

    /**
     * The name of the tokenizer as configured by the user.
     */
    public String getTokenizerName() {
        return current.tokenizerName;
    }

    public TokenizerFactory tokenizerFactory() {
        return current.tokenizerFactory;
    }

    public TokenFilterFactory[] tokenFilters() {
        return current.tokenFilters;
    }

    public CharFilterFactory[] charFilters() {
        return current.charFilters;
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return current.positionIncrementGap;
    }

    private boolean shouldReload(AnalyzerComponents source) {
        return this.current != source;
    }

    public synchronized void reload(String name,
                                    Settings settings,
                                    final Map<String, TokenizerFactory> tokenizers,
                                    final Map<String, CharFilterFactory> charFilters,
                                    final Map<String, TokenFilterFactory> tokenFilters) {
        AnalyzerComponents components = CustomAnalyzerProvider.createComponents(name, settings, tokenizers, charFilters, tokenFilters);
        this.current = components;
    }

    @Override
    public void close() {
        storedComponents.close();
    }

    void setStoredComponents(AnalyzerComponents components) {
        storedComponents.set(components);
    }

    AnalyzerComponents getStoredComponents() {
        return storedComponents.get();
    }

    public AnalyzerComponents getComponents() {
        return current;
    }

    @Override
    public int getOffsetGap(String field) {
        final AnalyzerComponents components = getComponents();
        if (components.offsetGap < 0) {
            return super.getOffsetGap(field);
        }
        return components.offsetGap;
    }

    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final AnalyzerComponents components = getStoredComponents();
        Tokenizer tokenizer = components.tokenizerFactory.create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : components.tokenFilters) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        final AnalyzerComponents components = getStoredComponents();
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
}
