/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexService.IndexCreationContext;

import java.io.Reader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ReloadableCustomAnalyzer extends Analyzer implements AnalyzerComponentsProvider {

    private volatile AnalyzerComponents components;

    // external resources that this analyzer is based on
    private final Set<String> resources;

    private final int positionIncrementGap;

    private final int offsetGap;

    /**
     * Reuse strategy used when this analyzer is invoked directly (not through an
     * {@link org.apache.lucene.analysis.AnalyzerWrapper}). The bundle {@code Object[] {components, tokenStream}}
     * stored in the per-strategy slot lets us detect reloads by comparing the cached
     * components reference against the current one. Wrapped use is handled by
     * {@link #createWrapperReuseStrategy()} — same logic, but the delegate reference
     * is captured via closure rather than via the cast below.
     */
    private static final ReuseStrategy REUSE_STRATEGY = new ReuseStrategy() {
        @Override
        public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
            AnalyzerComponents current = ((ReloadableCustomAnalyzer) analyzer).getComponents();
            Object[] stored = (Object[]) getStoredValue(analyzer);
            if (stored == null || stored[0] != current) {
                return null;
            }
            return (TokenStreamComponents) stored[1];
        }

        @Override
        public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents tokenStream) {
            setStoredValue(analyzer, new Object[] { ((ReloadableCustomAnalyzer) analyzer).getComponents(), tokenStream });
        }
    };

    public ReloadableCustomAnalyzer(AnalyzerComponents components, int positionIncrementGap, int offsetGap) {
        super(REUSE_STRATEGY);
        if (components.analysisMode().equals(AnalysisMode.SEARCH_TIME) == false) {
            throw new IllegalArgumentException(
                "ReloadableCustomAnalyzer must only be initialized with analysis components in AnalysisMode.SEARCH_TIME mode"
            );
        }
        this.components = components;
        this.positionIncrementGap = positionIncrementGap;
        this.offsetGap = offsetGap;

        Set<String> resourcesTemp = new HashSet<>();
        for (TokenFilterFactory tokenFilter : components.getTokenFilters()) {
            if (tokenFilter.getResourceName() != null) {
                resourcesTemp.add(tokenFilter.getResourceName());
            }
        }
        resources = resourcesTemp.isEmpty() ? null : Set.copyOf(resourcesTemp);
    }

    @Override
    public AnalyzerComponents getComponents() {
        return this.components;
    }

    public boolean usesResource(String resourceName) {
        if (resourceName == null) {
            return true;
        }
        if (resources == null) {
            return false;
        }
        return resources.contains(resourceName);
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

    public synchronized void reload(
        String name,
        Settings settings,
        final Map<String, TokenizerFactory> tokenizers,
        final Map<String, CharFilterFactory> charFilters,
        final Map<String, TokenFilterFactory> tokenFilters
    ) {
        AnalyzerComponents components = AnalyzerComponents.createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            name,
            settings,
            tokenizers,
            charFilters,
            tokenFilters
        );
        this.components = components;
    }

    /**
     * Reuse strategy for {@link org.apache.lucene.analysis.AnalyzerWrapper} subclasses that delegate to this
     * {@link ReloadableCustomAnalyzer}. Mirrors {@link #REUSE_STRATEGY} body, but captures
     * the delegate via closure instead of casting the {@code analyzer} parameter — that
     * parameter is the wrapper, not this instance, so the cast in {@link #REUSE_STRATEGY}
     * would fail. The bundled {@code {components, tokenStream}} per-strategy state keeps
     * each wrapper isolated, which matters when several wrappers share one delegate
     * (e.g. {@code SearchAsYouTypeFieldMapper} wraps the same analyzer with different
     * shingle sizes).
     */
    public ReuseStrategy createWrapperReuseStrategy() {
        return new ReuseStrategy() {
            @Override
            public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
                AnalyzerComponents current = ReloadableCustomAnalyzer.this.getComponents();
                Object[] stored = (Object[]) getStoredValue(analyzer);
                if (stored == null || stored[0] != current) {
                    return null;
                }
                return (TokenStreamComponents) stored[1];
            }

            @Override
            public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents tokenStream) {
                setStoredValue(analyzer, new Object[] { ReloadableCustomAnalyzer.this.getComponents(), tokenStream });
            }
        };
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final AnalyzerComponents components = getComponents();
        Tokenizer tokenizer = components.getTokenizerFactory().create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : components.getTokenFilters()) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        final AnalyzerComponents components = getComponents();
        if (CollectionUtils.isEmpty(components.getCharFilters()) == false) {
            for (CharFilterFactory charFilter : components.getCharFilters()) {
                reader = charFilter.create(reader);
            }
        }
        return reader;
    }
}
