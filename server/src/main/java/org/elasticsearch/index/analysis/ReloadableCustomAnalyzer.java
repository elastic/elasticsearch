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
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexService.IndexCreationContext;

import java.io.Reader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ReloadableCustomAnalyzer extends Analyzer implements AnalyzerComponentsProvider {

    private volatile AnalyzerComponents components;

    private CloseableThreadLocal<AnalyzerComponents> storedComponents = new CloseableThreadLocal<>();

    // external resources that this analyzer is based on
    private final Set<String> resources;

    private final int positionIncrementGap;

    private final int offsetGap;

    /**
     * Reuse strategy used when this analyzer is invoked directly (not through an
     * {@link org.apache.lucene.analysis.AnalyzerWrapper}).
     * <p>Wrapped use is handled by {@link #createWrapperReuseStrategy()}
     */
    private static final ReuseStrategy UPDATE_STRATEGY = new ReuseStrategy() {
        @Override
        public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
            ReloadableCustomAnalyzer custom = (ReloadableCustomAnalyzer) analyzer;
            if (custom.pinCurrentComponents() == false) {
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

    public ReloadableCustomAnalyzer(AnalyzerComponents components, int positionIncrementGap, int offsetGap) {
        super(UPDATE_STRATEGY);
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

    @Override
    public void close() {
        super.close();
        storedComponents.close();
    }

    /**
     * Reuse strategy for {@link org.apache.lucene.analysis.AnalyzerWrapper} subclasses that delegate
     * to this {@link ReloadableCustomAnalyzer}.
     *
     * <p>Tracks freshness independently from {@link #UPDATE_STRATEGY} by storing the
     * {@link AnalyzerComponents} version together with the {@link TokenStreamComponents} in the
     * wrapper analyzer's own per-thread slot, so the two strategies never interfere with each other.
     */
    public ReuseStrategy createWrapperReuseStrategy() {
        return new ReuseStrategy() {
            @Override
            public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
                // Pin storedComponents for this thread via the shared pinning logic.
                pinCurrentComponents();

                AnalyzerComponents pinned = getStoredComponents();
                WrapperEntry entry = (WrapperEntry) getStoredValue(analyzer);
                if (entry == null || entry.components != pinned) {
                    return null;
                }
                return entry.tsc;
            }

            @Override
            public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents tsc) {
                setStoredValue(analyzer, new WrapperEntry(getStoredComponents(), tsc));
            }
        };
    }

    /** Pairs an {@link AnalyzerComponents} snapshot with the corresponding {@link TokenStreamComponents} for a wrapper analyzer. */
    private record WrapperEntry(AnalyzerComponents components, TokenStreamComponents tsc) {}

    private void setStoredComponents(AnalyzerComponents components) {
        storedComponents.set(components);
    }

    private AnalyzerComponents getStoredComponents() {
        return storedComponents.get();
    }

    /**
     * Pins the current {@link AnalyzerComponents} snapshot to {@code storedComponents} for this thread.
     * Both {@link #UPDATE_STRATEGY} and {@link #createWrapperReuseStrategy()} use this single source
     * of truth for the "snapshot pinned for this call" contract.
     *
     * @return {@code true} if the previously pinned components are still current (reusable),
     *         {@code false} if components changed and a new snapshot was pinned
     */
    private boolean pinCurrentComponents() {
        AnalyzerComponents current = getComponents();
        AnalyzerComponents stored = getStoredComponents();
        if (stored == null || current != stored) {
            setStoredComponents(current);
            return false;
        }
        return true;
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
        if (CollectionUtils.isEmpty(components.getCharFilters()) == false) {
            for (CharFilterFactory charFilter : components.getCharFilters()) {
                reader = charFilter.create(reader);
            }
        }
        return reader;
    }
}
