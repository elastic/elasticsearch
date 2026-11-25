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
import org.apache.lucene.analysis.AnalyzerWrapper;

/**
 * Base class for Elasticsearch {@link AnalyzerWrapper} subclasses. Selects a
 * {@link ReuseStrategy} that detects when the wrapped delegate is a
 * {@link ReloadableCustomAnalyzer} and propagates reloads (e.g. synonym
 * updates) to the wrapper, instead of inheriting a strategy that either
 * ignores reloads or assumes the wrapping analyzer is itself reloadable.
 */
public abstract class ElasticsearchAnalyzerWrapper extends AnalyzerWrapper {

    /**
     * Reuse strategy for wrappers around a {@link ReloadableCustomAnalyzer}.
     * Tracks freshness independently by storing the {@link AnalyzerComponents} version
     * together with the {@link TokenStreamComponents} in the wrapper's per-thread slot.
     */
    private static final ReuseStrategy WRAPPER_REUSE_STRATEGY = new ReuseStrategy() {
        @Override
        public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
            ElasticsearchAnalyzerWrapper wrapper = (ElasticsearchAnalyzerWrapper) analyzer;
            ReloadableCustomAnalyzer rca = (ReloadableCustomAnalyzer) wrapper.getWrappedAnalyzer(fieldName);
            AnalyzerComponents current = rca.getComponents();
            WrapperEntry entry = (WrapperEntry) getStoredValue(analyzer);
            if (entry == null || entry.components != current) {
                return null;
            }
            return entry.tsc;
        }

        @Override
        public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents tsc) {
            ElasticsearchAnalyzerWrapper wrapper = (ElasticsearchAnalyzerWrapper) analyzer;
            ReloadableCustomAnalyzer rca = (ReloadableCustomAnalyzer) wrapper.getWrappedAnalyzer(fieldName);
            setStoredValue(analyzer, new WrapperEntry(rca.getComponents(), tsc));
        }
    };

    /** Pairs an {@link AnalyzerComponents} snapshot with the corresponding {@link TokenStreamComponents}. */
    private record WrapperEntry(AnalyzerComponents components, TokenStreamComponents tsc) {}

    protected ElasticsearchAnalyzerWrapper(Analyzer delegate) {
        super(reuseStrategyFor(delegate));
    }

    private static ReuseStrategy reuseStrategyFor(Analyzer delegate) {
        if (delegate instanceof ReloadableCustomAnalyzer) {
            return WRAPPER_REUSE_STRATEGY;
        }
        return delegate.getReuseStrategy();
    }
}
