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

    protected ElasticsearchAnalyzerWrapper(Analyzer delegate) {
        super(reuseStrategyFor(delegate));
    }

    private static ReuseStrategy reuseStrategyFor(Analyzer delegate) {
        if (delegate instanceof ReloadableCustomAnalyzer rca) {
            return rca.createWrapperReuseStrategy();
        }
        return delegate.getReuseStrategy();
    }
}
