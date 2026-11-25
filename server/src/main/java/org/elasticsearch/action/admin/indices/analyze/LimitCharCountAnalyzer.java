/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.analyze;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;

import java.io.Reader;
import java.util.function.Consumer;

/**
 * Wraps another {@link Analyzer} and caps how many characters its character filters may feed to the tokenizer
 * for a single field value, failing the request with a {@code 400} once the cap is exceeded.
 *
 * <p>It uses its own {@link Analyzer#PER_FIELD_REUSE_STRATEGY}: the delegate may be a
 * {@link org.elasticsearch.index.analysis.NamedAnalyzer}, whose reuse strategy rejects being wrapped by a
 * non-delegating wrapper.
 *
 * @see LimitingReader
 */
final class LimitCharCountAnalyzer extends AnalyzerWrapper {

    private final Analyzer delegate;
    private final int maxCharCount;

    LimitCharCountAnalyzer(Analyzer delegate, int maxCharCount) {
        super(PER_FIELD_REUSE_STRATEGY);
        this.delegate = delegate;
        this.maxCharCount = maxCharCount;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return delegate;
    }

    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        final Consumer<Reader> source = components.getSource();
        return new TokenStreamComponents(reader -> source.accept(new LimitingReader(reader, maxCharCount)), components.getTokenStream());
    }

    @Override
    public String toString() {
        return "LimitCharCountAnalyzer(" + delegate.toString() + ", maxCharCount=" + maxCharCount + ")";
    }
}
