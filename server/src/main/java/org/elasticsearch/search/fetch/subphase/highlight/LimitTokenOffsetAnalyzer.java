/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.miscellaneous.LimitTokenOffsetFilter;

/**
 * This analyzer limits the highlighting once it sees a token with a start offset &lt;= the configured limit,
 * which won't pass and will end the stream.
 * @see LimitTokenOffsetFilter
 */
public final class LimitTokenOffsetAnalyzer extends AnalyzerWrapper {

    private final Analyzer delegate;
    private final int maxOffset;

    /**
     * Build an analyzer that limits the highlighting once it sees a token with a start offset &lt;= the configured limit,
     * which won't pass and will end the stream. See {@link LimitTokenOffsetFilter} for more details.
     *
     * @param delegate the analyzer to wrap
     * @param maxOffset max number of tokens to produce
     */
    public LimitTokenOffsetAnalyzer(Analyzer delegate, int maxOffset) {
        super(delegate.getReuseStrategy());
        this.delegate = delegate;
        this.maxOffset = maxOffset;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return delegate;
    }

    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        return new TokenStreamComponents(components.getSource(), new LimitTokenOffsetFilter(components.getTokenStream(), maxOffset, false));
    }

    @Override
    public String toString() {
        return "LimitTokenOffsetAnalyzer(" + delegate.toString() + ", maxOffset=" + maxOffset + ")";
    }
}
