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
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.suggest.document.CompletionTokenStream;

import java.io.IOException;

/**
 * An analyzer wrapper that counts the number of tokens produced per field value during indexing
 * and records the count to a {@link TokenCountingMetrics} histogram. This enables observation of
 * the token count distribution across the fleet without enforcing any limits.
 *
 * <p>Each field value's token count is recorded when the token stream is finalized
 * via {@link TokenFilter#end()}. The counter resets in {@link TokenFilter#reset()}
 * before each new field value.
 */
public final class TokenCountingAnalyzer extends AnalyzerWrapper {

    private final Analyzer delegate;
    private final TokenCountingMetrics metrics;

    /**
     * Creates a new token-counting analyzer wrapper.
     *
     * <p>Uses {@link Analyzer#PER_FIELD_REUSE_STRATEGY} from the delegate, which caches
     * {@link TokenStreamComponents} permanently per field. This is safe today because
     * {@link ReloadableCustomAnalyzer} is search-time only. If index-time reloadable
     * analyzers are introduced in the future, the reuse strategy here will need to be
     * made reload-aware.
     *
     * @param delegate the underlying analyzer to wrap
     * @param metrics  metrics instance for recording token counts per field value
     */
    public TokenCountingAnalyzer(Analyzer delegate, TokenCountingMetrics metrics) {
        super(delegate.getReuseStrategy());
        this.delegate = delegate;
        this.metrics = metrics;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return delegate;
    }

    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        // CompletionTokenStream is a special token stream used by Lucene's suggest/completion fields.
        // Wrapping it with a TokenFilter would break the suggest indexing pipeline, so we skip counting
        // for completion fields.
        if (components.getTokenStream() instanceof CompletionTokenStream) {
            return components;
        }
        return new TokenStreamComponents(components.getSource(), new TokenCountingTokenFilter(components.getTokenStream(), metrics));
    }

    /**
     * A token filter that counts tokens for a single field value and records the count
     * to the metrics histogram when the token stream is finalized via {@link #end()}.
     *
     * <p>The counter resets to zero in {@link #reset()} (called by Lucene before each field
     * value's token stream starts).
     */
    private static final class TokenCountingTokenFilter extends TokenFilter {

        private final TokenCountingMetrics metrics;
        private int count;

        TokenCountingTokenFilter(TokenStream input, TokenCountingMetrics metrics) {
            super(input);
            this.metrics = metrics;
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            count = 0;
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (input.incrementToken() == false) {
                return false;
            }
            count++;
            return true;
        }

        @Override
        public void end() throws IOException {
            super.end();
            metrics.recordTokenCount(count);
        }
    }
}
