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
import java.util.function.IntSupplier;

/**
 * An analyzer wrapper that limits the number of tokens produced per field during indexing.
 * This protects against "monster documents" that can cause out-of-memory errors when Lucene's
 * analysis phase produces an excessive number of tokens (e.g. large text fields with n-gram analyzers).
 *
 * <p>Each field's {@link TokenCountingTokenFilter} resets its counter in {@link TokenFilter#reset()},
 * counts tokens during {@link TokenFilter#incrementToken()}, and throws a
 * {@link FieldTokenCountExceededException} if the limit is exceeded. The limit is read from the
 * supplier once per field (in {@code reset()}) so dynamic setting changes take effect immediately
 * without engine restart.
 */
public final class TokenCountingAnalyzer extends AnalyzerWrapper {

    private final Analyzer delegate;
    private final IntSupplier maxTokenCountSupplier;

    /**
     * Creates a new token-counting analyzer wrapper.
     *
     * @param delegate              the underlying analyzer to wrap
     * @param maxTokenCountSupplier supplies the maximum number of tokens allowed per field;
     *                              a value of -1 or less means no limit is enforced
     */
    public TokenCountingAnalyzer(Analyzer delegate, IntSupplier maxTokenCountSupplier) {
        super(delegate.getReuseStrategy());
        this.delegate = delegate;
        this.maxTokenCountSupplier = maxTokenCountSupplier;
    }

    /**
     * Creates a new token-counting analyzer wrapper with a fixed limit.
     *
     * @param delegate      the underlying analyzer to wrap
     * @param maxTokenCount the maximum number of tokens allowed per field
     */
    TokenCountingAnalyzer(Analyzer delegate, int maxTokenCount) {
        this(delegate, () -> maxTokenCount);
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
        return new TokenStreamComponents(
            components.getSource(),
            new TokenCountingTokenFilter(components.getTokenStream(), fieldName, maxTokenCountSupplier)
        );
    }

    /**
     * Exception thrown when a field exceeds the maximum allowed token count during indexing.
     * This is an {@link IllegalArgumentException} because the document itself is invalid for the
     * configured limits, similar to how other mapping limit violations are reported.
     */
    public static final class FieldTokenCountExceededException extends IllegalArgumentException {

        private final int maxTokenCount;
        private final String fieldName;

        public FieldTokenCountExceededException(String fieldName, int maxTokenCount) {
            super(
                "The number of tokens produced while analyzing field ["
                    + fieldName
                    + "] has exceeded the allowed maximum of ["
                    + maxTokenCount
                    + "]. This limit can be set by changing the [index.mapping.tokens_per_field.limit] index level setting."
            );
            this.maxTokenCount = maxTokenCount;
            this.fieldName = fieldName;
        }

        int getMaxTokenCount() {
            return maxTokenCount;
        }

        String getFieldName() {
            return fieldName;
        }
    }

    /**
     * A token filter that counts tokens for a single field and throws
     * {@link FieldTokenCountExceededException} when the limit is exceeded.
     *
     * <p>The counter resets to zero in {@link #reset()} (called by Lucene before each field's
     * token stream starts) and the limit is cached from the supplier at that point.
     */
    private static final class TokenCountingTokenFilter extends TokenFilter {

        private final String fieldName;
        private final IntSupplier limitSupplier;
        private int localCount;
        private int cachedLimit;

        TokenCountingTokenFilter(TokenStream input, String fieldName, IntSupplier limitSupplier) {
            super(input);
            this.fieldName = fieldName;
            this.limitSupplier = limitSupplier;
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            cachedLimit = limitSupplier.getAsInt();
            localCount = 0;
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (input.incrementToken() == false) {
                return false;
            }
            if (cachedLimit > 0) {
                localCount++;
                if (localCount > cachedLimit) {
                    throw new FieldTokenCountExceededException(fieldName, cachedLimit);
                }
            }
            return true;
        }
    }
}
