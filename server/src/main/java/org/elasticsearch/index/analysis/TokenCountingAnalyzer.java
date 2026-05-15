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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * An analyzer wrapper that counts the total number of tokens produced across all fields of a single document.
 * This protects against "monster documents" that can cause out-of-memory errors when Lucene's analysis phase
 * produces an excessive number of tokens (e.g. large text fields with n-gram analyzers).
 *
 * <p>The counter is stored in a thread-local and must be {@link #resetTokenCount() reset} before each document
 * is indexed. Since Lucene's {@code IndexWriter.addDocument()} processes all fields of a single document
 * sequentially on the calling thread, using a thread-local counter is safe.
 *
 * <p>When the token limit is exceeded, a {@link DocumentTokenCountExceededException} is thrown from within
 * Lucene's analysis pipeline, which propagates out of the {@code addDocument()} call.
 */
public final class TokenCountingAnalyzer extends AnalyzerWrapper {

    private final Analyzer delegate;
    private final LongSupplier maxTokenCountSupplier;
    private final ThreadLocal<AtomicLong> tokenCounter = ThreadLocal.withInitial(AtomicLong::new);

    /**
     * Creates a new token-counting analyzer wrapper.
     *
     * @param delegate              the underlying analyzer to wrap
     * @param maxTokenCountSupplier supplies the maximum number of tokens allowed per document;
     *                              a value of -1 or less means no limit is enforced
     */
    public TokenCountingAnalyzer(Analyzer delegate, LongSupplier maxTokenCountSupplier) {
        super(delegate.getReuseStrategy());
        this.delegate = delegate;
        this.maxTokenCountSupplier = maxTokenCountSupplier;
    }

    /**
     * Creates a new token-counting analyzer wrapper with a fixed limit.
     *
     * @param delegate      the underlying analyzer to wrap
     * @param maxTokenCount the maximum number of tokens allowed per document across all fields
     */
    public TokenCountingAnalyzer(Analyzer delegate, long maxTokenCount) {
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
        return new TokenStreamComponents(components.getSource(), new TokenCountingTokenFilter(components.getTokenStream()));
    }

    /**
     * Resets the per-document token counter. Must be called before each document is indexed.
     */
    public void resetTokenCount() {
        tokenCounter.get().set(0);
    }

    /**
     * Returns the current token count for the current thread/document.
     */
    public long getTokenCount() {
        return tokenCounter.get().get();
    }

    /**
     * Exception thrown when a document exceeds the maximum allowed token count during indexing.
     * This is an {@link IllegalArgumentException} because the document itself is invalid for the
     * configured limits, similar to how other mapping limit violations are reported.
     */
    public static final class DocumentTokenCountExceededException extends IllegalArgumentException {

        private final long maxTokenCount;

        public DocumentTokenCountExceededException(long maxTokenCount) {
            super(
                "The number of tokens produced while indexing this document has exceeded the allowed maximum of ["
                    + maxTokenCount
                    + "]. This limit can be set by changing the [index.mapping.total_tokens_per_document.limit] index level setting."
            );
            this.maxTokenCount = maxTokenCount;
        }

        public long getMaxTokenCount() {
            return maxTokenCount;
        }
    }

    /**
     * A token filter that increments a shared counter for each token and throws
     * {@link DocumentTokenCountExceededException} when the limit is exceeded.
     */
    private final class TokenCountingTokenFilter extends TokenFilter {

        TokenCountingTokenFilter(TokenStream input) {
            super(input);
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (input.incrementToken() == false) {
                return false;
            }
            long limit = maxTokenCountSupplier.getAsLong();
            if (limit > 0) {
                long count = tokenCounter.get().incrementAndGet();
                if (count > limit) {
                    throw new DocumentTokenCountExceededException(limit);
                }
            }
            return true;
        }
    }
}
