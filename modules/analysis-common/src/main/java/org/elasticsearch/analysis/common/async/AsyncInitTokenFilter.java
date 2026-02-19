/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common.async;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

public abstract class AsyncInitTokenFilter<T> extends TokenFilter {
    private final PlainActionFuture<T> resourceFuture;
    private final BiFunction<TokenStream, T, TokenFilter> tokenFilterBuilder;

    // No thread safety necessary because Lucene builds a token filter per thread
    private TokenFilter tokenFilter = null;

    public AsyncInitTokenFilter(
        TokenStream input,
        PlainActionFuture<T> resourceFuture,
        BiFunction<TokenStream, T, TokenFilter> tokenFilterBuilder
    ) {
        super(input);
        this.resourceFuture = Objects.requireNonNull(resourceFuture);
        this.tokenFilterBuilder = Objects.requireNonNull(tokenFilterBuilder);
    }

    @Override
    public final boolean incrementToken() throws IOException {
        boolean hasTokens = true;
        if (tokenFilter == null) {
            if (resourceFuture.isDone()) {
                tokenFilter = tokenFilterBuilder.apply(input, getResultFromCompletedFuture(resourceFuture));
            } else {
                hasTokens = input.incrementToken();
                if (hasTokens) {
                    // We use an empty token stream to validate analysis component compatibility with Elasticsearch when an analyzer is
                    // constructed. If the token stream is not empty, it means that we're processing a real token stream and our async
                    // initializing analyzer is not ready yet, so we have to fail.
                    throw new ElasticsearchStatusException(resourceNotReadyErrorMessage(), RestStatus.TOO_MANY_REQUESTS);
                }
            }
        }

        return hasTokens && tokenFilter.incrementToken();
    }

    @Override
    public void end() throws IOException {
        if (tokenFilter != null) {
            tokenFilter.end();
        } else {
            super.end();
        }
    }

    @Override
    public void close() throws IOException {
        if (tokenFilter != null) {
            tokenFilter.close();
        } else {
            super.close();
        }
    }

    @Override
    public void reset() throws IOException {
        if (tokenFilter != null) {
            tokenFilter.reset();
        } else {
            super.reset();
        }
    }

    protected abstract String resourceNotReadyErrorMessage();

    protected abstract String resourceFutureFailedErrorMessage();

    private T getResultFromCompletedFuture(PlainActionFuture<T> future) {
        try {
            return future.result();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new ElasticsearchStatusException(resourceFutureFailedErrorMessage(), ExceptionsHelper.status(cause), cause);
        }
    }
}
