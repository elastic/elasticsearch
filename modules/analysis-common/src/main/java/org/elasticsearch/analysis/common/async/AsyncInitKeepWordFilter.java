/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common.async;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.action.support.PlainActionFuture;

import java.util.function.BiFunction;

public class AsyncInitKeepWordFilter extends AsyncInitTokenFilter<CharArraySet> {
    public AsyncInitKeepWordFilter(
        TokenStream input,
        PlainActionFuture<CharArraySet> resourceFuture,
        BiFunction<TokenStream, CharArraySet, TokenFilter> tokenFilterBuilder
    ) {
        super(input, resourceFuture, tokenFilterBuilder);
    }

    @Override
    protected String resourceNotReadyErrorMessage() {
        return "Keep words loading in progress";
    }

    @Override
    protected String resourceFutureFailedErrorMessage() {
        return "Keep words loading failed";
    }
}
