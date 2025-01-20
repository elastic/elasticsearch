/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into an optimized version for extracting inner_hits.
 */
public final class InnerHitsRewriteContext extends QueryRewriteContext {
    public InnerHitsRewriteContext(final XContentParserConfiguration parserConfiguration, final LongSupplier nowInMillis) {
        super(parserConfiguration, null, nowInMillis);
    }

    @Override
    public InnerHitsRewriteContext convertToInnerHitsRewriteContext() {
        return this;
    }

    @Override
    public void executeAsyncActions(ActionListener<Void> listener) {
        // InnerHitsRewriteContext does not support async actions at all, and doesn't supply a valid `client` object
        throw new UnsupportedOperationException("InnerHitsRewriteContext does not support async actions");
    }

}
