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
 * A context object for rewriting {@link QueryBuilder} instances to optimize query execution
 * by reducing the global operations required for processing Lucene queries on a selection of top documents.
 *
 * This rewrite context ensures that expensive operations, such as approximate nearest neighbor (ANN) searches,
 * are avoided when evaluating documents in isolation. Instead, ANN queries are rewritten into exact k-NN queries,
 * which execute efficiently when processing only a small subset of documents (or none at all).
 *
 * Additionally, this context is beneficial for nested inner hits, allowing child documents to receive
 * independent similarity scores regardless of whether they matched the approximate nearest neighbor query.
 */
public final class PerDocumentQueryRewriteContext extends QueryRewriteContext {
    public PerDocumentQueryRewriteContext(final XContentParserConfiguration parserConfiguration, final LongSupplier nowInMillis) {
        super(parserConfiguration, null, nowInMillis);
    }

    @Override
    public PerDocumentQueryRewriteContext convertToPerDocumentQueryRewriteContext() {
        return this;
    }

    @Override
    public void executeAsyncActions(ActionListener<Void> listener) {
        // PerDocumentQueryRewriteContext does not support async actions at all, and doesn't supply a valid `client` object
        throw new UnsupportedOperationException("PerDocumentQueryRewriteContext does not support async actions");
    }

}
