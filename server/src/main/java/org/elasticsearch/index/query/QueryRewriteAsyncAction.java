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
import org.elasticsearch.client.internal.Client;

import java.util.List;
import java.util.function.Consumer;

public abstract class QueryRewriteAsyncAction {

    public abstract void execute(Client client, ActionListener<?> listener, List<Consumer<Object>> consumers);

    private final QueryRewriteContext queryRewriteContext;

    public QueryRewriteAsyncAction(QueryRewriteContext queryRewriteContext) {
        this.queryRewriteContext = queryRewriteContext;
    }

    protected QueryRewriteContext getQueryRewriteContext() {
        return queryRewriteContext;
    }
}
