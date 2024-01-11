/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into an optimized version for extracting inner_hits.
 */
public class InnerHitsRewriteContext extends QueryRewriteContext {
    public InnerHitsRewriteContext(
        final XContentParserConfiguration parserConfiguration,
        final Client client,
        final LongSupplier nowInMillis
    ) {
        super(parserConfiguration, client, nowInMillis);
    }

    @Override
    public InnerHitsRewriteContext convertToInnerHitsRewriteContext() {
        return this;
    }
}
