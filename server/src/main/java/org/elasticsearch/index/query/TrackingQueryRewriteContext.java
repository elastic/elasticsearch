/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.search.usage.QueriesUsageService;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.HashSet;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * {@link QueryRewriteContext} that tracks unique query types used in this context.
 */
public class TrackingQueryRewriteContext extends QueryRewriteContext {
    private final Set<String> usedQueries = new HashSet<>();

    public TrackingQueryRewriteContext(
        XContentParserConfiguration parserConfiguration,
        NamedWriteableRegistry writeableRegistry,
        Client client,
        LongSupplier nowInMillis
    ) {
        super(parserConfiguration, writeableRegistry, client, nowInMillis);
    }

    public void addQueryUsage(String query) {
        usedQueries.add(query);
    }

    public void reportQueriesUsage(QueriesUsageService queriesUsageService) {
        for (String query : usedQueries) {
            queriesUsageService.incrementQueryUsage(query);
        }
    }

}
