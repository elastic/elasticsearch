/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Test plugin that exposes a way to simulate search shard failures and warnings.
 */
public class ErrorQueryPlugin extends Plugin implements SearchPlugin {
    public ErrorQueryPlugin() {}

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(ErrorQueryBuilder.NAME, ErrorQueryBuilder::new, p -> ErrorQueryBuilder.PARSER.parse(p, null)));
    }
}
