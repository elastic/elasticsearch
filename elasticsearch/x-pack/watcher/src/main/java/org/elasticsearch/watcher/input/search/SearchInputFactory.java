/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.watcher.input.InputFactory;
import org.elasticsearch.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;

import java.io.IOException;

/**
 *
 */
public class SearchInputFactory extends InputFactory<SearchInput, SearchInput.Result, ExecutableSearchInput> {

    private final ClientProxy client;
    private final TimeValue defaultTimeout;
    private IndicesQueriesRegistry queryRegistry;

    @Inject
    public SearchInputFactory(Settings settings, ClientProxy client, IndicesQueriesRegistry queryRegistry) {
        super(Loggers.getLogger(ExecutableSimpleInput.class, settings));
        this.client = client;
        this.queryRegistry = queryRegistry;
        this.defaultTimeout = settings.getAsTime("watcher.input.search.default_timeout", null);
    }

    @Override
    public String type() {
        return SearchInput.TYPE;
    }

    @Override
    public SearchInput parseInput(String watchId, XContentParser parser) throws IOException {
        QueryParseContext context = new QueryParseContext(queryRegistry);
        context.reset(parser);
        return SearchInput.parse(watchId, parser, context);
    }

    @Override
    public ExecutableSearchInput createExecutable(SearchInput input) {
        return new ExecutableSearchInput(input, inputLogger, client, defaultTimeout);
    }
}
