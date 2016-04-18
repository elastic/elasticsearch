/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

import java.io.IOException;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.watcher.input.InputFactory;
import org.elasticsearch.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;

/**
 *
 */
public class SearchInputFactory extends InputFactory<SearchInput, SearchInput.Result, ExecutableSearchInput> {

    private final WatcherClientProxy client;
    private final TimeValue defaultTimeout;
    private final IndicesQueriesRegistry queryRegistry;
    private final AggregatorParsers aggParsers;
    private final Suggesters suggesters;
    private final ParseFieldMatcher parseFieldMatcher;

    @Inject
    public SearchInputFactory(Settings settings, WatcherClientProxy client, IndicesQueriesRegistry queryRegistry,
                              AggregatorParsers aggParsers, Suggesters suggesters) {
        super(Loggers.getLogger(ExecutableSimpleInput.class, settings));
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.client = client;
        this.queryRegistry = queryRegistry;
        this.aggParsers = aggParsers;
        this.suggesters = suggesters;
        this.defaultTimeout = settings.getAsTime("xpack.watcher.input.search.default_timeout", null);
    }

    @Override
    public String type() {
        return SearchInput.TYPE;
    }

    @Override
    public SearchInput parseInput(String watchId, XContentParser parser) throws IOException {
        QueryParseContext context = new QueryParseContext(queryRegistry, parser, parseFieldMatcher);
        return SearchInput.parse(watchId, parser, context, aggParsers, suggesters);
    }

    @Override
    public ExecutableSearchInput createExecutable(SearchInput input) {
        return new ExecutableSearchInput(input, inputLogger, client, defaultTimeout);
    }
}
