/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.search;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.input.InputFactory;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;

import java.io.IOException;
import java.util.function.Predicate;

public class SearchInputFactory extends InputFactory<SearchInput, SearchInput.Result, ExecutableSearchInput> {

    private final Client client;
    private final TimeValue defaultTimeout;
    private final WatcherSearchTemplateService searchTemplateService;

    public SearchInputFactory(
        Settings settings,
        Client client,
        NamedXContentRegistry xContentRegistry,
        Predicate<NodeFeature> clusterSupportsFeature,
        ScriptService scriptService
    ) {
        this.client = client;
        this.defaultTimeout = settings.getAsTime("xpack.watcher.input.search.default_timeout", TimeValue.timeValueMinutes(1));
        this.searchTemplateService = new WatcherSearchTemplateService(scriptService, xContentRegistry, clusterSupportsFeature);
    }

    @Override
    public String type() {
        return SearchInput.TYPE;
    }

    @Override
    public SearchInput parseInput(String watchId, XContentParser parser) throws IOException {
        return SearchInput.parse(watchId, parser);
    }

    @Override
    public ExecutableSearchInput createExecutable(SearchInput input) {
        return new ExecutableSearchInput(input, client, searchTemplateService, defaultTimeout);
    }
}
