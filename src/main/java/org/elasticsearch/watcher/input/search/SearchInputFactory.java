/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.input.InputFactory;
import org.elasticsearch.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;

import java.io.IOException;

/**
 *
 */
public class SearchInputFactory extends InputFactory<SearchInput, SearchInput.Result, ExecutableSearchInput> {

    private final ScriptServiceProxy scriptService;
    private final ClientProxy client;

    @Inject
    public SearchInputFactory(Settings settings, ScriptServiceProxy scriptService, ClientProxy client) {
        super(Loggers.getLogger(ExecutableSimpleInput.class, settings));
        this.scriptService = scriptService;
        this.client = client;
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
    public SearchInput.Result parseResult(String watchId, XContentParser parser) throws IOException {
        return SearchInput.Result.parse(watchId, parser);
    }

    @Override
    public ExecutableSearchInput createExecutable(SearchInput input) {
        return new ExecutableSearchInput(input, inputLogger, scriptService, client);
    }
}
