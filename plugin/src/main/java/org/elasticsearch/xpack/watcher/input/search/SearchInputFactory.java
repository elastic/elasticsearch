/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.input.search;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.input.InputFactory;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;

import java.io.IOException;

public class SearchInputFactory extends InputFactory<SearchInput, SearchInput.Result, ExecutableSearchInput> {
    private final WatcherClientProxy client;
    private final TimeValue defaultTimeout;
    private final WatcherSearchTemplateService searchTemplateService;

    public SearchInputFactory(Settings settings, InternalClient client, NamedXContentRegistry xContentRegistry,
            ScriptService scriptService) {
        this(settings, new WatcherClientProxy(settings, client), xContentRegistry, scriptService);
    }

    public SearchInputFactory(Settings settings, WatcherClientProxy client, NamedXContentRegistry xContentRegistry,
            ScriptService scriptService) {
        super(Loggers.getLogger(ExecutableSimpleInput.class, settings));
        this.client = client;
        this.defaultTimeout = settings.getAsTime("xpack.watcher.input.search.default_timeout", null);
        this.searchTemplateService = new WatcherSearchTemplateService(settings, scriptService, xContentRegistry);
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
        return new ExecutableSearchInput(input, inputLogger, client, searchTemplateService, defaultTimeout);
    }
}
