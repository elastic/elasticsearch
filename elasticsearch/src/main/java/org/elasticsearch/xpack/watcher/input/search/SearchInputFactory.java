/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.input.search;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.input.InputFactory;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;

import java.io.IOException;

public class SearchInputFactory extends InputFactory<SearchInput, SearchInput.Result, ExecutableSearchInput> {

    private final Settings settings;
    private final WatcherClientProxy client;
    private final TimeValue defaultTimeout;
    private final SearchRequestParsers searchRequestParsers;
    private final ParseFieldMatcher parseFieldMatcher;
    private final WatcherSearchTemplateService searchTemplateService;

    public SearchInputFactory(Settings settings, InternalClient client,
                              SearchRequestParsers searchRequestParsers, ScriptService scriptService) {
        this(settings, new WatcherClientProxy(settings, client), searchRequestParsers, scriptService);
    }

    public SearchInputFactory(Settings settings, WatcherClientProxy client,
                              SearchRequestParsers searchRequestParsers, ScriptService scriptService) {
        super(Loggers.getLogger(ExecutableSimpleInput.class, settings));
        this.settings = settings;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.client = client;
        this.searchRequestParsers = searchRequestParsers;
        this.defaultTimeout = settings.getAsTime("xpack.watcher.input.search.default_timeout", null);
        this.searchTemplateService = new WatcherSearchTemplateService(settings, scriptService, searchRequestParsers);
    }

    @Override
    public String type() {
        return SearchInput.TYPE;
    }

    @Override
    public SearchInput parseInput(String watchId, XContentParser parser, boolean upgradeInputSource) throws IOException {
        String defaultLegacyScriptLanguage = ScriptSettings.getLegacyDefaultLang(settings);
        return SearchInput.parse(inputLogger, watchId, parser, upgradeInputSource, defaultLegacyScriptLanguage,
                parseFieldMatcher, searchRequestParsers);
    }

    @Override
    public ExecutableSearchInput createExecutable(SearchInput input) {
        return new ExecutableSearchInput(input, inputLogger, client, searchTemplateService, defaultTimeout);
    }
}
