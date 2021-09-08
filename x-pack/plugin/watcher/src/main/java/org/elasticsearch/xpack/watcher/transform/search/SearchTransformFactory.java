/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transform.search;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.watcher.transform.TransformFactory;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;

import java.io.IOException;

public class SearchTransformFactory extends TransformFactory<SearchTransform, SearchTransform.Result, ExecutableSearchTransform> {

    private final Client client;
    private final TimeValue defaultTimeout;
    private final WatcherSearchTemplateService searchTemplateService;

    public SearchTransformFactory(Settings settings, Client client, NamedXContentRegistry xContentRegistry, ScriptService scriptService) {
        super(LogManager.getLogger(ExecutableSearchTransform.class));
        this.client = client;
        this.defaultTimeout = settings.getAsTime("xpack.watcher.transform.search.default_timeout", TimeValue.timeValueMinutes(1));
        this.searchTemplateService = new WatcherSearchTemplateService(scriptService, xContentRegistry);
    }

    @Override
    public String type() {
        return SearchTransform.TYPE;
    }

    @Override
    public SearchTransform parseTransform(String watchId, XContentParser parser) throws IOException {
        return SearchTransform.parse(watchId, parser);
    }

    @Override
    public ExecutableSearchTransform createExecutable(SearchTransform transform) {
        return new ExecutableSearchTransform(transform, transformLogger, client,  searchTemplateService, defaultTimeout);
    }
}
