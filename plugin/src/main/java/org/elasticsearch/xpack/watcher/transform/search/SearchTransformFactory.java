/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transform.search;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.transform.TransformFactory;

import java.io.IOException;

public class SearchTransformFactory extends TransformFactory<SearchTransform, SearchTransform.Result, ExecutableSearchTransform> {
    protected final WatcherClientProxy client;
    private final TimeValue defaultTimeout;
    private final WatcherSearchTemplateService searchTemplateService;

    public SearchTransformFactory(Settings settings, InternalClient client, NamedXContentRegistry xContentRegistry,
            ScriptService scriptService) {
        this(settings, new WatcherClientProxy(settings, client), xContentRegistry, scriptService);
    }

    public SearchTransformFactory(Settings settings, WatcherClientProxy client, NamedXContentRegistry xContentRegistry,
            ScriptService scriptService) {
        super(Loggers.getLogger(ExecutableSearchTransform.class, settings));
        this.client = client;
        this.defaultTimeout = settings.getAsTime("xpack.watcher.transform.search.default_timeout", null);
        this.searchTemplateService = new WatcherSearchTemplateService(settings, scriptService, xContentRegistry);
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
