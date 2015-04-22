/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.search;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.transform.TransformFactory;

import java.io.IOException;

/**
 *
 */
public class SearchTransformFactory extends TransformFactory<SearchTransform, SearchTransform.Result, ExecutableSearchTransform> {

    protected final ScriptServiceProxy scriptService;
    protected final ClientProxy client;

    @Inject
    public SearchTransformFactory(Settings settings, ScriptServiceProxy scriptService, ClientProxy client) {
        super(Loggers.getLogger(ExecutableSearchTransform.class, settings));
        this.scriptService = scriptService;
        this.client = client;
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
    public SearchTransform.Result parseResult(String watchId, XContentParser parser) throws IOException {
        return SearchTransform.Result.parse(watchId, parser);
    }

    @Override
    public ExecutableSearchTransform createExecutable(SearchTransform transform) {
        return new ExecutableSearchTransform(transform, transformLogger, scriptService, client);
    }
}
