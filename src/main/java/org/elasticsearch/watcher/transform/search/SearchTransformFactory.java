/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.search;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.DynamicIndexName;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.transform.TransformFactory;

import java.io.IOException;

/**
 *
 */
public class SearchTransformFactory extends TransformFactory<SearchTransform, SearchTransform.Result, ExecutableSearchTransform> {

    protected final ClientProxy client;
    protected final DynamicIndexName.Parser indexNameParser;
    private final TimeValue defaultTimeout;

    @Inject
    public SearchTransformFactory(Settings settings, ClientProxy client) {
        super(Loggers.getLogger(ExecutableSearchTransform.class, settings));
        this.client = client;
        String defaultDateFormat = DynamicIndexName.defaultDateFormat(settings, "watcher.transform.search");
        this.indexNameParser = new DynamicIndexName.Parser(defaultDateFormat);
        this.defaultTimeout = settings.getAsTime("watcher.transform.search.default_timeout", TimeValue.timeValueSeconds(30));
    }

    @Override
    public String type() {
        return SearchTransform.TYPE;
    }

    @Override
    public SearchTransform parseTransform(String watchId, XContentParser parser) throws IOException {
        return SearchTransform.parse(watchId, parser, defaultTimeout);
    }

    @Override
    public ExecutableSearchTransform createExecutable(SearchTransform transform) {
        return new ExecutableSearchTransform(transform, transformLogger, client, indexNameParser);
    }
}
