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
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.transform.TransformFactory;

import java.io.IOException;

/**
 *
 */
public class SearchTransformFactory extends TransformFactory<SearchTransform, SearchTransform.Result, ExecutableSearchTransform> {

    protected final ClientProxy client;
    private final TimeValue defaultTimeout;
    private IndicesQueriesRegistry queryRegistry;

    @Inject
    public SearchTransformFactory(Settings settings, ClientProxy client, IndicesQueriesRegistry queryRegistry) {
        super(Loggers.getLogger(ExecutableSearchTransform.class, settings));
        this.client = client;
        this.queryRegistry = queryRegistry;
        this.defaultTimeout = settings.getAsTime("watcher.transform.search.default_timeout", null);
    }

    @Override
    public String type() {
        return SearchTransform.TYPE;
    }

    @Override
    public SearchTransform parseTransform(String watchId, XContentParser parser) throws IOException {
        QueryParseContext context = new QueryParseContext(queryRegistry);
        context.reset(parser);
        return SearchTransform.parse(watchId, parser, context);
    }

    @Override
    public ExecutableSearchTransform createExecutable(SearchTransform transform) {
        return new ExecutableSearchTransform(transform, transformLogger, client, defaultTimeout);
    }
}
