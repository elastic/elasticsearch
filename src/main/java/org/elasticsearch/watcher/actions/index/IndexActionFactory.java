/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.ActionFactory;
import org.elasticsearch.watcher.actions.email.ExecutableEmailAction;
import org.elasticsearch.watcher.support.DynamicIndexName;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;

import java.io.IOException;

/**
 *
 */
public class IndexActionFactory extends ActionFactory<IndexAction, ExecutableIndexAction> {

    private final ClientProxy client;
    private final DynamicIndexName.Parser indexNamesParser;
    private final TimeValue defaultTimeout;

    @Inject
    public IndexActionFactory(Settings settings, ClientProxy client) {
        super(Loggers.getLogger(ExecutableEmailAction.class, settings));
        this.client = client;
        String defaultDateFormat = DynamicIndexName.defaultDateFormat(settings, "watcher.actions.index");
        this.indexNamesParser = new DynamicIndexName.Parser(defaultDateFormat);
        this.defaultTimeout = settings.getAsTime("watcher.action.index.default_timeout", TimeValue.timeValueSeconds(60));
    }

    @Override
    public String type() {
        return IndexAction.TYPE;
    }

    @Override
    public IndexAction parseAction(String watchId, String actionId, XContentParser parser) throws IOException {
        return IndexAction.parse(watchId, actionId, parser, defaultTimeout);
    }

    @Override
    public ExecutableIndexAction createExecutable(IndexAction action) {
        return new ExecutableIndexAction(action, actionLogger, client, indexNamesParser);
    }
}
