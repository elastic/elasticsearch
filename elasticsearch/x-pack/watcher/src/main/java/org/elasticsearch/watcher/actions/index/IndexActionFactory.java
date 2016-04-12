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
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;

import java.io.IOException;

/**
 *
 */
public class IndexActionFactory extends ActionFactory<IndexAction, ExecutableIndexAction> {

    private final WatcherClientProxy client;
    private final TimeValue defaultTimeout;

    @Inject
    public IndexActionFactory(Settings settings, WatcherClientProxy client) {
        super(Loggers.getLogger(ExecutableEmailAction.class, settings));
        this.client = client;
        this.defaultTimeout = settings.getAsTime("xpack.watcher.actions.index.default_timeout", null);
    }

    @Override
    public String type() {
        return IndexAction.TYPE;
    }

    @Override
    public IndexAction parseAction(String watchId, String actionId, XContentParser parser) throws IOException {
        return IndexAction.parse(watchId, actionId, parser);
    }

    @Override
    public ExecutableIndexAction createExecutable(IndexAction action) {
        return new ExecutableIndexAction(action, actionLogger, client, defaultTimeout);
    }
}
