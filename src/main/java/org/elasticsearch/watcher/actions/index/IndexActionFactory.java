/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.ActionFactory;
import org.elasticsearch.watcher.actions.email.ExecutableEmailAction;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;

import java.io.IOException;

/**
 *
 */
public class IndexActionFactory extends ActionFactory<IndexAction, IndexAction.Result, ExecutableIndexAction> {

    private final ClientProxy client;

    @Inject
    public IndexActionFactory(Settings settings, ClientProxy client) {
        super(Loggers.getLogger(ExecutableEmailAction.class, settings));
        this.client = client;
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
    public IndexAction.Result parseResult(Wid wid, String actionId, XContentParser parser) throws IOException {
        return IndexAction.Result.parse(wid.watchId(), actionId, parser);
    }

    @Override
    public ExecutableIndexAction createExecutable(IndexAction action) {
        return new ExecutableIndexAction(action, actionLogger, client);
    }
}
