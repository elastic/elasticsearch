/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.index;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;

import java.io.IOException;

public class IndexActionFactory extends ActionFactory {

    private final WatcherClientProxy client;
    private final TimeValue defaultTimeout;

    public IndexActionFactory(Settings settings, InternalClient client) {
        this(settings, new WatcherClientProxy(settings, client));
    }

    public IndexActionFactory(Settings settings, WatcherClientProxy client ) {
        super(Loggers.getLogger(IndexActionFactory.class, settings));
        this.client = client;
        this.defaultTimeout = settings.getAsTime("xpack.watcher.actions.index.default_timeout", null);
    }

    @Override
    public ExecutableIndexAction parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        return new ExecutableIndexAction(IndexAction.parse(watchId, actionId, parser), actionLogger, client, defaultTimeout);
    }
}
