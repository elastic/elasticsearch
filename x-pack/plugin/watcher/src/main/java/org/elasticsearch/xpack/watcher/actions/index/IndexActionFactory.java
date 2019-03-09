/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.index;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.ActionFactory;

import java.io.IOException;

public class IndexActionFactory extends ActionFactory {

    private final Client client;
    private final TimeValue indexDefaultTimeout;
    private final TimeValue bulkDefaultTimeout;

    public IndexActionFactory(Settings settings, Client client) {
        super(LogManager.getLogger(IndexActionFactory.class));
        this.client = client;
        this.indexDefaultTimeout = settings.getAsTime("xpack.watcher.actions.index.default_timeout", TimeValue.timeValueSeconds(30));
        this.bulkDefaultTimeout = settings.getAsTime("xpack.watcher.actions.bulk.default_timeout", TimeValue.timeValueMinutes(1));
    }

    @Override
    public ExecutableIndexAction parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        return new ExecutableIndexAction(IndexAction.parse(watchId, actionId, parser), actionLogger, client,
                indexDefaultTimeout, bulkDefaultTimeout);
    }
}
