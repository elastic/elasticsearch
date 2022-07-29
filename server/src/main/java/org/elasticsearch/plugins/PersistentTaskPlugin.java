/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugins;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;

/**
 * Plugin for registering persistent tasks executors.
 */
public interface PersistentTaskPlugin {

    /**
     * Returns additional persistent tasks executors added by this plugin.
     */
    default List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return Collections.emptyList();
    }

}
