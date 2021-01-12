/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.reindex.ReindexSslConfig;
import org.elasticsearch.index.reindex.TransportReindexAction;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;

import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;

public class TransportEnrichReindexAction extends TransportReindexAction {

    private final Client bulkClient;

    @Inject
    public TransportEnrichReindexAction(
        Settings settings,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ScriptService scriptService,
        AutoCreateIndex autoCreateIndex,
        Client client,
        TransportService transportService,
        Environment environment,
        ResourceWatcherService watcherService
    ) {
        super(
            EnrichReindexAction.NAME,
            settings,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            clusterService,
            scriptService,
            autoCreateIndex,
            client,
            transportService,
            new ReindexSslConfig(settings, environment, watcherService)
        );
        this.bulkClient = new OriginSettingClient(client, ENRICH_ORIGIN);
    }

    @Override
    protected Client getBulkClient() {
        return bulkClient;
    }
}
