/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ReindexSslConfig;
import org.elasticsearch.index.reindex.TransportReindexAction;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;

import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;

/**
 * A specialized version of {@link TransportReindexAction} which performs the search part of the reindex in the security context of the
 * current user, but the indexing part of the reindex in the security context of the Enrich plugin. This is necessary as Enrich indices are
 * protected system indices, and typically cannot be accessed directly by users.
 */
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

    @Override
    protected void validate(ReindexRequest request) {
        // Validate the request in Enrich's security context, to be sure the validation can access system indices
        try (ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashWithOrigin(ENRICH_ORIGIN)) {
            reindexValidator.initialValidation(request);
        }
    }
}
