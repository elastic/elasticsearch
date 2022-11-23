/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class TransportReindexAction extends HandledTransportAction<ReindexRequest, BulkByScrollResponse> {
    public static final Setting<List<String>> REMOTE_CLUSTER_WHITELIST = Setting.listSetting(
        "reindex.remote.whitelist",
        emptyList(),
        Function.identity(),
        Property.NodeScope
    );

    protected final ReindexValidator reindexValidator;
    private final Reindexer reindexer;

    protected final Client client;

    @Inject
    public TransportReindexAction(
        Settings settings,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ScriptService scriptService,
        AutoCreateIndex autoCreateIndex,
        Client client,
        TransportService transportService,
        ReindexSslConfig sslConfig
    ) {
        this(
            ReindexAction.NAME,
            settings,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            clusterService,
            scriptService,
            autoCreateIndex,
            client,
            transportService,
            sslConfig
        );
    }

    protected TransportReindexAction(
        String name,
        Settings settings,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ScriptService scriptService,
        AutoCreateIndex autoCreateIndex,
        Client client,
        TransportService transportService,
        ReindexSslConfig sslConfig
    ) {
        super(name, transportService, actionFilters, ReindexRequest::new);
        this.client = client;
        this.reindexValidator = new ReindexValidator(settings, clusterService, indexNameExpressionResolver, autoCreateIndex);
        this.reindexer = new Reindexer(clusterService, client, threadPool, scriptService, sslConfig);
    }

    @Override
    protected void doExecute(Task task, ReindexRequest request, ActionListener<BulkByScrollResponse> listener) {
        validate(request);
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        reindexer.initTask(
            bulkByScrollTask,
            request,
            listener.delegateFailure((l, v) -> reindexer.execute(bulkByScrollTask, request, getBulkClient(), l))
        );
    }

    /**
     * This method can be overridden to specify a different {@link Client} to be used for indexing than that used for the search/input
     * part of the reindex. For example, a {@link org.elasticsearch.client.internal.FilterClient} can be provided to transform bulk index
     * requests before they are fully performed.
     */
    protected Client getBulkClient() {
        return client;
    }

    /**
     * This method can be overridden if different than usual validation is needed. This method should throw an exception if validation
     * fails.
     */
    protected void validate(ReindexRequest request) {
        reindexValidator.initialValidation(request);
    }
}
