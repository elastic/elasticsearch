/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class TransportReindexAction extends HandledTransportAction<ReindexRequest, BulkByScrollResponse> {
    public static final Setting<List<String>> REMOTE_CLUSTER_WHITELIST =
            Setting.listSetting("reindex.remote.whitelist", emptyList(), Function.identity(), Property.NodeScope);

    private final ReindexValidator reindexValidator;
    private final Reindexer reindexer;

    @Inject
    public TransportReindexAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, ScriptService scriptService,
            AutoCreateIndex autoCreateIndex, Client client, TransportService transportService, ReindexSslConfig sslConfig) {
        super(ReindexAction.NAME, transportService, actionFilters, ReindexRequest::new);
        this.reindexValidator = new ReindexValidator(settings, clusterService, indexNameExpressionResolver, autoCreateIndex);
        this.reindexer = new Reindexer(clusterService, client, threadPool, scriptService, sslConfig);
    }

    @Override
    protected void doExecute(Task task, ReindexRequest request, ActionListener<BulkByScrollResponse> listener) {
        reindexValidator.initialValidation(request);
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        reindexer.initTask(bulkByScrollTask, request, new ActionListener<Void>() {
            @Override
            public void onResponse(Void v) {
                reindexer.execute(bulkByScrollTask, request, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
