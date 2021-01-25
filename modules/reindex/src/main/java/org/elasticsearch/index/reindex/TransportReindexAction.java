/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.function.Function;

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

public class TransportReindexAction extends HandledTransportAction<ReindexRequest, BulkByScrollResponse> {
    public static final Setting<List<String>> REMOTE_CLUSTER_WHITELIST =
            Setting.listSetting("reindex.remote.whitelist", emptyList(), Function.identity(), Property.NodeScope);

    protected final ReindexValidator reindexValidator;
    private final Reindexer reindexer;

    protected final Client client;

    @Inject
    public TransportReindexAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, ScriptService scriptService,
            AutoCreateIndex autoCreateIndex, Client client, TransportService transportService, ReindexSslConfig sslConfig) {
        this(ReindexAction.NAME, settings, threadPool, actionFilters, indexNameExpressionResolver, clusterService, scriptService,
            autoCreateIndex, client, transportService, sslConfig);
    }

    protected TransportReindexAction(String name, Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService,
                                     ScriptService scriptService, AutoCreateIndex autoCreateIndex, Client client,
                                     TransportService transportService, ReindexSslConfig sslConfig) {
        super(name, transportService, actionFilters, ReindexRequest::new);
        this.client = client;
        this.reindexValidator = new ReindexValidator(settings, clusterService, indexNameExpressionResolver, autoCreateIndex);
        this.reindexer = new Reindexer(clusterService, client, threadPool, scriptService, sslConfig);
    }

    @Override
    protected void doExecute(Task task, ReindexRequest request, ActionListener<BulkByScrollResponse> listener) {
        validate(request);
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        reindexer.initTask(bulkByScrollTask, request, new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                reindexer.execute(bulkByScrollTask, request, getBulkClient(), listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * This method can be overridden to specify a different {@link Client} to be used for indexing than that used for the search/input
     * part of the reindex. For example, a {@link org.elasticsearch.client.FilterClient} can be provided to transform bulk index requests
     * before they are fully performed.
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
