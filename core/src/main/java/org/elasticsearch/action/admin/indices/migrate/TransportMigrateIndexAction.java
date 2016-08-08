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

package org.elasticsearch.action.admin.indices.migrate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class TransportMigrateIndexAction extends TransportMasterNodeAction<MigrateIndexRequest, MigrateIndexResponse> {
    /**
     * The running migrate actions on this node. All access to the map are synchronized on it.
     */
    private final Map<String, MigrateIndexTask> runningTasks = new HashMap<>();
    private final Client client;

    @Inject
    public TransportMigrateIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
        super(settings, MigrateIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                MigrateIndexRequest::new);
        this.client = client;
    }

    @Override
    protected String executor() {
        // Since this task might block while it synchronizes to coalesce running tasks we shouldn't run this on the listener thread pool.
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected MigrateIndexResponse newResponse() {
        return new MigrateIndexResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(MigrateIndexRequest request, ClusterState state) {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, true, request.indicesOptions().expandWildcardsOpen(),
                request.indicesOptions().expandWildcardsClosed());
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request.indices()));
    }

    /**
     * Get the running task to migrate to {@code destinationIndex} if there is one, null if there isn't one.
     */
    MigrateIndexTask getRunningTask(String destinationIndex) {
        synchronized (runningTasks) {
            return runningTasks.get(destinationIndex);
        }
    }

    @Override
    protected final void masterOperation(MigrateIndexRequest request, ClusterState state, ActionListener<MigrateIndexResponse> listener)
            throws Exception {
        throw new UnsupportedOperationException("Task required");
    }

    @Override
    protected final void masterOperation(Task t, MigrateIndexRequest request, ClusterState state,
            ActionListener<MigrateIndexResponse> listener) {
        MigrateIndexTask task = (MigrateIndexTask) t;
        task.setListener(listener);
        coalesceConcurrentRequestsToCreateSameIndex(task, state);
    }

    void coalesceConcurrentRequestsToCreateSameIndex(MigrateIndexTask task, ClusterState state) {
        synchronized (runningTasks) {
            MigrateIndexTask currentlyRunning = runningTasks.get(task.getRequest().getCreateIndexRequest().index());
            if (currentlyRunning != null) {
                currentlyRunning.addDuplicate(task);
                /* 
                 * Return early here because we've done all we need to do to handle this request - we'll get the response when
                 * currentlyRunning finishes.
                 */
                return;
            }
            // Add our task to the map so another migration for the same index can wait for this one to complete.
            runningTasks.put(task.getRequest().getCreateIndexRequest().index(), task);
            // Wrap the listener in one that'll fire all 
            ActionListener<MigrateIndexResponse> originalListener = task.getListener();
            task.setListener(new ActionListener<MigrateIndexResponse>() {
                @Override
                public void onResponse(MigrateIndexResponse response) {
                    sendResponse(task, originalListener, l -> l.onResponse(response));
                }

                @Override
                public void onFailure(Exception e) {
                    sendResponse(task, originalListener, l -> l.onFailure(e));
                }
            });
        }
        // Start the actual migration outside of the synchronized block - we don't need the lock again until we send the response.
        startMigration(task, state);
    }

    void sendResponse(MigrateIndexTask task, ActionListener<MigrateIndexResponse> originalListener,
            Consumer<ActionListener<MigrateIndexResponse>> send) {
        synchronized (runningTasks) {
            try {
                Executor executor = threadPool.executor(ThreadPool.Names.LISTENER); 
                executor.execute(() -> send.accept(originalListener));
                for (MigrateIndexTask duplicate : task.getDuplicates()) {
                    executor.execute(() -> send.accept(duplicate.getListener()));
                }
            } finally {
                MigrateIndexTask removed = runningTasks.remove(task.getRequest().getCreateIndexRequest().index());
                assert removed == task;
            }
        }
    }

    void startMigration(MigrateIndexTask task, ClusterState state) {
        
    }
}
