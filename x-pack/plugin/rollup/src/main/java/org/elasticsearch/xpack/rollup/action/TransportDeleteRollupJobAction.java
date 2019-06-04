/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;

import java.util.List;
import java.util.Map;

public class TransportDeleteRollupJobAction extends TransportTasksAction<RollupJobTask, DeleteRollupJobAction.Request,
    DeleteRollupJobAction.Response, DeleteRollupJobAction.Response> {

    private final Client client;
    private static final Logger logger = LogManager.getLogger(TransportDeleteRollupJobAction.class);

    @Inject
    public TransportDeleteRollupJobAction(TransportService transportService, ActionFilters actionFilters,
                                          ClusterService clusterService, Client client) {
        super(DeleteRollupJobAction.NAME, clusterService, transportService, actionFilters, DeleteRollupJobAction.Request::new,
            DeleteRollupJobAction.Response::new, DeleteRollupJobAction.Response::new, ThreadPool.Names.SAME);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, DeleteRollupJobAction.Request request, ActionListener<DeleteRollupJobAction.Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();

        if (nodes.isLocalNodeElectedMaster()) {
            PersistentTasksCustomMetaData pTasksMeta = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            if (pTasksMeta != null && pTasksMeta.getTask(request.getId()) != null) {
                super.doExecute(task, request, wrapListener(request, listener, clusterService, client));
            } else {
                // If we couldn't find the job in the persistent task CS, it means it was deleted prior to this call,
                // no need to go looking for the allocated task
                listener.onFailure(new ResourceNotFoundException("the task with id [" + request.getId() + "] doesn't exist"));
            }

        } else {
            // Delegates DeleteJob to elected master node, so it becomes the coordinating node.
            // Non-master nodes may have a stale cluster state that shows jobs which are cancelled
            // on the master, which makes testing difficult.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException("no known master nodes"));
            } else {
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                    new ActionListenerResponseHandler<>(listener, DeleteRollupJobAction.Response::new));
            }
        }
    }

    /**
     * If the user wants to delete the data, we need to chain extra steps: DBQ the data, then clean up _meta
     */
    static ActionListener<DeleteRollupJobAction.Response> wrapListener(DeleteRollupJobAction.Request request,
                                                                       ActionListener<DeleteRollupJobAction.Response> listener,
                                                                       ClusterService clusterService, Client client) {
        if (request.shouldDeleteData()) {
            return ActionListener.wrap(response -> {
                // If we successfully stopped the job, and deleted the data, we can clean up
                // the _meta config
                if (response.isDeleted()) {
                    // We're back on the master, so get the cluster state directly and update mappings
                    updateMetadata(request.getId(), clusterService.state().getMetaData().getIndices(), client, listener);
                } else {
                    throw new RuntimeException("Encountered errors while deleting job ["
                        + request.getId() + "], not removing config from _meta mapping.");
                }
            }, listener::onFailure);
        }
        return listener;
    }

    static void updateMetadata(String jobId, ImmutableOpenMap<String, IndexMetaData> indices,
                               Client client, ActionListener<DeleteRollupJobAction.Response> listener) {
        for (ObjectObjectCursor<String, IndexMetaData> entry : indices) {
            RollupJobCaps cap = getCap(jobId, entry.key, entry.value);
            if (cap != null) {
                // This is the index we're interested in.
                // Find and remove the job config from the mapping, then put it back
                Map<String, Object> mapping = entry.value.mapping().getSourceAsMap();
                @SuppressWarnings("unchecked")
                Map<String, Object> rollupJobs = (Map<String, Object>) ((Map) mapping.get("_meta")).get("_rollup");
                rollupJobs.remove(jobId);

                putNewMetadata(cap.getRollupIndex(), mapping, client, listener);
                return;
            }
        }
        throw new IllegalStateException("Could not find config for job [" + jobId + "] in metadata when deleting.");
    }

    private static RollupJobCaps getCap(String jobId, String indexName, IndexMetaData indexMetaData) {
        return TransportGetRollupCapsAction.findRollupIndexCaps(indexName, indexMetaData)
            .map(rollupIndexCaps -> rollupIndexCaps.getJobByID(jobId))
            .orElse(null);
    }

    private static void putNewMetadata(String indexName, Map<String, Object> newMapping, Client client,
                                       ActionListener<DeleteRollupJobAction.Response> listener) {
        PutMappingRequest request = new PutMappingRequest(indexName);
        request.type(RollupField.TYPE_NAME);
        request.source(newMapping);
        client.execute(PutMappingAction.INSTANCE, request, ActionListener.wrap(acknowledgedResponse -> {
            if (acknowledgedResponse.isAcknowledged()) {
                // Finally, declare success
                listener.onResponse(new DeleteRollupJobAction.Response(true));
            } else {
                throw new RuntimeException("Attempt to remove job from _meta config was not acknowledged");
            }
        }, listener::onFailure));
    }

    @Override
    protected void taskOperation(DeleteRollupJobAction.Request request, RollupJobTask jobTask,
                                 ActionListener<DeleteRollupJobAction.Response> listener) {

        assert jobTask.getConfig().getId().equals(request.getId());
        IndexerState state = ((RollupJobStatus) jobTask.getStatus()).getIndexerState();
        if (state.equals(IndexerState.STOPPED) ) {
            jobTask.onCancelled();
            if (request.shouldDeleteData()) {
                deleteDataFromIndex(jobTask.getConfig(), listener, client);
            } else {
                // User doesn't want to delete data, so we can return success now
                listener.onResponse(new DeleteRollupJobAction.Response(true));
            }
        } else {
            listener.onFailure(new IllegalStateException("Could not delete job [" + request.getId() + "] because " +
                "indexer state is [" + state + "].  Job must be [" + IndexerState.STOPPED + "] before deletion."));
        }
    }

    @Override
    protected DeleteRollupJobAction.Response newResponse(DeleteRollupJobAction.Request request, List<DeleteRollupJobAction.Response> tasks,
                                                         List<TaskOperationFailure> taskOperationFailures,
                                                         List<FailedNodeException> failedNodeExceptions) {
        // There should theoretically only be one task running the rollup job
        // If there are more, in production it should be ok as long as they are acknowledge shutting down.
        // But in testing we'd like to know there were more than one hence the assert
        assert tasks.size() + taskOperationFailures.size() == 1;
        boolean cancelled = tasks.size() > 0 && tasks.stream().allMatch(DeleteRollupJobAction.Response::isDeleted);
        return new DeleteRollupJobAction.Response(cancelled, taskOperationFailures, failedNodeExceptions);
    }

    static void deleteDataFromIndex(RollupJobConfig config, ActionListener<DeleteRollupJobAction.Response> listener, Client client) {

        ActionListener<BulkByScrollResponse> dbqListener = ActionListener.wrap(response -> {
            if (response.isTimedOut()) {
                String msg = "DeleteByQuery on index [" + config.getRollupIndex()
                    + "] for job [" + config.getId() + "] timed out after " + response.getTook().getStringRep();
                logger.error(msg);
                listener.onFailure(new ElasticsearchTimeoutException(msg));
                return;
            }

            if (response.getBulkFailures().size() > 0) {
                logger.warn("Encountered bulk failures while running DeleteByQuery on index [" + config.getRollupIndex() + "]");
            }
            if (response.getSearchFailures().size() > 0) {
                logger.warn("Encountered search failures while running DeleteByQuery on index [" + config.getRollupIndex() + "]");
            }
            listener.onResponse(new DeleteRollupJobAction.Response(true));
        }, listener::onFailure);

        DeleteByQueryRequest dbq = new DeleteByQueryRequest(config.getRollupIndex());
        QueryBuilder builder = new BoolQueryBuilder()
            .filter(new TermQueryBuilder(RollupField.ROLLUP_META + "." + RollupField.ID.getPreferredName(), config.getId()));
        dbq.setQuery(builder);
        dbq.setConflicts("proceed");
        client.execute(DeleteByQueryAction.INSTANCE, dbq, dbqListener);
    }

}
