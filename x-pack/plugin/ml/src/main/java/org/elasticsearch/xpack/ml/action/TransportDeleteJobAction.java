/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteJobAction extends TransportMasterNodeAction<DeleteJobAction.Request, AcknowledgedResponse> {

    private static final int MAX_SNAPSHOTS_TO_DELETE = 10000;

    private final Client client;
    private final PersistentTasksService persistentTasksService;
    private final Auditor auditor;
    private final JobResultsProvider jobResultsProvider;

    /**
     * A map of task listeners by job_id.
     * Subsequent delete requests store their listeners in the corresponding list in this map
     * and wait to be notified when the first deletion task completes.
     * This is guarded by synchronizing on its lock.
     */
    private final Map<String, List<ActionListener<AcknowledgedResponse>>> listenersByJobId;

    @Inject
    public TransportDeleteJobAction(TransportService transportService, ClusterService clusterService,
                                    ThreadPool threadPool, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver, PersistentTasksService persistentTasksService,
                                    Client client, Auditor auditor, JobResultsProvider jobResultsProvider) {
        super(DeleteJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, DeleteJobAction.Request::new);
        this.client = client;
        this.persistentTasksService = persistentTasksService;
        this.auditor = auditor;
        this.jobResultsProvider = jobResultsProvider;
        this.listenersByJobId = new HashMap<>();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(DeleteJobAction.Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        throw new UnsupportedOperationException("the Task parameter is required");
    }

    @Override
    protected void masterOperation(Task task, DeleteJobAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        logger.debug("Deleting job '{}'", request.getJobId());

        JobManager.getJobOrThrowIfUnknown(request.getJobId(), state);

        TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, taskId);

        // Check if there is a deletion task for this job already and if yes wait for it to complete
        synchronized (listenersByJobId) {
            if (listenersByJobId.containsKey(request.getJobId())) {
                logger.debug("[{}] Deletion task [{}] will wait for existing deletion task to complete",
                        request.getJobId(), task.getId());
                listenersByJobId.get(request.getJobId()).add(listener);
                return;
            } else {
                List<ActionListener<AcknowledgedResponse>> listeners = new ArrayList<>();
                listeners.add(listener);
                listenersByJobId.put(request.getJobId(), listeners);
            }
        }

        auditor.info(request.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_DELETING, taskId));

        // The listener that will be executed at the end of the chain will notify all listeners
        ActionListener<AcknowledgedResponse> finalListener = ActionListener.wrap(
                ack -> notifyListeners(request.getJobId(), ack, null),
                e -> notifyListeners(request.getJobId(), null, e)
        );

        ActionListener<Boolean> markAsDeletingListener = ActionListener.wrap(
                response -> {
                    if (request.isForce()) {
                        forceDeleteJob(parentTaskClient, request, finalListener);
                    } else {
                        normalDeleteJob(parentTaskClient, request, finalListener);
                    }
                },
                e -> {
                    auditor.error(request.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_DELETING_FAILED, e.getMessage()));
                    finalListener.onFailure(e);
                });

        markJobAsDeleting(request.getJobId(), markAsDeletingListener, request.isForce());
    }

    private void notifyListeners(String jobId, @Nullable AcknowledgedResponse ack, @Nullable Exception error) {
        synchronized (listenersByJobId) {
            List<ActionListener<AcknowledgedResponse>> listeners = listenersByJobId.remove(jobId);
            if (listeners == null) {
                logger.error("[{}] No deletion job listeners could be found", jobId);
                return;
            }
            for (ActionListener<AcknowledgedResponse> listener : listeners) {
                if (error != null) {
                    listener.onFailure(error);
                } else {
                    listener.onResponse(ack);
                }
            }
        }
    }

    private void normalDeleteJob(ParentTaskAssigningClient parentTaskClient, DeleteJobAction.Request request,
                                 ActionListener<AcknowledgedResponse> listener) {
        String jobId = request.getJobId();

        // Step 4. When the job has been removed from the cluster state, return a response
        // -------
        CheckedConsumer<Boolean, Exception> apiResponseHandler = jobDeleted -> {
            if (jobDeleted) {
                logger.info("Job [" + jobId + "] deleted");
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DELETED));
                listener.onResponse(new AcknowledgedResponse(true));
            } else {
                listener.onResponse(new AcknowledgedResponse(false));
            }
        };

        // Step 3. When the physical storage has been deleted, remove from Cluster State
        // -------
        CheckedConsumer<Boolean, Exception> deleteJobStateHandler = response -> clusterService.submitStateUpdateTask(
                "delete-job-" + jobId,
                new AckedClusterStateUpdateTask<Boolean>(request, ActionListener.wrap(apiResponseHandler, listener::onFailure)) {

                    @Override
                    protected Boolean newResponse(boolean acknowledged) {
                        return acknowledged && response;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        MlMetadata currentMlMetadata = MlMetadata.getMlMetadata(currentState);
                        if (currentMlMetadata.getJobs().containsKey(jobId) == false) {
                            // We wouldn't have got here if the job never existed so
                            // the Job must have been deleted by another action.
                            // Don't error in this case
                            return currentState;
                        }

                        MlMetadata.Builder builder = new MlMetadata.Builder(currentMlMetadata);
                        builder.deleteJob(jobId, currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
                        return buildNewClusterState(currentState, builder);
                    }
                });


        // Step 2. Remove the job from any calendars
        CheckedConsumer<Boolean, Exception> removeFromCalendarsHandler = response -> jobResultsProvider.removeJobFromCalendars(jobId,
                ActionListener.wrap(deleteJobStateHandler::accept, listener::onFailure ));


        // Step 1. Delete the physical storage
        deleteJobDocuments(parentTaskClient, jobId, removeFromCalendarsHandler, listener::onFailure);
    }

    private void deleteJobDocuments(ParentTaskAssigningClient parentTaskClient, String jobId,
                                    CheckedConsumer<Boolean, Exception> finishedHandler, Consumer<Exception> failureHandler) {

        final String indexName = AnomalyDetectorsIndex.getPhysicalIndexFromState(clusterService.state(), jobId);
        final String indexPattern = indexName + "-*";

        final ActionListener<AcknowledgedResponse> completionHandler = ActionListener.wrap(
                response -> finishedHandler.accept(response.isAcknowledged()),
                failureHandler);

        // Step 7. If we did not drop the index and after DBQ state done, we delete the aliases
        ActionListener<BulkByScrollResponse> dbqHandler = ActionListener.wrap(
                bulkByScrollResponse -> {
                    if (bulkByScrollResponse == null) { // no action was taken by DBQ, assume Index was deleted
                        completionHandler.onResponse(new AcknowledgedResponse(true));
                    } else {
                        if (bulkByScrollResponse.isTimedOut()) {
                            logger.warn("[{}] DeleteByQuery for indices [{}, {}] timed out.", jobId, indexName, indexPattern);
                        }
                        if (!bulkByScrollResponse.getBulkFailures().isEmpty()) {
                            logger.warn("[{}] {} failures and {} conflicts encountered while running DeleteByQuery on indices [{}, {}].",
                                    jobId, bulkByScrollResponse.getBulkFailures().size(), bulkByScrollResponse.getVersionConflicts(),
                                    indexName, indexPattern);
                            for (BulkItemResponse.Failure failure : bulkByScrollResponse.getBulkFailures()) {
                                logger.warn("DBQ failure: " + failure);
                            }
                        }
                        deleteAliases(parentTaskClient, jobId, completionHandler);
                    }
                },
                failureHandler);

        // Step 6. If we did not delete the index, we run a delete by query
        ActionListener<Boolean> deleteByQueryExecutor = ActionListener.wrap(
                response -> {
                    if (response) {
                        logger.info("Running DBQ on [" + indexName + "," + indexPattern + "] for job [" + jobId + "]");
                        DeleteByQueryRequest request = new DeleteByQueryRequest(indexName, indexPattern);
                        ConstantScoreQueryBuilder query =
                                new ConstantScoreQueryBuilder(new TermQueryBuilder(Job.ID.getPreferredName(), jobId));
                        request.setQuery(query);
                        request.setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
                        request.setSlices(5);
                        request.setAbortOnVersionConflict(false);
                        request.setRefresh(true);

                        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, dbqHandler);
                    } else { // We did not execute DBQ, no need to delete aliases or check the response
                        dbqHandler.onResponse(null);
                    }
                },
                failureHandler);

        // Step 5. If we have any hits, that means we are NOT the only job on this index, and should not delete it
        // if we do not have any hits, we can drop the index and then skip the DBQ and alias deletion
        ActionListener<SearchResponse> customIndexSearchHandler = ActionListener.wrap(
                searchResponse -> {
                    if (searchResponse == null || searchResponse.getHits().getTotalHits().value > 0) {
                        deleteByQueryExecutor.onResponse(true); // We need to run DBQ and alias deletion
                    } else {
                        logger.info("Running DELETE Index on [" + indexName + "] for job [" + jobId + "]");
                        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
                        request.indicesOptions(IndicesOptions.lenientExpandOpen());
                        // If we have deleted the index, then we don't need to delete the aliases or run the DBQ
                        executeAsyncWithOrigin(
                                parentTaskClient.threadPool().getThreadContext(),
                                ML_ORIGIN,
                                request,
                                ActionListener.<AcknowledgedResponse>wrap(
                                        response -> deleteByQueryExecutor.onResponse(false), // skip DBQ && Alias
                                        failureHandler),
                                parentTaskClient.admin().indices()::delete);
                    }
                },
                failure -> {
                    if (failure.getClass() == IndexNotFoundException.class) { // assume the index is already deleted
                        deleteByQueryExecutor.onResponse(false); // skip DBQ && Alias
                    } else {
                        failureHandler.accept(failure);
                    }
                }
        );

        // Step 4. Determine if we are on a shared index by looking at `.ml-anomalies-shared` or the custom index's aliases
        ActionListener<Boolean> deleteCategorizerStateHandler = ActionListener.wrap(
                response -> {
                    if (indexName.equals(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX +
                            AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT)) {
                        //don't bother searching the index any further, we are on the default shared
                        customIndexSearchHandler.onResponse(null);
                    } else {
                        SearchSourceBuilder source = new SearchSourceBuilder()
                                .size(1)
                                .trackTotalHits(true)
                                .query(QueryBuilders.boolQuery().filter(
                                        QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))));

                        SearchRequest searchRequest = new SearchRequest(indexName);
                        searchRequest.source(source);
                        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, customIndexSearchHandler);
                    }
                },
                failureHandler
        );

        // Step 3. Delete quantiles done, delete the categorizer state
        ActionListener<Boolean> deleteQuantilesHandler = ActionListener.wrap(
                response -> deleteCategorizerState(parentTaskClient, jobId, 1, deleteCategorizerStateHandler),
                failureHandler);

        // Step 2. Delete state done, delete the quantiles
        ActionListener<BulkResponse> deleteStateHandler = ActionListener.wrap(
                bulkResponse -> deleteQuantiles(parentTaskClient, jobId, deleteQuantilesHandler),
                failureHandler);

        // Step 1. Delete the model state
        deleteModelState(parentTaskClient, jobId, deleteStateHandler);
    }

    private void deleteQuantiles(ParentTaskAssigningClient parentTaskClient, String jobId, ActionListener<Boolean> finishedHandler) {
        // The quantiles type and doc ID changed in v5.5 so delete both the old and new format
        DeleteByQueryRequest request = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobStateIndexName());
        // Just use ID here, not type, as trying to delete different types spams the logs with an exception stack trace
        IdsQueryBuilder query = new IdsQueryBuilder().addIds(Quantiles.documentId(jobId),
                // TODO: remove in 7.0
                Quantiles.v54DocumentId(jobId));
        request.setQuery(query);
        request.setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
        request.setAbortOnVersionConflict(false);
        request.setRefresh(true);

        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(
                response -> finishedHandler.onResponse(true),
                e -> {
                    // It's not a problem for us if the index wasn't found - it's equivalent to document not found
                    if (e instanceof IndexNotFoundException) {
                        finishedHandler.onResponse(true);
                    } else {
                        finishedHandler.onFailure(e);
                    }
                }));
    }

    private void deleteModelState(ParentTaskAssigningClient parentTaskClient, String jobId, ActionListener<BulkResponse> listener) {
        GetModelSnapshotsAction.Request request = new GetModelSnapshotsAction.Request(jobId, null);
        request.setPageParams(new PageParams(0, MAX_SNAPSHOTS_TO_DELETE));
        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, GetModelSnapshotsAction.INSTANCE, request, ActionListener.wrap(
                response -> {
                    List<ModelSnapshot> deleteCandidates = response.getPage().results();
                    JobDataDeleter deleter = new JobDataDeleter(parentTaskClient, jobId);
                    deleter.deleteModelSnapshots(deleteCandidates, listener);
                },
                listener::onFailure));
    }

    private void deleteCategorizerState(ParentTaskAssigningClient parentTaskClient, String jobId, int docNum,
                                        ActionListener<Boolean> finishedHandler) {
        // The categorizer state type and doc ID changed in v5.5 so delete both the old and new format
        DeleteByQueryRequest request = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobStateIndexName());
        // Just use ID here, not type, as trying to delete different types spams the logs with an exception stack trace
        IdsQueryBuilder query = new IdsQueryBuilder().addIds(CategorizerState.documentId(jobId, docNum),
                // TODO: remove in 7.0
                CategorizerState.v54DocumentId(jobId, docNum));
        request.setQuery(query);
        request.setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
        request.setAbortOnVersionConflict(false);
        request.setRefresh(true);

        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(
                response -> {
                    // If we successfully deleted a document try the next one; if not we're done
                    if (response.getDeleted() > 0) {
                        // There's an assumption here that there won't be very many categorizer
                        // state documents, so the recursion won't go more than, say, 5 levels deep
                        deleteCategorizerState(parentTaskClient, jobId, docNum + 1, finishedHandler);
                        return;
                    }
                    finishedHandler.onResponse(true);
                },
                e -> {
                    // It's not a problem for us if the index wasn't found - it's equivalent to document not found
                    if (e instanceof IndexNotFoundException) {
                        finishedHandler.onResponse(true);
                    } else {
                        finishedHandler.onFailure(e);
                    }
                }));
    }

    private void deleteAliases(ParentTaskAssigningClient parentTaskClient, String jobId,
                               ActionListener<AcknowledgedResponse> finishedHandler) {
        final String readAliasName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        final String writeAliasName = AnomalyDetectorsIndex.resultsWriteAlias(jobId);

        // first find the concrete indices associated with the aliases
        GetAliasesRequest aliasesRequest = new GetAliasesRequest().aliases(readAliasName, writeAliasName)
                .indicesOptions(IndicesOptions.lenientExpandOpen());
        executeAsyncWithOrigin(parentTaskClient.threadPool().getThreadContext(), ML_ORIGIN, aliasesRequest,
                ActionListener.<GetAliasesResponse>wrap(
                        getAliasesResponse -> {
                            // remove the aliases from the concrete indices found in the first step
                            IndicesAliasesRequest removeRequest = buildRemoveAliasesRequest(getAliasesResponse);
                            if (removeRequest == null) {
                                // don't error if the job's aliases have already been deleted - carry on and delete the
                                // rest of the job's data
                                finishedHandler.onResponse(new AcknowledgedResponse(true));
                                return;
                            }
                            executeAsyncWithOrigin(parentTaskClient.threadPool().getThreadContext(), ML_ORIGIN, removeRequest,
                                    ActionListener.<AcknowledgedResponse>wrap(
                                            finishedHandler::onResponse,
                                            finishedHandler::onFailure),
                                    parentTaskClient.admin().indices()::aliases);
                        },
                        finishedHandler::onFailure), parentTaskClient.admin().indices()::getAliases);
    }

    private IndicesAliasesRequest buildRemoveAliasesRequest(GetAliasesResponse getAliasesResponse) {
        Set<String> aliases = new HashSet<>();
        List<String> indices = new ArrayList<>();
        for (ObjectObjectCursor<String, List<AliasMetaData>> entry : getAliasesResponse.getAliases()) {
            // The response includes _all_ indices, but only those associated with
            // the aliases we asked about will have associated AliasMetaData
            if (entry.value.isEmpty() == false) {
                indices.add(entry.key);
                entry.value.forEach(metadata -> aliases.add(metadata.getAlias()));
            }
        }
        return aliases.isEmpty() ? null : new IndicesAliasesRequest().addAliasAction(
                IndicesAliasesRequest.AliasActions.remove()
                        .aliases(aliases.toArray(new String[aliases.size()]))
                        .indices(indices.toArray(new String[indices.size()])));
    }

    private void forceDeleteJob(ParentTaskAssigningClient parentTaskClient, DeleteJobAction.Request request,
                                ActionListener<AcknowledgedResponse> listener) {

        logger.debug("Force deleting job [{}]", request.getJobId());

        final ClusterState state = clusterService.state();
        final String jobId = request.getJobId();

        // 3. Delete the job
        ActionListener<Boolean> removeTaskListener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean response) {
                normalDeleteJob(parentTaskClient, request, listener);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ResourceNotFoundException) {
                    normalDeleteJob(parentTaskClient, request, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        };

        // 2. Cancel the persistent task. This closes the process gracefully so
        // the process should be killed first.
        ActionListener<KillProcessAction.Response> killJobListener = ActionListener.wrap(
                response -> removePersistentTask(request.getJobId(), state, removeTaskListener),
                e -> {
                    if (e instanceof ElasticsearchStatusException) {
                        // Killing the process marks the task as completed so it
                        // may have disappeared when we get here
                        removePersistentTask(request.getJobId(), state, removeTaskListener);
                    } else {
                        listener.onFailure(e);
                    }
                }
        );

        // 1. Kill the job's process
        killProcess(parentTaskClient, jobId, killJobListener);
    }

    private void killProcess(ParentTaskAssigningClient parentTaskClient, String jobId,
                             ActionListener<KillProcessAction.Response> listener) {
        KillProcessAction.Request killRequest = new KillProcessAction.Request(jobId);
        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, KillProcessAction.INSTANCE, killRequest, listener);
    }

    private void removePersistentTask(String jobId, ClusterState currentState,
                                      ActionListener<Boolean> listener) {
        PersistentTasksCustomMetaData tasks = currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
        if (jobTask == null) {
            listener.onResponse(null);
        } else {
            persistentTasksService.sendRemoveRequest(jobTask.getId(),
                    new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                        @Override
                        public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> task) {
                            listener.onResponse(Boolean.TRUE);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
        }
    }

    private void markJobAsDeleting(String jobId, ActionListener<Boolean> listener, boolean force) {
        clusterService.submitStateUpdateTask("mark-job-as-deleted", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                PersistentTasksCustomMetaData tasks = currentState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
                MlMetadata.Builder builder = new MlMetadata.Builder(MlMetadata.getMlMetadata(currentState));
                builder.markJobAsDeleting(jobId, tasks, force);
                return buildNewClusterState(currentState, builder);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.debug("Job [" + jobId + "] is successfully marked as deleted");
                listener.onResponse(true);
            }
        });
    }

    static boolean jobIsDeletedFromState(String jobId, ClusterState clusterState) {
        return !MlMetadata.getMlMetadata(clusterState).getJobs().containsKey(jobId);
    }

    private static ClusterState buildNewClusterState(ClusterState currentState, MlMetadata.Builder builder) {
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, builder.build()).build());
        return newState.build();
    }
}
