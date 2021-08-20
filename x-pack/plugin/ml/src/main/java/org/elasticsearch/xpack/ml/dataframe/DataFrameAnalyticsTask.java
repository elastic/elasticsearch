/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.license.LicensedAllocatedPersistentTask;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.stats.ProgressTracker;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsHolder;
import org.elasticsearch.xpack.ml.dataframe.steps.DataFrameAnalyticsStep;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.MlParserUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class DataFrameAnalyticsTask extends LicensedAllocatedPersistentTask implements StartDataFrameAnalyticsAction.TaskMatcher {

    private static final Logger LOGGER = LogManager.getLogger(DataFrameAnalyticsTask.class);

    private final Client client;
    private final DataFrameAnalyticsManager analyticsManager;
    private final DataFrameAnalyticsAuditor auditor;
    private final StartDataFrameAnalyticsAction.TaskParams taskParams;
    private volatile boolean isStopping;
    private volatile boolean isMarkAsCompletedCalled;
    private volatile StatsHolder statsHolder;
    private volatile DataFrameAnalyticsStep currentStep;

    public DataFrameAnalyticsTask(long id, String type, String action, TaskId parentTask, Map<String, String> headers,
                                  Client client, DataFrameAnalyticsManager analyticsManager, DataFrameAnalyticsAuditor auditor,
                                  StartDataFrameAnalyticsAction.TaskParams taskParams, XPackLicenseState licenseState) {
        super(
            id,
            type,
            action,
            MlTasks.DATA_FRAME_ANALYTICS_TASK_ID_PREFIX + taskParams.getId(),
            parentTask,
            headers,
            MachineLearning.ML_ANALYTICS_JOBS_FEATURE,
            MlTasks.DATA_FRAME_ANALYTICS_TASK_ID_PREFIX + taskParams.getId(),
            licenseState
        );
        this.client = new ParentTaskAssigningClient(Objects.requireNonNull(client), parentTask);
        this.analyticsManager = Objects.requireNonNull(analyticsManager);
        this.auditor = Objects.requireNonNull(auditor);
        this.taskParams = Objects.requireNonNull(taskParams);
    }

    public void setStep(DataFrameAnalyticsStep step) {
        currentStep = step;
    }

    public StartDataFrameAnalyticsAction.TaskParams getParams() {
        return taskParams;
    }

    public boolean isStopping() {
        return isStopping;
    }

    public void setStatsHolder(StatsHolder statsHolder) {
        this.statsHolder = Objects.requireNonNull(statsHolder);
    }

    @Nullable
    public StatsHolder getStatsHolder() {
        return statsHolder;
    }

    @Override
    protected void onCancelled() {
        stop(getReasonCancelled(), StopDataFrameAnalyticsAction.DEFAULT_TIMEOUT);
        markAsCompleted();
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        // onCancelled implements graceful shutdown of children
        return false;
    }

    @Override
    public void doMarkAsCompleted() {
        // It is possible that the stop API has been called in the meantime and that
        // may also cause this method to be called. We check whether we have already
        // been marked completed to avoid doing it twice. We need to capture that
        // locally instead of relying to isCompleted() because of the asynchronous
        // persistence of progress.
        synchronized (this) {
            if (isMarkAsCompletedCalled) {
                return;
            }
            isMarkAsCompletedCalled = true;
        }

        persistProgress(client, taskParams.getId(), () -> super.markAsCompleted());
    }

    @Override
    public void doMarkAsFailed(Exception e) {
        persistProgress(client, taskParams.getId(), () -> super.markAsFailed(e));
    }

    public void stop(String reason, TimeValue timeout) {
        isStopping = true;

        LOGGER.debug(() -> new ParameterizedMessage("[{}] Stopping task due to reason [{}]", getParams().getId(), reason));

        DataFrameAnalyticsStep cachedCurrentStep = currentStep;
        ActionListener<Void> stepProgressListener = ActionListener.wrap(
            aVoid -> cachedCurrentStep.cancel(reason, timeout),
            e -> {
                LOGGER.error(new ParameterizedMessage("[{}] Error updating progress for step [{}]",
                    taskParams.getId(), cachedCurrentStep.name()), e);
                // We should log the error but it shouldn't stop us from stopping the task
                cachedCurrentStep.cancel(reason, timeout);
            }
        );

        if (cachedCurrentStep != null) {
            cachedCurrentStep.updateProgress(stepProgressListener);
        }
    }

    public void setFailed(Exception error) {
        if (analyticsManager.isNodeShuttingDown()) {
            LOGGER.warn(
                new ParameterizedMessage("[{}] *Not* setting task to failed because the node is being shutdown", taskParams.getId()),
                error);
            return;
        }
        persistProgress(client, taskParams.getId(), () -> {
            LOGGER.error(new ParameterizedMessage("[{}] Setting task to failed", taskParams.getId()), error);
            String reason = ExceptionsHelper.unwrapCause(error).getMessage();
            DataFrameAnalyticsTaskState newTaskState =
                new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.FAILED, getAllocationId(), reason);
            updatePersistentTaskState(
                newTaskState,
                ActionListener.wrap(
                    updatedTask -> {
                        String message = Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_UPDATED_STATE_WITH_REASON,
                            DataFrameAnalyticsState.FAILED, reason);
                        auditor.info(getParams().getId(), message);
                        LOGGER.info("[{}] {}", getParams().getId(), message);
                    },
                    e -> LOGGER.error(new ParameterizedMessage("[{}] Could not update task state to [{}] with reason [{}]",
                        getParams().getId(), DataFrameAnalyticsState.FAILED, reason), e)
                )
            );
        });
    }

    public void persistProgress(Runnable runnable) {
        persistProgress(client, taskParams.getId(), runnable);
    }

    // Visible for testing
    void persistProgress(Client client, String jobId, Runnable runnable) {
        LOGGER.debug("[{}] Persisting progress", jobId);

        SetOnce<StoredProgress> storedProgress = new SetOnce<>();

        String progressDocId = StoredProgress.documentId(jobId);

        // Step 4: Run the runnable provided as the argument
        ActionListener<IndexResponse> indexProgressDocListener = ActionListener.wrap(
            indexResponse -> {
                LOGGER.debug("[{}] Successfully indexed progress document: {}", jobId, storedProgress.get().get());
                runnable.run();
            },
            indexError -> {
                LOGGER.error(new ParameterizedMessage(
                    "[{}] cannot persist progress as an error occurred while indexing", jobId), indexError);
                runnable.run();
            }
        );

        // Step 3: Create or update the progress document:
        //   - if the document did not exist, create the new one in the current write index
        //   - if the document did exist, update it in the index where it resides (not necessarily the current write index)
        ActionListener<SearchResponse> searchFormerProgressDocListener = ActionListener.wrap(
            searchResponse -> {
                String indexOrAlias = AnomalyDetectorsIndex.jobStateIndexWriteAlias();
                StoredProgress previous = null;
                if (searchResponse.getHits().getHits().length > 0) {
                    indexOrAlias = searchResponse.getHits().getHits()[0].getIndex();
                    try {
                        previous = MlParserUtils.parse(searchResponse.getHits().getHits()[0], StoredProgress.PARSER);
                    } catch (Exception ex) {
                        LOGGER.warn(new ParameterizedMessage("[{}] failed to parse previously stored progress", jobId), ex);
                    }
                }

                List<PhaseProgress> progress = statsHolder.getProgressTracker().report();
                storedProgress.set(new StoredProgress(progress));
                if (storedProgress.get().equals(previous)) {
                    LOGGER.debug(() -> new ParameterizedMessage(
                        "[{}] new progress is the same as previously persisted progress. Skipping storage of progress: {}",
                        jobId, progress));
                    runnable.run();
                    return;
                }

                IndexRequest indexRequest = new IndexRequest(indexOrAlias)
                    .id(progressDocId)
                    .setRequireAlias(AnomalyDetectorsIndex.jobStateIndexWriteAlias().equals(indexOrAlias))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                try (XContentBuilder jsonBuilder = JsonXContent.contentBuilder()) {
                    LOGGER.debug(() -> new ParameterizedMessage("[{}] Persisting progress is: {}", jobId, progress));
                    storedProgress.get().toXContent(jsonBuilder, Payload.XContent.EMPTY_PARAMS);
                    indexRequest.source(jsonBuilder);
                }
                executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, indexProgressDocListener);
            },
            e -> {
                LOGGER.error(new ParameterizedMessage(
                    "[{}] cannot persist progress as an error occurred while retrieving former progress document", jobId), e);
                runnable.run();
            }
        );

        // Step 2: Search for existing progress document in .ml-state*
        ActionListener<Void> stepProgressUpdateListener = ActionListener.wrap(
            aVoid -> {
                SearchRequest searchRequest =
                    new SearchRequest(AnomalyDetectorsIndex.jobStateIndexPattern())
                        .source(
                            new SearchSourceBuilder()
                                .size(1)
                                .query(new IdsQueryBuilder().addIds(progressDocId)));
                executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, searchFormerProgressDocListener);
            },
            e -> {
                LOGGER.error(new ParameterizedMessage(
                    "[{}] cannot persist progress as an error occurred while updating task progress", taskParams.getId()), e);
                runnable.run();
            }
        );

        // Step 1: Update reindexing progress as it could be stale
        updateTaskProgress(stepProgressUpdateListener);
    }

    public void updateTaskProgress(ActionListener<Void> updateProgressListener) {
        synchronized (this) {
            if (currentStep != null) {
                currentStep.updateProgress(updateProgressListener);
            } else {
                updateProgressListener.onResponse(null);
            }
        }
    }

    /**
     * This captures the possible states a job can be when it starts.
     * {@code FIRST_TIME} means the job has never been started before.
     * {@code RESUMING_REINDEXING} means the job was stopped while it was reindexing.
     * {@code RESUMING_ANALYZING} means the job was stopped while it was analyzing.
     * {@code FINISHED} means the job had finished.
     */
    public enum StartingState {
        FIRST_TIME, RESUMING_REINDEXING, RESUMING_ANALYZING, RESUMING_INFERENCE, FINISHED
    }

    public StartingState determineStartingState() {
        return determineStartingState(taskParams.getId(), statsHolder.getProgressTracker().report());
    }

    public static StartingState determineStartingState(String jobId, List<PhaseProgress> progressOnStart) {
        PhaseProgress lastIncompletePhase = null;
        for (PhaseProgress phaseProgress : progressOnStart) {
            if (phaseProgress.getProgressPercent() < 100) {
                lastIncompletePhase = phaseProgress;
                break;
            }
        }

        if (lastIncompletePhase == null) {
            return StartingState.FINISHED;
        }

        LOGGER.debug("[{}] Last incomplete progress [{}, {}]", jobId, lastIncompletePhase.getPhase(),
            lastIncompletePhase.getProgressPercent());

        if (ProgressTracker.REINDEXING.equals(lastIncompletePhase.getPhase())) {
            return lastIncompletePhase.getProgressPercent() == 0 ? StartingState.FIRST_TIME : StartingState.RESUMING_REINDEXING;
        }
        if (ProgressTracker.INFERENCE.equals(lastIncompletePhase.getPhase())) {
            return StartingState.RESUMING_INFERENCE;
        }
        return StartingState.RESUMING_ANALYZING;
    }
}
