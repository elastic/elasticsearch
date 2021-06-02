/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.stats.DataCountsTracker;
import org.elasticsearch.xpack.ml.dataframe.stats.ProgressTracker;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsPersister;
import org.elasticsearch.xpack.ml.dataframe.steps.StepResponse;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class AnalyticsProcessManager {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsProcessManager.class);

    private final Settings settings;
    private final Client client;
    private final ExecutorService executorServiceForJob;
    private final ExecutorService executorServiceForProcess;
    private final AnalyticsProcessFactory<AnalyticsResult> processFactory;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();
    private final DataFrameAnalyticsAuditor auditor;
    private final TrainedModelProvider trainedModelProvider;
    private final ResultsPersisterService resultsPersisterService;
    private final int numAllocatedProcessors;

    public AnalyticsProcessManager(Settings settings,
                                   Client client,
                                   ThreadPool threadPool,
                                   AnalyticsProcessFactory<AnalyticsResult> analyticsProcessFactory,
                                   DataFrameAnalyticsAuditor auditor,
                                   TrainedModelProvider trainedModelProvider,
                                   ResultsPersisterService resultsPersisterService,
                                   int numAllocatedProcessors) {
        this(
            settings,
            client,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
            threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME),
            analyticsProcessFactory,
            auditor,
            trainedModelProvider,
            resultsPersisterService,
            numAllocatedProcessors);
    }

    // Visible for testing
    public AnalyticsProcessManager(Settings settings,
                                   Client client,
                                   ExecutorService executorServiceForJob,
                                   ExecutorService executorServiceForProcess,
                                   AnalyticsProcessFactory<AnalyticsResult> analyticsProcessFactory,
                                   DataFrameAnalyticsAuditor auditor,
                                   TrainedModelProvider trainedModelProvider,
                                   ResultsPersisterService resultsPersisterService,
                                   int numAllocatedProcessors) {
        this.settings = Objects.requireNonNull(settings);
        this.client = Objects.requireNonNull(client);
        this.executorServiceForJob = Objects.requireNonNull(executorServiceForJob);
        this.executorServiceForProcess = Objects.requireNonNull(executorServiceForProcess);
        this.processFactory = Objects.requireNonNull(analyticsProcessFactory);
        this.auditor = Objects.requireNonNull(auditor);
        this.trainedModelProvider = Objects.requireNonNull(trainedModelProvider);
        this.resultsPersisterService = Objects.requireNonNull(resultsPersisterService);
        this.numAllocatedProcessors = numAllocatedProcessors;
    }

    public void runJob(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config, DataFrameDataExtractorFactory dataExtractorFactory,
                       ActionListener<StepResponse> listener) {
        executorServiceForJob.execute(() -> {
            ProcessContext processContext = new ProcessContext(config);
            synchronized (processContextByAllocation) {
                if (task.isStopping()) {
                    LOGGER.debug("[{}] task is stopping. Marking as complete before creating process context.",
                        task.getParams().getId());
                    // The task was requested to stop before we created the process context
                    auditor.info(config.getId(), Messages.DATA_FRAME_ANALYTICS_AUDIT_FINISHED_ANALYSIS);
                    listener.onResponse(new StepResponse(true));
                    return;
                }
                if (processContextByAllocation.putIfAbsent(task.getAllocationId(), processContext) != null) {
                    listener.onFailure(ExceptionsHelper.serverError(
                        "[" + config.getId() + "] Could not create process as one already exists"));
                    return;
                }
            }

            // Fetch existing model state (if any)
            final boolean hasState = hasModelState(config);

            boolean isProcessStarted;
            try {
                isProcessStarted = processContext.startProcess(dataExtractorFactory, task, hasState);
            } catch (Exception e) {
                processContext.stop();
                processContextByAllocation.remove(task.getAllocationId());
                listener.onFailure(processContext.getFailureReason() == null ?
                        e : ExceptionsHelper.serverError(processContext.getFailureReason()));
                return;
            }

            if (isProcessStarted) {
                executorServiceForProcess.execute(() -> processContext.resultProcessor.get().process(processContext.process.get()));
                executorServiceForProcess.execute(() -> processData(task, processContext, hasState, listener));
            } else {
                processContextByAllocation.remove(task.getAllocationId());
                auditor.info(config.getId(), Messages.DATA_FRAME_ANALYTICS_AUDIT_FINISHED_ANALYSIS);
                listener.onResponse(new StepResponse(true));
            }
        });
    }

    private boolean hasModelState(DataFrameAnalyticsConfig config) {
        if (config.getAnalysis().persistsState() == false) {
            return false;
        }

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            SearchResponse searchResponse = client.prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
                .setSize(1)
                .setFetchSource(false)
                .setQuery(QueryBuilders.idsQuery().addIds(config.getAnalysis().getStateDocIdPrefix(config.getId()) + "1"))
                .get();
            return searchResponse.getHits().getHits().length == 1;
        }
    }

    private void processData(DataFrameAnalyticsTask task, ProcessContext processContext, boolean hasState,
                             ActionListener<StepResponse> listener) {
        LOGGER.info("[{}] Started loading data", processContext.config.getId());
        auditor.info(processContext.config.getId(), Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_STARTED_LOADING_DATA));

        DataFrameAnalyticsConfig config = processContext.config;
        DataFrameDataExtractor dataExtractor = processContext.dataExtractor.get();
        AnalyticsProcess<AnalyticsResult> process = processContext.process.get();
        AnalyticsResultProcessor resultProcessor = processContext.resultProcessor.get();
        try {
            writeHeaderRecord(dataExtractor, process, task);
            writeDataRows(dataExtractor, process, task);
            process.writeEndOfDataMessage();
            LOGGER.debug(() -> new ParameterizedMessage("[{}] Flushing input stream", processContext.config.getId()));
            process.flushStream();
            LOGGER.debug(() -> new ParameterizedMessage("[{}] Flushing input stream completed", processContext.config.getId()));

            restoreState(config, process, hasState);

            LOGGER.info("[{}] Started analyzing", processContext.config.getId());
            auditor.info(processContext.config.getId(), Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_STARTED_ANALYZING));

            LOGGER.info("[{}] Waiting for result processor to complete", config.getId());
            resultProcessor.awaitForCompletion();
            processContext.setFailureReason(resultProcessor.getFailure());
            LOGGER.info("[{}] Result processor has completed", config.getId());
        } catch (Exception e) {
            if (task.isStopping()) {
                // Errors during task stopping are expected but we still want to log them just in case.
                String errorMsg =
                    new ParameterizedMessage(
                        "[{}] Error while processing data [{}]; task is stopping", config.getId(), e.getMessage()).getFormattedMessage();
                LOGGER.debug(errorMsg, e);
            } else {
                String errorMsg =
                    new ParameterizedMessage("[{}] Error while processing data [{}]", config.getId(), e.getMessage()).getFormattedMessage();
                LOGGER.error(errorMsg, e);
                processContext.setFailureReason(errorMsg);
            }
        } finally {
            closeProcess(task);

            processContextByAllocation.remove(task.getAllocationId());
            LOGGER.debug("Removed process context for task [{}]; [{}] processes still running", config.getId(),
                processContextByAllocation.size());

            if (processContext.getFailureReason() == null) {
                auditor.info(config.getId(), Messages.DATA_FRAME_ANALYTICS_AUDIT_FINISHED_ANALYSIS);
                listener.onResponse(new StepResponse(false));
            } else {
                LOGGER.error("[{}] Marking task failed; {}", config.getId(), processContext.getFailureReason());
                listener.onFailure(ExceptionsHelper.serverError(processContext.getFailureReason()));
                // Note: We are not marking the task as failed here as we want the user to be able to inspect the failure reason.
            }
        }
    }

    private void writeDataRows(DataFrameDataExtractor dataExtractor, AnalyticsProcess<AnalyticsResult> process,
                               DataFrameAnalyticsTask task) throws IOException {
        ProgressTracker progressTracker = task.getStatsHolder().getProgressTracker();
        DataCountsTracker dataCountsTracker = task.getStatsHolder().getDataCountsTracker();

        // The extra fields are for the doc hash and the control field (should be an empty string)
        String[] record = new String[dataExtractor.getFieldNames().size() + 2];
        // The value of the control field should be an empty string for data frame rows
        record[record.length - 1] = "";

        long totalRows = process.getConfig().rows();
        long rowsProcessed = 0;

        while (dataExtractor.hasNext()) {
            Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
            if (rows.isPresent()) {
                for (DataFrameDataExtractor.Row row : rows.get()) {
                    if (row.shouldSkip()) {
                        dataCountsTracker.incrementSkippedDocsCount();
                    } else {
                        String[] rowValues = row.getValues();
                        System.arraycopy(rowValues, 0, record, 0, rowValues.length);
                        record[record.length - 2] = String.valueOf(row.getChecksum());
                        if (row.isTraining()) {
                            dataCountsTracker.incrementTrainingDocsCount();
                            process.writeRecord(record);
                        }
                    }
                }
                rowsProcessed += rows.get().size();
                progressTracker.updateLoadingDataProgress(rowsProcessed >= totalRows ? 100 : (int) (rowsProcessed * 100.0 / totalRows));
            }
        }
    }

    private void writeHeaderRecord(DataFrameDataExtractor dataExtractor,
                                   AnalyticsProcess<AnalyticsResult> process,
                                   DataFrameAnalyticsTask task) throws IOException {
        List<String> fieldNames = dataExtractor.getFieldNames();
        LOGGER.debug(() -> new ParameterizedMessage("[{}] header row fields {}", task.getParams().getId(), fieldNames));

        // We add 2 extra fields, both named dot:
        //   - the document hash
        //   - the control message
        String[] headerRecord = new String[fieldNames.size() + 2];
        for (int i = 0; i < fieldNames.size(); i++) {
            headerRecord[i] = fieldNames.get(i);
        }

        headerRecord[headerRecord.length - 2] = ".";
        headerRecord[headerRecord.length - 1] = ".";
        process.writeRecord(headerRecord);
    }

    private void restoreState(DataFrameAnalyticsConfig config, AnalyticsProcess<AnalyticsResult> process, boolean hasState) {
        if (config.getAnalysis().persistsState() == false) {
            LOGGER.debug("[{}] Analysis does not support state", config.getId());
            return;
        }

        if (hasState == false) {
            LOGGER.debug("[{}] No model state available to restore", config.getId());
            return;
        }

        LOGGER.debug("[{}] Restoring from previous model state", config.getId());
        auditor.info(config.getId(), Messages.DATA_FRAME_ANALYTICS_AUDIT_RESTORING_STATE);

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            process.restoreState(client, config.getAnalysis().getStateDocIdPrefix(config.getId()));
        } catch (Exception e) {
            LOGGER.error(new ParameterizedMessage("[{}] Failed to restore state", process.getConfig().jobId()), e);
            throw ExceptionsHelper.serverError("Failed to restore state: " + e.getMessage());
        }
    }

    private AnalyticsProcess<AnalyticsResult> createProcess(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config,
                                                            AnalyticsProcessConfig analyticsProcessConfig, boolean hasState) {
        AnalyticsProcess<AnalyticsResult> process = processFactory.createAnalyticsProcess(
            config, analyticsProcessConfig, hasState, executorServiceForProcess, onProcessCrash(task));
        if (process.isProcessAlive() == false) {
            throw ExceptionsHelper.serverError("Failed to start data frame analytics process");
        }
        return process;
    }

    private Consumer<String> onProcessCrash(DataFrameAnalyticsTask task) {
        return reason -> {
            ProcessContext processContext = processContextByAllocation.get(task.getAllocationId());
            if (processContext != null) {
                processContext.setFailureReason(reason);
                processContext.stop();
            }
        };
    }

    private void closeProcess(DataFrameAnalyticsTask task) {
        String configId = task.getParams().getId();
        LOGGER.info("[{}] Closing process", configId);

        ProcessContext processContext = processContextByAllocation.get(task.getAllocationId());
        try {
            processContext.process.get().close();
            LOGGER.info("[{}] Closed process", configId);
        } catch (Exception e) {
            if (task.isStopping()) {
                LOGGER.debug(() -> new ParameterizedMessage(
                    "[{}] Process closing was interrupted by kill request due to the task being stopped", configId), e);
                LOGGER.info("[{}] Closed process", configId);
            } else {
                LOGGER.error("[" + configId + "] Error closing data frame analyzer process", e);
                String errorMsg = new ParameterizedMessage(
                    "[{}] Error closing data frame analyzer process [{}]", configId, e.getMessage()).getFormattedMessage();
                processContext.setFailureReason(errorMsg);
            }
        }
    }

    public void stop(DataFrameAnalyticsTask task) {
        ProcessContext processContext;
        synchronized (processContextByAllocation) {
            processContext = processContextByAllocation.get(task.getAllocationId());
        }
        if (processContext != null) {
            LOGGER.debug("[{}] Stopping process", task.getParams().getId());
            processContext.stop();
        } else {
            LOGGER.debug("[{}] No process context to stop", task.getParams().getId());
        }
    }

    // Visible for testing
    int getProcessContextCount() {
        return processContextByAllocation.size();
    }

    class ProcessContext {

        private final DataFrameAnalyticsConfig config;
        private final SetOnce<AnalyticsProcess<AnalyticsResult>> process = new SetOnce<>();
        private final SetOnce<DataFrameDataExtractor> dataExtractor = new SetOnce<>();
        private final SetOnce<AnalyticsResultProcessor> resultProcessor = new SetOnce<>();
        private final SetOnce<String> failureReason = new SetOnce<>();

        ProcessContext(DataFrameAnalyticsConfig config) {
            this.config = Objects.requireNonNull(config);
        }

        String getFailureReason() {
            return failureReason.get();
        }

        void setFailureReason(String failureReason) {
            if (failureReason == null) {
                return;
            }
            // Only set the new reason if there isn't one already as we want to keep the first reason (most likely the root cause).
            this.failureReason.trySet(failureReason);
        }

        synchronized void stop() {
            LOGGER.debug("[{}] Stopping process", config.getId());
            if (dataExtractor.get() != null) {
                dataExtractor.get().cancel();
            }
            if (resultProcessor.get() != null) {
                resultProcessor.get().cancel();
            }
            if (process.get() != null) {
                try {
                    process.get().kill(true);
                } catch (IOException e) {
                    LOGGER.error(new ParameterizedMessage("[{}] Failed to kill process", config.getId()), e);
                }
            }
        }

        /**
         * @return {@code true} if the process was started or {@code false} if it was not because it was stopped in the meantime
         */
        synchronized boolean startProcess(DataFrameDataExtractorFactory dataExtractorFactory, DataFrameAnalyticsTask task,
                                          boolean hasState) {
            if (task.isStopping()) {
                // The job was stopped before we started the process so no need to start it
                return false;
            }

            dataExtractor.set(dataExtractorFactory.newExtractor(false));
            AnalyticsProcessConfig analyticsProcessConfig =
                createProcessConfig(dataExtractor.get(), dataExtractorFactory.getExtractedFields());
            LOGGER.debug("[{}] creating analytics process with config [{}]", config.getId(), Strings.toString(analyticsProcessConfig));
            // If we have no rows, that means there is no data so no point in starting the native process
            // just finish the task
            if (analyticsProcessConfig.rows() == 0) {
                LOGGER.info("[{}] no data found to analyze. Will not start analytics native process.", config.getId());
                return false;
            }
            process.set(createProcess(task, config, analyticsProcessConfig, hasState));
            resultProcessor.set(createResultProcessor(task, dataExtractorFactory));
            return true;
        }

        private AnalyticsProcessConfig createProcessConfig(DataFrameDataExtractor dataExtractor,
                                                           ExtractedFields extractedFields) {
            DataFrameDataExtractor.DataSummary dataSummary = dataExtractor.collectDataSummary();
            Set<String> categoricalFields = dataExtractor.getCategoricalFields(config.getAnalysis());
            int threads = Math.min(config.getMaxNumThreads(), numAllocatedProcessors);
            return new AnalyticsProcessConfig(
                config.getId(),
                dataSummary.rows,
                dataSummary.cols,
                config.getModelMemoryLimit(),
                threads,
                config.getDest().getResultsField(),
                categoricalFields,
                config.getAnalysis(),
                extractedFields);
        }

        private AnalyticsResultProcessor createResultProcessor(DataFrameAnalyticsTask task,
                                                               DataFrameDataExtractorFactory dataExtractorFactory) {
            DataFrameRowsJoiner dataFrameRowsJoiner =
                new DataFrameRowsJoiner(config.getId(), settings, task.getParentTaskId(),
                        dataExtractorFactory.newExtractor(true), resultsPersisterService);
            StatsPersister statsPersister = new StatsPersister(config.getId(), resultsPersisterService, auditor);
            return new AnalyticsResultProcessor(
                config, dataFrameRowsJoiner, task.getStatsHolder(), trainedModelProvider, auditor, statsPersister,
                dataExtractor.get().getExtractedFields());
        }
    }
}
