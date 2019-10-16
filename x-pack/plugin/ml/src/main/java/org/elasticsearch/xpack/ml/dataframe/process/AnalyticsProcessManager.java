/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.customprocessing.CustomProcessor;
import org.elasticsearch.xpack.ml.dataframe.process.customprocessing.CustomProcessorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

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

    private final Client client;
    private final ThreadPool threadPool;
    private final AnalyticsProcessFactory<AnalyticsResult> processFactory;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();
    private final DataFrameAnalyticsAuditor auditor;
    private final TrainedModelProvider trainedModelProvider;

    public AnalyticsProcessManager(Client client,
                                   ThreadPool threadPool,
                                   AnalyticsProcessFactory<AnalyticsResult> analyticsProcessFactory,
                                   DataFrameAnalyticsAuditor auditor,
                                   TrainedModelProvider trainedModelProvider) {
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.processFactory = Objects.requireNonNull(analyticsProcessFactory);
        this.auditor = Objects.requireNonNull(auditor);
        this.trainedModelProvider = Objects.requireNonNull(trainedModelProvider);
    }

    public void runJob(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config, DataFrameDataExtractorFactory dataExtractorFactory,
                       Consumer<Exception> finishHandler) {
        threadPool.generic().execute(() -> {
            if (task.isStopping()) {
                // The task was requested to stop before we created the process context
                finishHandler.accept(null);
                return;
            }

            ProcessContext processContext = new ProcessContext(config.getId());
            if (processContextByAllocation.putIfAbsent(task.getAllocationId(), processContext) != null) {
                finishHandler.accept(ExceptionsHelper.serverError("[" + processContext.id
                    + "] Could not create process as one already exists"));
                return;
            }

            BytesReference state = getModelState(config);

            if (processContext.startProcess(dataExtractorFactory, config, task, state)) {
                ExecutorService executorService = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
                executorService.execute(() -> processResults(processContext));
                executorService.execute(() -> processData(task, config, processContext.dataExtractor,
                    processContext.process, processContext.resultProcessor, finishHandler, state));
            } else {
                finishHandler.accept(null);
            }
        });
    }

    @Nullable
    private BytesReference getModelState(DataFrameAnalyticsConfig config) {
        if (config.getAnalysis().persistsState() == false) {
            return null;
        }

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            SearchRequest searchRequest = new SearchRequest(AnomalyDetectorsIndex.jobStateIndexPattern());
            searchRequest.source().size(1).query(QueryBuilders.idsQuery().addIds(config.getAnalysis().getStateDocId(config.getId())));
            SearchResponse searchResponse = client.prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
                .setSize(1)
                .setQuery(QueryBuilders.idsQuery().addIds(config.getAnalysis().getStateDocId(config.getId())))
                .get();
            SearchHit[] hits = searchResponse.getHits().getHits();
            return hits.length == 0 ? null : hits[0].getSourceRef();
        }
    }

    private void processResults(ProcessContext processContext) {
        try {
            processContext.resultProcessor.process(processContext.process);
        } catch (Exception e) {
            processContext.setFailureReason(e.getMessage());
        }
    }

    private void processData(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config, DataFrameDataExtractor dataExtractor,
                             AnalyticsProcess<AnalyticsResult> process, AnalyticsResultProcessor resultProcessor,
                             Consumer<Exception> finishHandler, BytesReference state) {

        try {
            ProcessContext processContext = processContextByAllocation.get(task.getAllocationId());
            writeHeaderRecord(dataExtractor, process);
            writeDataRows(dataExtractor, process, config.getAnalysis(), task.getProgressTracker());
            process.writeEndOfDataMessage();
            process.flushStream();

            restoreState(config, state, process, finishHandler);

            LOGGER.info("[{}] Waiting for result processor to complete", config.getId());
            resultProcessor.awaitForCompletion();
            processContext.setFailureReason(resultProcessor.getFailure());

            refreshDest(config);
            LOGGER.info("[{}] Result processor has completed", config.getId());
        } catch (Exception e) {
            String errorMsg = new ParameterizedMessage("[{}] Error while processing data [{}]", config.getId(), e.getMessage())
                .getFormattedMessage();
            LOGGER.error(errorMsg, e);
            processContextByAllocation.get(task.getAllocationId()).setFailureReason(errorMsg);
        } finally {
            closeProcess(task);

            ProcessContext processContext = processContextByAllocation.remove(task.getAllocationId());
            LOGGER.debug("Removed process context for task [{}]; [{}] processes still running", config.getId(),
                processContextByAllocation.size());

            if (processContext.getFailureReason() == null) {
                // This results in marking the persistent task as complete
                LOGGER.info("[{}] Marking task completed", config.getId());
                finishHandler.accept(null);
            } else {
                LOGGER.error("[{}] Marking task failed; {}", config.getId(), processContext.getFailureReason());
                task.updateState(DataFrameAnalyticsState.FAILED, processContext.getFailureReason());
            }
        }
    }

    private void writeDataRows(DataFrameDataExtractor dataExtractor, AnalyticsProcess<AnalyticsResult> process,
                               DataFrameAnalysis analysis, DataFrameAnalyticsTask.ProgressTracker progressTracker) throws IOException {

        CustomProcessor customProcessor = new CustomProcessorFactory(dataExtractor.getFieldNames()).create(analysis);

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
                    if (row.shouldSkip() == false) {
                        String[] rowValues = row.getValues();
                        System.arraycopy(rowValues, 0, record, 0, rowValues.length);
                        record[record.length - 2] = String.valueOf(row.getChecksum());
                        customProcessor.process(record);
                        process.writeRecord(record);
                    }
                }
                rowsProcessed += rows.get().size();
                progressTracker.loadingDataPercent.set(rowsProcessed >= totalRows ? 100 : (int) (rowsProcessed * 100.0 / totalRows));
            }
        }
    }

    private void writeHeaderRecord(DataFrameDataExtractor dataExtractor, AnalyticsProcess<AnalyticsResult> process) throws IOException {
        List<String> fieldNames = dataExtractor.getFieldNames();

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

    private void restoreState(DataFrameAnalyticsConfig config, @Nullable BytesReference state, AnalyticsProcess<AnalyticsResult> process,
                              Consumer<Exception> failureHandler) {
        if (config.getAnalysis().persistsState() == false) {
            LOGGER.debug("[{}] Analysis does not support state", config.getId());
            return;
        }

        if (state == null) {
            LOGGER.debug("[{}] No model state available to restore", config.getId());
            return;
        }

        LOGGER.debug("[{}] Restoring from previous model state", config.getId());
        auditor.info(config.getId(), Messages.DATA_FRAME_ANALYTICS_AUDIT_RESTORING_STATE);

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            process.restoreState(state);
        } catch (Exception e) {
            LOGGER.error(new ParameterizedMessage("[{}] Failed to restore state", process.getConfig().jobId()), e);
            failureHandler.accept(ExceptionsHelper.serverError("Failed to restore state", e));
        }
    }

    private AnalyticsProcess<AnalyticsResult> createProcess(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config,
                                                            AnalyticsProcessConfig analyticsProcessConfig, @Nullable BytesReference state) {
        ExecutorService executorService = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
        AnalyticsProcess<AnalyticsResult> process = processFactory.createAnalyticsProcess(config, analyticsProcessConfig, state,
            executorService, onProcessCrash(task));
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

    private void refreshDest(DataFrameAnalyticsConfig config) {
        ClientHelper.executeWithHeaders(config.getHeaders(), ClientHelper.ML_ORIGIN, client,
            () -> client.execute(RefreshAction.INSTANCE, new RefreshRequest(config.getDest().getIndex())).actionGet());
    }

    private void closeProcess(DataFrameAnalyticsTask task) {
        String configId = task.getParams().getId();
        LOGGER.info("[{}] Closing process", configId);

        ProcessContext processContext = processContextByAllocation.get(task.getAllocationId());
        try {
            processContext.process.close();
            LOGGER.info("[{}] Closed process", configId);
        } catch (Exception e) {
            String errorMsg = new ParameterizedMessage("[{}] Error closing data frame analyzer process [{}]"
                , configId, e.getMessage()).getFormattedMessage();
            processContext.setFailureReason(errorMsg);
        }
    }

    public void stop(DataFrameAnalyticsTask task) {
        ProcessContext processContext = processContextByAllocation.get(task.getAllocationId());
        if (processContext != null) {
            LOGGER.debug("[{}] Stopping process", task.getParams().getId() );
            processContext.stop();
        } else {
            LOGGER.debug("[{}] No process context to stop", task.getParams().getId() );
            task.markAsCompleted();
        }
    }

    class ProcessContext {

        private final String id;
        private volatile AnalyticsProcess<AnalyticsResult> process;
        private volatile DataFrameDataExtractor dataExtractor;
        private volatile AnalyticsResultProcessor resultProcessor;
        private volatile boolean processKilled;
        private volatile String failureReason;

        ProcessContext(String id) {
            this.id = Objects.requireNonNull(id);
        }

        public String getId() {
            return id;
        }

        public boolean isProcessKilled() {
            return processKilled;
        }

        private synchronized void setFailureReason(String failureReason) {
            // Only set the new reason if there isn't one already as we want to keep the first reason
            if (failureReason != null) {
                this.failureReason = failureReason;
            }
        }

        private String getFailureReason() {
            return failureReason;
        }

        public synchronized void stop() {
            LOGGER.debug("[{}] Stopping process", id);
            processKilled = true;
            if (dataExtractor != null) {
                dataExtractor.cancel();
            }
            if (process != null) {
                try {
                    process.kill();
                } catch (IOException e) {
                    LOGGER.error(new ParameterizedMessage("[{}] Failed to kill process", id), e);
                }
            }
        }

        /**
         * @return {@code true} if the process was started or {@code false} if it was not because it was stopped in the meantime
         */
        private synchronized boolean startProcess(DataFrameDataExtractorFactory dataExtractorFactory, DataFrameAnalyticsConfig config,
                                                  DataFrameAnalyticsTask task, @Nullable BytesReference state) {
            if (processKilled) {
                // The job was stopped before we started the process so no need to start it
                return false;
            }

            dataExtractor = dataExtractorFactory.newExtractor(false);
            AnalyticsProcessConfig analyticsProcessConfig = createProcessConfig(config, dataExtractor);
            LOGGER.trace("[{}] creating analytics process with config [{}]", config.getId(), Strings.toString(analyticsProcessConfig));
            // If we have no rows, that means there is no data so no point in starting the native process
            // just finish the task
            if (analyticsProcessConfig.rows() == 0) {
                LOGGER.info("[{}] no data found to analyze. Will not start analytics native process.", config.getId());
                return false;
            }
            process = createProcess(task, config, analyticsProcessConfig, state);
            DataFrameRowsJoiner dataFrameRowsJoiner = new DataFrameRowsJoiner(config.getId(), client,
                dataExtractorFactory.newExtractor(true));
            resultProcessor = new AnalyticsResultProcessor(config, dataFrameRowsJoiner, this::isProcessKilled, task.getProgressTracker(),
                trainedModelProvider, auditor);
            return true;
        }

        private AnalyticsProcessConfig createProcessConfig(DataFrameAnalyticsConfig config, DataFrameDataExtractor dataExtractor) {
            DataFrameDataExtractor.DataSummary dataSummary = dataExtractor.collectDataSummary();
            Set<String> categoricalFields = dataExtractor.getCategoricalFields(config.getAnalysis());
            AnalyticsProcessConfig processConfig = new AnalyticsProcessConfig(config.getId(), dataSummary.rows, dataSummary.cols,
                config.getModelMemoryLimit(), 1, config.getDest().getResultsField(), categoricalFields, config.getAnalysis());
            return processConfig;
        }
    }
}
