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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.TransportStartDataFrameAnalyticsAction.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class AnalyticsProcessManager {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsProcessManager.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final AnalyticsProcessFactory<AnalyticsResult> processFactory;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();

    public AnalyticsProcessManager(Client client,
                                   ThreadPool threadPool,
                                   AnalyticsProcessFactory<AnalyticsResult> analyticsProcessFactory) {
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.processFactory = Objects.requireNonNull(analyticsProcessFactory);
    }

    public void runJob(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config,
                       DataFrameDataExtractorFactory dataExtractorFactory, Consumer<Exception> finishHandler) {
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
            if (processContext.startProcess(dataExtractorFactory, config, task)) {
                ExecutorService executorService = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
                executorService.execute(() -> processResults(processContext));
                executorService.execute(() -> processData(task, config, processContext.dataExtractor,
                    processContext.process, processContext.resultProcessor, finishHandler));
            } else {
                finishHandler.accept(null);
            }
        });
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
                             Consumer<Exception> finishHandler) {

        try {
            writeHeaderRecord(dataExtractor, process);
            writeDataRows(dataExtractor, process);
            process.writeEndOfDataMessage();
            process.flushStream();

            LOGGER.info("[{}] Waiting for result processor to complete", config.getId());
            resultProcessor.awaitForCompletion();
            processContextByAllocation.get(task.getAllocationId()).setFailureReason(resultProcessor.getFailure());

            refreshDest(config);
            LOGGER.info("[{}] Result processor has completed", config.getId());
        } catch (Exception e) {
            String errorMsg = new ParameterizedMessage("[{}] Error while processing data", config.getId()).getFormattedMessage();
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

    private void writeDataRows(DataFrameDataExtractor dataExtractor, AnalyticsProcess<AnalyticsResult> process) throws IOException {
        // The extra fields are for the doc hash and the control field (should be an empty string)
        String[] record = new String[dataExtractor.getFieldNames().size() + 2];
        // The value of the control field should be an empty string for data frame rows
        record[record.length - 1] = "";

        while (dataExtractor.hasNext()) {
            Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
            if (rows.isPresent()) {
                for (DataFrameDataExtractor.Row row : rows.get()) {
                    if (row.shouldSkip() == false) {
                        String[] rowValues = row.getValues();
                        System.arraycopy(rowValues, 0, record, 0, rowValues.length);
                        record[record.length - 2] = String.valueOf(row.getChecksum());
                        process.writeRecord(record);
                    }
                }
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

    private AnalyticsProcess<AnalyticsResult> createProcess(DataFrameAnalyticsTask task, AnalyticsProcessConfig analyticsProcessConfig) {
        ExecutorService executorService = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
        AnalyticsProcess<AnalyticsResult> process = processFactory.createAnalyticsProcess(task.getParams().getId(), analyticsProcessConfig,
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

    @Nullable
    public Integer getProgressPercent(long allocationId) {
        ProcessContext processContext = processContextByAllocation.get(allocationId);
        return processContext == null ? null : processContext.progressPercent.get();
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
        private final AtomicInteger progressPercent = new AtomicInteger(0);
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

        void setProgressPercent(int progressPercent) {
            this.progressPercent.set(progressPercent);
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
                                                  DataFrameAnalyticsTask task) {
            if (processKilled) {
                // The job was stopped before we started the process so no need to start it
                return false;
            }

            dataExtractor = dataExtractorFactory.newExtractor(false);
            process = createProcess(task, createProcessConfig(config, dataExtractor));
            DataFrameRowsJoiner dataFrameRowsJoiner = new DataFrameRowsJoiner(config.getId(), client,
                dataExtractorFactory.newExtractor(true));
            resultProcessor = new AnalyticsResultProcessor(id, dataFrameRowsJoiner, this::isProcessKilled, this::setProgressPercent);
            return true;
        }

        private AnalyticsProcessConfig createProcessConfig(DataFrameAnalyticsConfig config, DataFrameDataExtractor dataExtractor) {
            DataFrameDataExtractor.DataSummary dataSummary = dataExtractor.collectDataSummary();
            Set<String> categoricalFields = dataExtractor.getCategoricalFields();
            AnalyticsProcessConfig processConfig = new AnalyticsProcessConfig(dataSummary.rows, dataSummary.cols,
                config.getModelMemoryLimit(), 1, config.getDest().getResultsField(), categoricalFields, config.getAnalysis());
            return processConfig;
        }
    }
}
