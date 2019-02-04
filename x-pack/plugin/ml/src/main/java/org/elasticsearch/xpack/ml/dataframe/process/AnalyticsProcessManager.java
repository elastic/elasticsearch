/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.analyses.DataFrameAnalysesUtils;
import org.elasticsearch.xpack.ml.dataframe.analyses.DataFrameAnalysis;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class AnalyticsProcessManager {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsProcessManager.class);

    private final Client client;
    private final Environment environment;
    private final ThreadPool threadPool;
    private final AnalyticsProcessFactory processFactory;

    public AnalyticsProcessManager(Client client, Environment environment, ThreadPool threadPool,
                                   AnalyticsProcessFactory analyticsProcessFactory) {
        this.client = Objects.requireNonNull(client);
        this.environment = Objects.requireNonNull(environment);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.processFactory = Objects.requireNonNull(analyticsProcessFactory);
    }

    public void runJob(DataFrameAnalyticsConfig config, DataFrameDataExtractorFactory dataExtractorFactory,
                       Consumer<Exception> finishHandler) {
        threadPool.generic().execute(() -> {
            DataFrameDataExtractor dataExtractor = dataExtractorFactory.newExtractor(false);
            AnalyticsProcess process = createProcess(config.getId(), createProcessConfig(config, dataExtractor));
            ExecutorService executorService = threadPool.executor(MachineLearning.AUTODETECT_THREAD_POOL_NAME);
            AnalyticsResultProcessor resultProcessor = new AnalyticsResultProcessor(client, dataExtractorFactory.newExtractor(true));
            executorService.execute(() -> resultProcessor.process(process));
            executorService.execute(() -> processData(config.getId(), dataExtractor, process, resultProcessor, finishHandler));
        });
    }

    private void processData(String jobId, DataFrameDataExtractor dataExtractor, AnalyticsProcess process,
                             AnalyticsResultProcessor resultProcessor, Consumer<Exception> finishHandler) {
        try {
            writeHeaderRecord(dataExtractor, process);
            writeDataRows(dataExtractor, process);
            process.writeEndOfDataMessage();
            process.flushStream();

            LOGGER.info("[{}] Waiting for result processor to complete", jobId);
            resultProcessor.awaitForCompletion();
            LOGGER.info("[{}] Result processor has completed", jobId);
        } catch (IOException e) {
            LOGGER.error(new ParameterizedMessage("[{}] Error writing data to the process", jobId), e);
            // TODO Handle this failure by setting the task state to FAILED
        } finally {
            LOGGER.info("[{}] Closing process", jobId);
            try {
                process.close();
                LOGGER.info("[{}] Closed process", jobId);

                // This results in marking the persistent task as complete
                finishHandler.accept(null);
            } catch (IOException e) {
                LOGGER.error("[{}] Error closing data frame analyzer process", jobId);
                finishHandler.accept(e);
            }
        }
    }

    private void writeDataRows(DataFrameDataExtractor dataExtractor, AnalyticsProcess process) throws IOException {
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

    private void writeHeaderRecord(DataFrameDataExtractor dataExtractor, AnalyticsProcess process) throws IOException {
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

    private AnalyticsProcess createProcess(String jobId, AnalyticsProcessConfig analyticsProcessConfig) {
        // TODO We should rename the thread pool to reflect its more general use now, e.g. JOB_PROCESS_THREAD_POOL_NAME
        ExecutorService executorService = threadPool.executor(MachineLearning.AUTODETECT_THREAD_POOL_NAME);
        AnalyticsProcess process = processFactory.createAnalyticsProcess(jobId, analyticsProcessConfig, executorService);
        if (process.isProcessAlive() == false) {
            throw ExceptionsHelper.serverError("Failed to start data frame analytics process");
        }
        return process;
    }

    private AnalyticsProcessConfig createProcessConfig(DataFrameAnalyticsConfig config, DataFrameDataExtractor dataExtractor) {
        DataFrameDataExtractor.DataSummary dataSummary = dataExtractor.collectDataSummary();
        List<DataFrameAnalysis> dataFrameAnalyses = DataFrameAnalysesUtils.readAnalyses(config.getAnalyses());
        // TODO We will not need this assertion after we add support for multiple analyses
        assert dataFrameAnalyses.size() == 1;

        AnalyticsProcessConfig processConfig = new AnalyticsProcessConfig(dataSummary.rows, dataSummary.cols,
                new ByteSizeValue(1, ByteSizeUnit.GB), 1, dataFrameAnalyses.get(0));
        return processConfig;
    }
}
