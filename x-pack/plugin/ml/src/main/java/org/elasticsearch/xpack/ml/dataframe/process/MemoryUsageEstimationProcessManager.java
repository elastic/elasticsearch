/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.results.MemoryUsageEstimationResult;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class MemoryUsageEstimationProcessManager {

    private static final Logger LOGGER = LogManager.getLogger(MemoryUsageEstimationProcessManager.class);

    private final ExecutorService executorServiceForJob;
    private final ExecutorService executorServiceForProcess;
    private final AnalyticsProcessFactory<MemoryUsageEstimationResult> processFactory;

    public MemoryUsageEstimationProcessManager(ExecutorService executorServiceForJob,
                                               ExecutorService executorServiceForProcess,
                                               AnalyticsProcessFactory<MemoryUsageEstimationResult> processFactory) {
        this.executorServiceForJob = Objects.requireNonNull(executorServiceForJob);
        this.executorServiceForProcess = Objects.requireNonNull(executorServiceForProcess);
        this.processFactory = Objects.requireNonNull(processFactory);
    }

    public void runJobAsync(String jobId,
                            DataFrameAnalyticsConfig config,
                            DataFrameDataExtractorFactory dataExtractorFactory,
                            ActionListener<MemoryUsageEstimationResult> listener) {
        executorServiceForJob.execute(() -> {
            try {
                MemoryUsageEstimationResult result = runJob(jobId, config, dataExtractorFactory);
                listener.onResponse(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private MemoryUsageEstimationResult runJob(String jobId,
                                               DataFrameAnalyticsConfig config,
                                               DataFrameDataExtractorFactory dataExtractorFactory) {
        DataFrameDataExtractor dataExtractor = dataExtractorFactory.newExtractor(false);
        DataFrameDataExtractor.DataSummary dataSummary = dataExtractor.collectDataSummary();
        Set<String> categoricalFields = dataExtractor.getCategoricalFields();
        if (dataSummary.rows == 0) {
            return new MemoryUsageEstimationResult(ByteSizeValue.ZERO, ByteSizeValue.ZERO);
        }
        AnalyticsProcessConfig processConfig =
            new AnalyticsProcessConfig(
                dataSummary.rows,
                dataSummary.cols,
                // For memory estimation the model memory limit here should be set high enough not to trigger an error when C++ code
                // compares the limit to the result of estimation.
                new ByteSizeValue(1, ByteSizeUnit.PB),
                1,
                "",
                categoricalFields,
                config.getAnalysis());
        AnalyticsProcess<MemoryUsageEstimationResult> process =
            processFactory.createAnalyticsProcess(
                jobId,
                processConfig,
                executorServiceForProcess,
                // The handler passed here will never be called as AbstractNativeProcess.detectCrash method returns early when
                // (processInStream == null) which is the case for MemoryUsageEstimationProcess.
                reason -> {});
        try {
            return readResult(jobId, process);
        } catch (Exception e) {
            String errorMsg =
                new ParameterizedMessage(
                    "[{}] Error while processing process output [{}], process errors: [{}]",
                    jobId, e.getMessage(), process.readError()).getFormattedMessage();
            throw ExceptionsHelper.serverError(errorMsg, e);
        } finally {
            process.consumeAndCloseOutputStream();
            try {
                LOGGER.info("[{}] Closing process", jobId);
                process.close();
                LOGGER.info("[{}] Closed process", jobId);
            } catch (Exception e) {
                String errorMsg =
                    new ParameterizedMessage(
                        "[{}] Error while closing process [{}], process errors: [{}]",
                        jobId, e.getMessage(), process.readError()).getFormattedMessage();
                throw ExceptionsHelper.serverError(errorMsg, e);
            }
        }
    }

    /**
     * Extracts {@link MemoryUsageEstimationResult} from process' output.
     */
    private static MemoryUsageEstimationResult readResult(String jobId, AnalyticsProcess<MemoryUsageEstimationResult> process) {
        Iterator<MemoryUsageEstimationResult> iterator = process.readAnalyticsResults();
        if (iterator.hasNext() == false) {
            String errorMsg =
                new ParameterizedMessage("[{}] Memory usage estimation process returned no results", jobId).getFormattedMessage();
            throw ExceptionsHelper.serverError(errorMsg);
        }
        MemoryUsageEstimationResult result = iterator.next();
        if (iterator.hasNext()) {
            String errorMsg =
                new ParameterizedMessage("[{}] Memory usage estimation process returned more than one result", jobId).getFormattedMessage();
            throw ExceptionsHelper.serverError(errorMsg);
        }
        return result;
    }
}
