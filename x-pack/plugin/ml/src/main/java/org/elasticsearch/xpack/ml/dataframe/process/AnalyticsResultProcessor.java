/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask.ProgressTracker;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

public class AnalyticsResultProcessor {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsResultProcessor.class);

    private final String dataFrameAnalyticsId;
    private final DataFrameRowsJoiner dataFrameRowsJoiner;
    private final Supplier<Boolean> isProcessKilled;
    private final ProgressTracker progressTracker;
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private volatile String failure;

    public AnalyticsResultProcessor(String dataFrameAnalyticsId, DataFrameRowsJoiner dataFrameRowsJoiner, Supplier<Boolean> isProcessKilled,
                                    ProgressTracker progressTracker) {
        this.dataFrameAnalyticsId = Objects.requireNonNull(dataFrameAnalyticsId);
        this.dataFrameRowsJoiner = Objects.requireNonNull(dataFrameRowsJoiner);
        this.isProcessKilled = Objects.requireNonNull(isProcessKilled);
        this.progressTracker = Objects.requireNonNull(progressTracker);
    }

    @Nullable
    public String getFailure() {
        return failure == null ? dataFrameRowsJoiner.getFailure() : failure;
    }

    public void awaitForCompletion() {
        try {
            completionLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error(new ParameterizedMessage("[{}] Interrupted waiting for results processor to complete", dataFrameAnalyticsId), e);
        }
    }

    public void process(AnalyticsProcess<AnalyticsResult> process) {
        long totalRows = process.getConfig().rows();
        long processedRows = 0;

        // TODO When java 9 features can be used, we will not need the local variable here
        try (DataFrameRowsJoiner resultsJoiner = dataFrameRowsJoiner) {
            Iterator<AnalyticsResult> iterator = process.readAnalyticsResults();
            while (iterator.hasNext()) {
                AnalyticsResult result = iterator.next();
                processResult(result, resultsJoiner);
                if (result.getRowResults() != null) {
                    processedRows++;
                    progressTracker.writingResultsPercent.set(processedRows >= totalRows ? 100 : (int) (processedRows * 100.0 / totalRows));
                }
            }
            if (isProcessKilled.get() == false) {
                // This means we completed successfully so we need to set the progress to 100.
                // This is because due to skipped rows, it is possible the processed rows will not reach the total rows.
                progressTracker.writingResultsPercent.set(100);
            }
        } catch (Exception e) {
            if (isProcessKilled.get()) {
                // No need to log error as it's due to stopping
            } else {
                LOGGER.error(new ParameterizedMessage("[{}] Error parsing data frame analytics output", dataFrameAnalyticsId), e);
                failure = "error parsing data frame analytics output: [" + e.getMessage() + "]";
            }
        } finally {
            completionLatch.countDown();
            process.consumeAndCloseOutputStream();
        }
    }

    private void processResult(AnalyticsResult result, DataFrameRowsJoiner resultsJoiner) {
        RowResults rowResults = result.getRowResults();
        if (rowResults != null) {
            resultsJoiner.processRowResults(rowResults);
        }
        Integer progressPercent = result.getProgressPercent();
        if (progressPercent != null) {
            progressTracker.analyzingPercent.set(progressPercent);
        }
    }
}
