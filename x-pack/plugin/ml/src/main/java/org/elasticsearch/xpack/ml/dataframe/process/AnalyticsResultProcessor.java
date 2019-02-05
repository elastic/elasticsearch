/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AnalyticsResultProcessor {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsResultProcessor.class);

    private final DataFrameRowsJoiner dataFrameRowsJoiner;
    private final CountDownLatch completionLatch = new CountDownLatch(1);

    public AnalyticsResultProcessor(DataFrameRowsJoiner dataFrameRowsJoiner) {
        this.dataFrameRowsJoiner = Objects.requireNonNull(dataFrameRowsJoiner);
    }

    public void awaitForCompletion() {
        try {
            if (completionLatch.await(30, TimeUnit.MINUTES) == false) {
                LOGGER.warn("Timeout waiting for results processor to complete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("Interrupted waiting for results processor to complete");
        }
    }

    public void process(AnalyticsProcess process) {

        try {
            Iterator<AnalyticsResult> iterator = process.readAnalyticsResults();
            while (iterator.hasNext()) {
                AnalyticsResult result = iterator.next();
                processResult(result);
            }
        } catch (Exception e) {
            LOGGER.error("Error parsing data frame analytics output", e);
        } finally {
            completionLatch.countDown();
            process.consumeAndCloseOutputStream();
        }
    }

    private void processResult(AnalyticsResult result) {
        RowResults rowResults = result.getRowResults();
        if (rowResults != null) {
            dataFrameRowsJoiner.processRowResults(rowResults);
        }
    }
}
