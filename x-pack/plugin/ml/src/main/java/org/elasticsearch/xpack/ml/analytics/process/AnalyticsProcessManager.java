/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.analytics.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.env.Environment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.analytics.DataFrameDataExtractor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

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

    public void processData(String jobId, DataFrameDataExtractor dataExtractor) {
        threadPool.generic().execute(() -> {
            AnalyticsProcess process = createProcess(jobId);
            try {
                // Fake header
                process.writeRecord(dataExtractor.getFieldNamesArray());

                while (dataExtractor.hasNext()) {
                    Optional<List<String[]>> records = dataExtractor.next();
                    if (records.isPresent()) {
                        for (String[] record : records.get()) {
                            process.writeRecord(record);
                        }
                    }
                }
                process.flushStream();

                LOGGER.debug("[{}] Closing process", jobId);
                process.close();
                LOGGER.info("[{}] Closed process", jobId);
            } catch (IOException e) {

            } finally {
                try {
                    process.close();
                } catch (IOException e) {
                    LOGGER.error("[{}] Error closing data frame analyzer process", jobId);
                }
            }
        });
    }

    private AnalyticsProcess createProcess(String jobId) {
        // TODO We should rename the thread pool to reflect its more general use now, e.g. JOB_PROCESS_THREAD_POOL_NAME
        ExecutorService executorService = threadPool.executor(MachineLearning.AUTODETECT_THREAD_POOL_NAME);
        AnalyticsProcess process = processFactory.createAnalyticsProcess(jobId, executorService);
        if (process.isProcessAlive() == false) {
            throw ExceptionsHelper.serverError("Failed to start analytics process");
        }
        return process;
    }
}
