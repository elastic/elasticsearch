/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Updates job data counts, i.e. the number of processed records, fields etc.
 * One instance of this class handles updates for all jobs.
 */
public class JobDataCountsPersister {

    private static final Logger logger = LogManager.getLogger(JobDataCountsPersister.class);

    private final ResultsPersisterService resultsPersisterService;
    private final Client client;
    private final AnomalyDetectionAuditor auditor;

    private final Map<String, CountDownLatch> ongoingPersists = new ConcurrentHashMap<>();

    public JobDataCountsPersister(Client client, ResultsPersisterService resultsPersisterService, AnomalyDetectionAuditor auditor) {
        this.resultsPersisterService = resultsPersisterService;
        this.client = client;
        this.auditor = auditor;
    }

    private static XContentBuilder serialiseCounts(DataCounts counts) throws IOException {
        XContentBuilder builder = jsonBuilder();
        return counts.toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    /**
     * Update a job's data counts stats and figures.
     * If the previous call for the same job is still in progress
     * @param jobId Job to update.
     * @param counts The counts.
     * @param mustWait Whether to wait for the counts to be persisted.
     *                 This will involve waiting for the supplied counts
     *                 and also potentially the previous counts to be
     *                 persisted if that previous persist is still ongoing.
     * @return <code>true</code> if the counts were sent for persistence, or <code>false</code>
     *         if the previous persist was still in progress.
     */
    public boolean persistDataCounts(String jobId, DataCounts counts, boolean mustWait) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch previousLatch = ongoingPersists.putIfAbsent(jobId, latch);
        while (previousLatch != null) {
            if (mustWait == false) {
                return false;
            }
            previousLatch.await();
            previousLatch = ongoingPersists.putIfAbsent(jobId, latch);
        }
        counts.setLogTime(Instant.now());
        try {
            resultsPersisterService.indexWithRetry(
                jobId,
                AnomalyDetectorsIndex.resultsWriteAlias(jobId),
                counts,
                ToXContent.EMPTY_PARAMS,
                WriteRequest.RefreshPolicy.NONE,
                DataCounts.documentId(jobId),
                true,
                () -> true,
                retryMessage -> logger.debug("[{}] Job data_counts {}", jobId, retryMessage),
                ActionListener.wrap(r -> ongoingPersists.remove(jobId).countDown(), e -> {
                    ongoingPersists.remove(jobId).countDown();
                    logger.error(() -> "[" + jobId + "] Failed persisting data_counts stats", e);
                    auditor.error(jobId, "Failed persisting data_counts stats: " + e.getMessage());
                })
            );
        } catch (IOException e) {
            // An exception caught here basically means toXContent() failed, which should never happen
            logger.error(() -> "[" + jobId + "] Failed writing data_counts stats", e);
            return false;
        }
        if (mustWait) {
            latch.await();
        }
        return true;
    }

    /**
     * Very similar to {@link JobDataCountsPersister#persistDataCounts(String, DataCounts, boolean)}.
     * <p>
     * Two differences are:
     *  - The caller is notified on persistence failure
     *  - If the persistence fails, it is not automatically retried
     * @param jobId Job to update
     * @param counts The counts
     * @param listener ActionType response listener
     */
    public void persistDataCountsAsync(String jobId, DataCounts counts, ActionListener<Boolean> listener) {
        counts.setLogTime(Instant.now());
        try (XContentBuilder content = serialiseCounts(counts)) {
            final IndexRequest request = new IndexRequest(AnomalyDetectorsIndex.resultsWriteAlias(jobId)).id(DataCounts.documentId(jobId))
                .setRequireAlias(true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(content);
            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                IndexAction.INSTANCE,
                request,
                listener.delegateFailure((l, r) -> l.onResponse(true))
            );
        } catch (IOException ioe) {
            String msg = "[" + jobId + "] Failed writing data_counts stats";
            logger.error(msg, ioe);
            listener.onFailure(ExceptionsHelper.serverError(msg, ioe));
        }
    }
}
