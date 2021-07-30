/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.time.Instant;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Update a job's dataCounts
 * i.e. the number of processed records, fields etc.
 */
public class JobDataCountsPersister {

    private static final Logger logger = LogManager.getLogger(JobDataCountsPersister.class);

    private final ResultsPersisterService resultsPersisterService;
    private final Client client;
    private final AnomalyDetectionAuditor auditor;

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
     * Update the job's data counts stats and figures.
     * NOTE: This call is synchronous and pauses the calling thread.
     * @param jobId Job to update
     * @param counts The counts
     */
    public void persistDataCounts(String jobId, DataCounts counts) {
        counts.setLogTime(Instant.now());
        try {
            resultsPersisterService.indexWithRetry(jobId,
                AnomalyDetectorsIndex.resultsWriteAlias(jobId),
                counts,
                ToXContent.EMPTY_PARAMS,
                WriteRequest.RefreshPolicy.NONE,
                DataCounts.documentId(jobId),
                true,
                () -> true,
                retryMessage -> logger.debug("[{}] Job data_counts {}", jobId, retryMessage));
        } catch (IOException ioe) {
            logger.error(() -> new ParameterizedMessage("[{}] Failed writing data_counts stats", jobId), ioe);
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("[{}] Failed persisting data_counts stats", jobId), ex);
            auditor.error(jobId, "Failed persisting data_counts stats: " + ex.getMessage());
        }
    }

    /**
     * The same as {@link JobDataCountsPersister#persistDataCounts(String, DataCounts)} but done Asynchronously.
     *
     * Two differences are:
     *  - The listener is notified on persistence failure
     *  - If the persistence fails, it is not automatically retried
     * @param jobId Job to update
     * @param counts The counts
     * @param listener ActionType response listener
     */
    public void persistDataCountsAsync(String jobId, DataCounts counts, ActionListener<Boolean> listener) {
        counts.setLogTime(Instant.now());
        try (XContentBuilder content = serialiseCounts(counts)) {
            final IndexRequest request = new IndexRequest(AnomalyDetectorsIndex.resultsWriteAlias(jobId))
                .id(DataCounts.documentId(jobId))
                .setRequireAlias(true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(content);
            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, request,
                    listener.delegateFailure((l, r) -> l.onResponse(true)));
        } catch (IOException ioe) {
            String msg = new ParameterizedMessage("[{}] Failed writing data_counts stats", jobId).getFormattedMessage();
            logger.error(msg, ioe);
            listener.onFailure(ExceptionsHelper.serverError(msg, ioe));
        }
    }
}
