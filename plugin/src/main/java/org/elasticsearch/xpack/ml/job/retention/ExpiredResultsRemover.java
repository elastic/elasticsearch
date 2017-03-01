/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.bulk.byscroll.DeleteByQueryRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ml.action.MlDeleteByQueryAction;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.function.Function;

/**
 * Removes all results that have expired the configured retention time
 * of their respective job. A result is deleted if its timestamp is earlier
 * than the start of the current day (local time-zone) minus the retention
 * period.
 */
public class ExpiredResultsRemover extends AbstractExpiredJobDataRemover {

    private static final Logger LOGGER = Loggers.getLogger(ExpiredResultsRemover.class);

    private final Client client;
    private final Auditor auditor;

    public ExpiredResultsRemover(Client client, ClusterService clusterService, Auditor auditor) {
        super(clusterService);
        this.client = Objects.requireNonNull(client);
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected Long getRetentionDays(Job job) {
        return job.getResultsRetentionDays();
    }

    @Override
    protected void removeDataBefore(Job job, long cutoffEpochMs, Runnable onFinish) {
        LOGGER.info("Removing results of job [{}] that have a timestamp before [{}]", job.getId(), cutoffEpochMs);
        QueryBuilder excludeFilter = QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ModelSizeStats.RESULT_TYPE_VALUE);
        DeleteByQueryRequest request = createDBQRequest(job, Result.TYPE.getPreferredName(), cutoffEpochMs, excludeFilter);

        client.execute(MlDeleteByQueryAction.INSTANCE, request, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                try {
                    auditResultsWereDeleted(job.getId(), cutoffEpochMs);
                    onFinish.run();
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.error("Failed to remove expired results for job [" + job.getId() + "]", e);
                onFinish.run();
            }
        });
    }

    private DeleteByQueryRequest createDBQRequest(Job job, String type, long cutoffEpochMs, QueryBuilder excludeFilter) {
        SearchRequest searchRequest = new SearchRequest();
        // We need to create the DeleteByQueryRequest before we modify the SearchRequest
        // because the constructor of the former wipes the latter
        DeleteByQueryRequest request = new DeleteByQueryRequest(searchRequest);
        request.setSlices(5);

        searchRequest.indices(AnomalyDetectorsIndex.jobResultsAliasedName(job.getId()));
        searchRequest.types(type);
        QueryBuilder query = createQuery(job.getId(), cutoffEpochMs).mustNot(excludeFilter);
        searchRequest.source(new SearchSourceBuilder().query(query));
        return request;
    }

    private void auditResultsWereDeleted(String jobId, long cutoffEpochMs) {
        Instant instant = Instant.ofEpochMilli(cutoffEpochMs);
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.systemDefault());
        String formatted = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zonedDateTime);
        String msg = Messages.getMessage(Messages.JOB_AUDIT_OLD_RESULTS_DELETED, formatted);
        auditor.info(jobId, msg);
    }
}
