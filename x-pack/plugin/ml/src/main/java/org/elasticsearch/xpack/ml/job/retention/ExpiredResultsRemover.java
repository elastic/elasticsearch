/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Removes all results that have expired the configured retention time
 * of their respective job. A result is deleted if its timestamp is earlier
 * than the start of the current day (local time-zone) minus the retention
 * period.
 *
 * This is expected to be used by actions requiring admin rights. Thus,
 * it is also expected that the provided client will be a client with the
 * ML origin so that permissions to manage ML indices are met.
 */
public class ExpiredResultsRemover extends AbstractExpiredJobDataRemover {

    private static final Logger LOGGER = LogManager.getLogger(ExpiredResultsRemover.class);

    private final OriginSettingClient client;
    private final AnomalyDetectionAuditor auditor;

    public ExpiredResultsRemover(OriginSettingClient client, AnomalyDetectionAuditor auditor) {
        super(client);
        this.client = Objects.requireNonNull(client);
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected Long getRetentionDays(Job job) {
        return job.getResultsRetentionDays();
    }

    @Override
    protected void removeDataBefore(Job job, long cutoffEpochMs, ActionListener<Boolean> listener) {
        LOGGER.debug("Removing results of job [{}] that have a timestamp before [{}]", job.getId(), cutoffEpochMs);
        DeleteByQueryRequest request = createDBQRequest(job, cutoffEpochMs);

        client.execute(DeleteByQueryAction.INSTANCE, request, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                try {
                    if (bulkByScrollResponse.getDeleted() > 0) {
                        auditResultsWereDeleted(job.getId(), cutoffEpochMs);
                    }
                    listener.onResponse(true);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new ElasticsearchException("Failed to remove expired results for job [" + job.getId() + "]", e));
            }
        });
    }

    private DeleteByQueryRequest createDBQRequest(Job job, long cutoffEpochMs) {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);

        // Delete the documents gradually.
        // With DEFAULT_SCROLL_SIZE = 1000 this implies we spread deletion of 1 million documents over 5000 seconds ~= 83 minutes.
        request.setBatchSize(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE);
        request.setRequestsPerSecond(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE / 5);

        request.indices(AnomalyDetectorsIndex.jobResultsAliasedName(job.getId()));
        QueryBuilder excludeFilter = QueryBuilders.termsQuery(Result.RESULT_TYPE.getPreferredName(),
                ModelSizeStats.RESULT_TYPE_VALUE, ForecastRequestStats.RESULT_TYPE_VALUE, Forecast.RESULT_TYPE_VALUE);
        QueryBuilder query = createQuery(job.getId(), cutoffEpochMs)
                .filter(QueryBuilders.existsQuery(Result.RESULT_TYPE.getPreferredName()))
                .mustNot(excludeFilter);
        request.setQuery(query);

        // _doc is the most efficient sort order and will also disable scoring
        request.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);
        return request;
    }

    private void auditResultsWereDeleted(String jobId, long cutoffEpochMs) {
        Instant instant = Instant.ofEpochMilli(cutoffEpochMs);
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.systemDefault());
        String formatted = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zonedDateTime);
        String msg = Messages.getMessage(Messages.JOB_AUDIT_OLD_RESULTS_DELETED, formatted);
        LOGGER.debug("[{}] {}", jobId, msg);
        auditor.info(jobId, msg);
    }
}
