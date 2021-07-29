/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.job.retention.ExpiredResultsRemover.latestBucketTime;

/**
 * Removes all the automatically created annotations that have expired the configured retention time of their respective job.
 * An annotation is deleted if its timestamp is earlier than the timestamp of the latest bucket minus the retention period.
 *
 * This is expected to be used by actions requiring admin rights. Thus,
 * it is also expected that the provided client will be a client with the
 * ML origin so that permissions to manage ML indices are met.
 */
public class ExpiredAnnotationsRemover extends AbstractExpiredJobDataRemover {

    private static final Logger LOGGER = LogManager.getLogger(ExpiredAnnotationsRemover.class);

    private final AnomalyDetectionAuditor auditor;
    private final ThreadPool threadPool;

    public ExpiredAnnotationsRemover(OriginSettingClient client, Iterator<Job> jobIterator, TaskId parentTaskId,
                                     AnomalyDetectionAuditor auditor, ThreadPool threadPool) {
        super(client, jobIterator, parentTaskId);
        this.auditor = Objects.requireNonNull(auditor);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    Long getRetentionDays(Job job) {
        return job.getSystemAnnotationsRetentionDays();
    }

    @Override
    protected void removeDataBefore(
        Job job,
        float requestsPerSecond,
        long latestTimeMs,
        long cutoffEpochMs,
        ActionListener<Boolean> listener
    ) {
        DeleteByQueryRequest request = createDBQRequest(job, requestsPerSecond, cutoffEpochMs);
        request.setParentTask(getParentTaskId());

        client.execute(DeleteByQueryAction.INSTANCE, request, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                try {
                    if (bulkByScrollResponse.getDeleted() > 0) {
                        auditAnnotationsWereDeleted(job.getId(), cutoffEpochMs);
                    }
                    listener.onResponse(true);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new ElasticsearchException("Failed to remove expired annotations for job [" + job.getId() + "]", e));
            }
        });
    }

    private static DeleteByQueryRequest createDBQRequest(Job job, float requestsPerSec, long cutoffEpochMs) {
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), job.getId()))
            .filter(QueryBuilders.rangeQuery(Annotation.TIMESTAMP.getPreferredName()).lt(cutoffEpochMs).format("epoch_millis"))
            .filter(QueryBuilders.termQuery(Annotation.CREATE_USERNAME.getPreferredName(), XPackUser.NAME));
        DeleteByQueryRequest request = new DeleteByQueryRequest(AnnotationIndex.READ_ALIAS_NAME)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .setBatchSize(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE)
            // We are deleting old data, we should simply proceed as a version conflict could mean that another deletion is taking place
            .setAbortOnVersionConflict(false)
            .setTimeout(DEFAULT_MAX_DURATION)
            .setRequestsPerSecond(requestsPerSec)
            .setQuery(query);
        return request;
    }

    @Override
    void calcCutoffEpochMs(String jobId, long retentionDays, ActionListener<CutoffDetails> listener) {
        ThreadedActionListener<CutoffDetails> threadedActionListener = new ThreadedActionListener<>(LOGGER, threadPool,
                MachineLearning.UTILITY_THREAD_POOL_NAME, listener, false);
        latestBucketTime(client, getParentTaskId(), jobId, ActionListener.wrap(
                latestTime -> {
                    if (latestTime == null) {
                        threadedActionListener.onResponse(null);
                    } else {
                        long cutoff = latestTime - new TimeValue(retentionDays, TimeUnit.DAYS).getMillis();
                        threadedActionListener.onResponse(new CutoffDetails(latestTime, cutoff));
                    }
                },
                listener::onFailure
        ));
    }

    private void auditAnnotationsWereDeleted(String jobId, long cutoffEpochMs) {
        Instant instant = Instant.ofEpochMilli(cutoffEpochMs);
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.systemDefault());
        String formatted = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zonedDateTime);
        String msg = Messages.getMessage(Messages.JOB_AUDIT_OLD_ANNOTATIONS_DELETED, formatted);
        LOGGER.debug("[{}] {}", jobId, msg);
        auditor.info(jobId, msg);
    }
}
