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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Removes all results that have expired the configured retention time
 * of their respective job. A result is deleted if its timestamp is earlier
 * than the timestamp of the latest bucket minus the retention period.
 *
 * This is expected to be used by actions requiring admin rights. Thus,
 * it is also expected that the provided client will be a client with the
 * ML origin so that permissions to manage ML indices are met.
 */
public class ExpiredResultsRemover extends AbstractExpiredJobDataRemover {

    private static final Logger LOGGER = LogManager.getLogger(ExpiredResultsRemover.class);

    private final AnomalyDetectionAuditor auditor;
    private final ThreadPool threadPool;

    public ExpiredResultsRemover(OriginSettingClient client, Iterator<Job> jobIterator, TaskId parentTaskId,
                                 AnomalyDetectionAuditor auditor, ThreadPool threadPool) {
        super(client, jobIterator, parentTaskId);
        this.auditor = Objects.requireNonNull(auditor);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    Long getRetentionDays(Job job) {
        return job.getResultsRetentionDays();
    }

    @Override
    protected void removeDataBefore(
        Job job,
        float requestsPerSecond,
        long latestTimeMs,
        long cutoffEpochMs,
        ActionListener<Boolean> listener
    ) {
        LOGGER.debug("Removing results of job [{}] that have a timestamp before [{}]", job.getId(), cutoffEpochMs);
        DeleteByQueryRequest request = createDBQRequest(job, requestsPerSecond, cutoffEpochMs);
        request.setParentTask(getParentTaskId());

        client.execute(DeleteByQueryAction.INSTANCE, request, new ActionListener<>() {
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

    private DeleteByQueryRequest createDBQRequest(Job job, float requestsPerSec, long cutoffEpochMs) {
        QueryBuilder excludeFilter = QueryBuilders.termsQuery(
            Result.RESULT_TYPE.getPreferredName(),
            ModelSizeStats.RESULT_TYPE_VALUE,
            ForecastRequestStats.RESULT_TYPE_VALUE,
            Forecast.RESULT_TYPE_VALUE);
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), job.getId()))
            .filter(QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName()).lt(cutoffEpochMs).format("epoch_millis"))
            .filter(QueryBuilders.existsQuery(Result.RESULT_TYPE.getPreferredName()))
            .mustNot(excludeFilter);
        DeleteByQueryRequest request = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobResultsAliasedName(job.getId()))
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .setBatchSize(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE)
            // We are deleting old data, we should simply proceed as a version conflict could mean that another deletion is taking place
            .setAbortOnVersionConflict(false)
            .setTimeout(DEFAULT_MAX_DURATION)
            .setRequestsPerSecond(requestsPerSec)
            .setQuery(query);

        // _doc is the most efficient sort order and will also disable scoring
        request.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);
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

    static void latestBucketTime(OriginSettingClient client, TaskId parentTaskId, String jobId, ActionListener<Long> listener) {
        SortBuilder<?> sortBuilder = new FieldSortBuilder(Result.TIMESTAMP.getPreferredName()).order(SortOrder.DESC);
        QueryBuilder bucketType = QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), Bucket.RESULT_TYPE_VALUE);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.sort(sortBuilder);
        searchSourceBuilder.query(bucketType);
        searchSourceBuilder.size(1);
        searchSourceBuilder.trackTotalHits(false);

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));
        searchRequest.setParentTask(parentTaskId);

        client.search(searchRequest, ActionListener.wrap(
                response -> {
                    SearchHit[] hits = response.getHits().getHits();
                    if (hits.length == 0) {
                        // no buckets found
                        listener.onResponse(null);
                    } else {

                        try (InputStream stream = hits[0].getSourceRef().streamInput();
                             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                                     .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                            Bucket bucket = Bucket.LENIENT_PARSER.apply(parser, null);
                            listener.onResponse(bucket.getTimestamp().getTime());
                        } catch (IOException e) {
                            listener.onFailure(new ElasticsearchParseException("failed to parse bucket", e));
                        }
                    }
                }, listener::onFailure
        ));
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
