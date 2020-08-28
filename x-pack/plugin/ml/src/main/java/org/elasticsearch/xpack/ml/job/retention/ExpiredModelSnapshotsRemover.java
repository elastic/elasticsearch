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
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotField;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;
import org.elasticsearch.xpack.ml.utils.VolatileCursorIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Deletes all model snapshots that have expired the configured retention time
 * of their respective job with the exception of the currently used snapshot.
 * A snapshot is deleted if its timestamp is earlier than the start of the
 * current day (local time-zone) minus the retention period.
 *
 * This is expected to be used by actions requiring admin rights. Thus,
 * it is also expected that the provided client will be a client with the
 * ML origin so that permissions to manage ML indices are met.
 */
public class ExpiredModelSnapshotsRemover extends AbstractExpiredJobDataRemover {

    private static final Logger LOGGER = LogManager.getLogger(ExpiredModelSnapshotsRemover.class);

    private static final long MS_IN_ONE_DAY = TimeValue.timeValueDays(1).getMillis();

    /**
     *  The max number of snapshots to fetch per job. It is set to 10K, the default for an index as
     *  we don't change that in our ML indices. It should be more than enough for most cases. If not,
     *  it will take a few iterations to delete all snapshots, which is OK.
     */
    private static final int MODEL_SNAPSHOT_SEARCH_SIZE = 10000;

    private final ThreadPool threadPool;

    public ExpiredModelSnapshotsRemover(OriginSettingClient client, Iterator<Job> jobIterator, ThreadPool threadPool) {
        super(client, jobIterator);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    Long getRetentionDays(Job job) {
        // If a daily retention cutoff is set then we need to tell the base class that this is the cutoff
        // point so that we get to consider deleting model snapshots older than this. Later on we will
        // not actually delete all of the ones in between the hard cutoff and the daily retention cutoff.
        Long retentionDaysForConsideration = job.getDailyModelSnapshotRetentionAfterDays();
        if (retentionDaysForConsideration == null) {
            retentionDaysForConsideration = job.getModelSnapshotRetentionDays();
        }
        return retentionDaysForConsideration;
    }

    @Override
    void calcCutoffEpochMs(String jobId, long retentionDays, ActionListener<CutoffDetails> listener) {
        ThreadedActionListener<CutoffDetails> threadedActionListener = new ThreadedActionListener<>(LOGGER, threadPool,
                MachineLearning.UTILITY_THREAD_POOL_NAME, listener, false);

        latestSnapshotTimeStamp(jobId, ActionListener.wrap(
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

    private void latestSnapshotTimeStamp(String jobId, ActionListener<Long> listener) {
        SortBuilder<?> sortBuilder = new FieldSortBuilder(ModelSnapshot.TIMESTAMP.getPreferredName()).order(SortOrder.DESC);
        QueryBuilder snapshotQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.existsQuery(ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName()))
                .filter(QueryBuilders.existsQuery(ModelSnapshot.TIMESTAMP.getPreferredName()));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.sort(sortBuilder);
        searchSourceBuilder.query(snapshotQuery);
        searchSourceBuilder.size(1);
        searchSourceBuilder.trackTotalHits(false);
        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.docValueField(ModelSnapshot.TIMESTAMP.getPreferredName(), "epoch_millis");

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));

        client.search(searchRequest, ActionListener.wrap(
                response -> {
                    SearchHit[] hits = response.getHits().getHits();
                    if (hits.length == 0) {
                        // no snapshots found
                        listener.onResponse(null);
                    } else {
                        String timestamp = stringFieldValueOrNull(hits[0], ModelSnapshot.TIMESTAMP.getPreferredName());
                        if (timestamp == null) {
                            LOGGER.warn("Model snapshot document [{}] has a null timestamp field", hits[0].getId());
                            listener.onResponse(null);
                        } else {
                            long timestampMs = TimeUtils.parseToEpochMs(timestamp);
                            listener.onResponse(timestampMs);
                        }
                    }
                },
                listener::onFailure)
        );
    }

    @Override
    protected void removeDataBefore(
        Job job,
        float requestsPerSec,
        long latestTimeMs,
        long cutoffEpochMs,
        ActionListener<Boolean> listener
    ) {
        // TODO: delete this test if we ever allow users to revert a job to no model snapshot, e.g. to recover from data loss
        if (job.getModelSnapshotId() == null) {
            // No snapshot to remove
            listener.onResponse(true);
            return;
        }
        LOGGER.debug("Considering model snapshots of job [{}] that have a timestamp before [{}] for removal", job.getId(), cutoffEpochMs);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(AnomalyDetectorsIndex.jobResultsAliasedName(job.getId()));

        QueryBuilder activeSnapshotFilter = QueryBuilders.termQuery(
            ModelSnapshotField.SNAPSHOT_ID.getPreferredName(), job.getModelSnapshotId());
        QueryBuilder retainFilter = QueryBuilders.termQuery(ModelSnapshot.RETAIN.getPreferredName(), true);
        QueryBuilder query = createQuery(job.getId(), cutoffEpochMs)
            .filter(QueryBuilders.existsQuery(ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName()))
            .mustNot(activeSnapshotFilter)
            .mustNot(retainFilter);

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        source.size(MODEL_SNAPSHOT_SEARCH_SIZE);
        source.sort(ModelSnapshot.TIMESTAMP.getPreferredName());
        source.fetchSource(false);
        source.docValueField(Job.ID.getPreferredName(), null);
        source.docValueField(ModelSnapshotField.SNAPSHOT_ID.getPreferredName(), null);
        source.docValueField(ModelSnapshot.TIMESTAMP.getPreferredName(), "epoch_millis");
        searchRequest.source(source);

        long deleteAllBeforeMs = (job.getModelSnapshotRetentionDays() == null)
            ? 0 : latestTimeMs - TimeValue.timeValueDays(job.getModelSnapshotRetentionDays()).getMillis();
        client.execute(SearchAction.INSTANCE, searchRequest, new ThreadedActionListener<>(LOGGER, threadPool,
            MachineLearning.UTILITY_THREAD_POOL_NAME, expiredSnapshotsListener(job.getId(), deleteAllBeforeMs, listener), false));
    }

    private ActionListener<SearchResponse> expiredSnapshotsListener(String jobId, long deleteAllBeforeMs,
                                                                    ActionListener<Boolean> listener) {
        return new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                long nextToKeepMs = deleteAllBeforeMs;
                try {
                    List<JobSnapshotId> snapshotIds = new ArrayList<>();
                    for (SearchHit hit : searchResponse.getHits()) {
                        String timestamp = stringFieldValueOrNull(hit, ModelSnapshot.TIMESTAMP.getPreferredName());
                        if (timestamp == null) {
                            LOGGER.warn("Model snapshot document [{}] has a null timestamp field", hit.getId());
                            continue;
                        }
                        long timestampMs = TimeUtils.parseToEpochMs(timestamp);
                        if (timestampMs >= nextToKeepMs) {
                            do {
                                nextToKeepMs += MS_IN_ONE_DAY;
                            } while (timestampMs >= nextToKeepMs);
                            continue;
                        }
                        JobSnapshotId idPair = new JobSnapshotId(
                            stringFieldValueOrNull(hit, Job.ID.getPreferredName()),
                            stringFieldValueOrNull(hit, ModelSnapshotField.SNAPSHOT_ID.getPreferredName()));

                        if (idPair.hasNullValue() == false) {
                            snapshotIds.add(idPair);
                        }
                    }
                    deleteModelSnapshots(new VolatileCursorIterator<>(snapshotIds), listener);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new ElasticsearchException("[" + jobId +  "] Search for expired snapshots failed", e));
            }
        };
    }

    private void deleteModelSnapshots(Iterator<JobSnapshotId> modelSnapshotIterator, ActionListener<Boolean> listener) {
        if (modelSnapshotIterator.hasNext() == false) {
            listener.onResponse(true);
            return;
        }
        JobSnapshotId idPair = modelSnapshotIterator.next();
        DeleteModelSnapshotAction.Request deleteSnapshotRequest =
            new DeleteModelSnapshotAction.Request(idPair.jobId, idPair.snapshotId);
        client.execute(DeleteModelSnapshotAction.INSTANCE, deleteSnapshotRequest, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    try {
                        deleteModelSnapshots(modelSnapshotIterator, listener);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(new ElasticsearchException("[" + idPair.jobId +  "] Failed to delete snapshot ["
                            + idPair.snapshotId + "]", e));
                }
            });
    }

    static class JobSnapshotId {
        private final String jobId;
        private final String snapshotId;

        JobSnapshotId(String jobId, String snapshotId) {
            this.jobId = jobId;
            this.snapshotId = snapshotId;
        }

        boolean hasNullValue() {
            return jobId == null || snapshotId == null;
        }
    }
}
