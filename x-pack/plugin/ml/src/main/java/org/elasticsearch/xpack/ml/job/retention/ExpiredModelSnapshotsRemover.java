/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

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
     * The max number of snapshots to fetch per job. It is set to 10K, the default for an index as
     * we don't change that in our ML indices. It should be more than enough for most cases. If not,
     * it will take a few iterations to delete all snapshots, which is OK.
     */
    private static final int MODEL_SNAPSHOT_SEARCH_SIZE = 10000;

    private final ThreadPool threadPool;
    private final JobResultsProvider jobResultsProvider;
    private final AnomalyDetectionAuditor auditor;

    public ExpiredModelSnapshotsRemover(
        OriginSettingClient client,
        Iterator<Job> jobIterator,
        TaskId parentTaskId,
        ThreadPool threadPool,
        JobResultsProvider jobResultsProvider,
        AnomalyDetectionAuditor auditor
    ) {
        super(client, jobIterator, parentTaskId);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.jobResultsProvider = jobResultsProvider;
        this.auditor = auditor;
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
        latestSnapshotTimeStamp(jobId, listener.delegateFailureAndWrap((l, latestTime) -> {
            ThreadedActionListener<CutoffDetails> threadedActionListener = new ThreadedActionListener<>(
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
                l
            );
            if (latestTime == null) {
                threadedActionListener.onResponse(null);
            } else {
                long cutoff = latestTime - new TimeValue(retentionDays, TimeUnit.DAYS).getMillis();
                threadedActionListener.onResponse(new CutoffDetails(latestTime, cutoff));
            }
        }));
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
        searchRequest.setParentTask(getParentTaskId());

        client.search(searchRequest, listener.delegateFailureAndWrap((delegate, response) -> {
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length == 0) {
                // no snapshots found
                delegate.onResponse(null);
            } else {
                String timestamp = stringFieldValueOrNull(hits[0], ModelSnapshot.TIMESTAMP.getPreferredName());
                if (timestamp == null) {
                    LOGGER.warn("Model snapshot document [{}] has a null timestamp field", hits[0].getId());
                    delegate.onResponse(null);
                } else {
                    long timestampMs = TimeUtils.parseToEpochMs(timestamp);
                    delegate.onResponse(timestampMs);
                }
            }
        }));
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
        LOGGER.debug(
            () -> format(
                "Considering model snapshots of job [%s] that have a timestamp before [%s] for removal",
                job.getId(),
                cutoffEpochMs
            )
        );

        long deleteAllBeforeMs = (job.getModelSnapshotRetentionDays() == null)
            ? 0
            : latestTimeMs - TimeValue.timeValueDays(job.getModelSnapshotRetentionDays()).getMillis();
        ActionListener<QueryPage<ModelSnapshot>> snapshotsListener = expiredSnapshotsListener(job, deleteAllBeforeMs, listener);
        jobResultsProvider.modelSnapshots(
            job.getId(),
            0,
            MODEL_SNAPSHOT_SEARCH_SIZE,
            null,
            String.valueOf(cutoffEpochMs),
            ModelSnapshot.TIMESTAMP.getPreferredName(),
            false,
            null,
            null,
            snapshotsListener::onResponse,
            snapshotsListener::onFailure
        );
    }

    private ActionListener<QueryPage<ModelSnapshot>> expiredSnapshotsListener(
        Job job,
        long deleteAllBeforeMs,
        ActionListener<Boolean> listener
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(QueryPage<ModelSnapshot> searchResponse) {
                long nextToKeepMs = deleteAllBeforeMs;
                try {
                    List<ModelSnapshot> snapshots = new ArrayList<>();
                    for (ModelSnapshot snapshot : searchResponse.results()) {
                        // We don't want to delete the currently used snapshot or a snapshot marked to be retained
                        if (snapshot.getSnapshotId().equals(job.getModelSnapshotId()) || snapshot.isRetain()) {
                            continue;
                        }
                        if (snapshot.getTimestamp() == null) {
                            LOGGER.warn("Model snapshot document [{}] has a null timestamp field", snapshot.getSnapshotId());
                            continue;
                        }
                        long timestampMs = snapshot.getTimestamp().getTime();
                        if (timestampMs >= nextToKeepMs) {
                            do {
                                nextToKeepMs += MS_IN_ONE_DAY;
                            } while (timestampMs >= nextToKeepMs);
                            continue;
                        }
                        snapshots.add(snapshot);
                    }
                    deleteModelSnapshots(snapshots, job.getId(), listener);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "[{}] Search for expired snapshots failed",
                        RestStatus.TOO_MANY_REQUESTS,
                        e,
                        job.getId()
                    )
                );
            }
        };
    }

    private void deleteModelSnapshots(List<ModelSnapshot> modelSnapshots, String jobId, ActionListener<Boolean> listener) {
        if (modelSnapshots.isEmpty()) {
            listener.onResponse(true);
            return;
        }

        String stateIndexName = AnomalyDetectorsIndex.jobStateIndexPattern();

        List<String> idsToDelete = new ArrayList<>();
        Set<String> indices = new HashSet<>();
        indices.add(stateIndexName);
        indices.add(AnnotationIndex.READ_ALIAS_NAME);
        for (ModelSnapshot modelSnapshot : modelSnapshots) {
            idsToDelete.addAll(modelSnapshot.stateDocumentIds());
            idsToDelete.add(ModelSnapshot.documentId(modelSnapshot));
            idsToDelete.add(ModelSnapshot.annotationDocumentId(modelSnapshot));
            indices.add(AnomalyDetectorsIndex.jobResultsAliasedName(modelSnapshot.getJobId()));
        }

        // Remove read-only indices
        List<String> indicesToQuery;
        try {
            indicesToQuery = WritableIndexExpander.getInstance().getWritableIndices(indices);
        } catch (Exception e) {
            LOGGER.error("Failed to get writable indices for [" + jobId + "]", e);
            listener.onFailure(e);
            return;
        }
        if (indicesToQuery.isEmpty()) {
            LOGGER.info("No writable model snapshot indices found for [{}] job. No expired model snapshots to remove.", jobId);
            listener.onResponse(true);
            return;
        }

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(indicesToQuery.toArray(new String[0])).setRefresh(true)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.idsQuery().addIds(idsToDelete.toArray(new String[0])));

        // _doc is the most efficient sort order and will also disable scoring
        deleteByQueryRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            DeleteByQueryAction.INSTANCE,
            deleteByQueryRequest,
            listener.delegateFailureAndWrap((l, bulkResponse) -> {
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_SNAPSHOTS_DELETED, modelSnapshots.size()));
                LOGGER.debug(
                    () -> format(
                        "[%s] deleted model snapshots %s with descriptions %s",
                        jobId,
                        modelSnapshots.stream().map(ModelSnapshot::getSnapshotId).collect(toList()),
                        modelSnapshots.stream().map(ModelSnapshot::getDescription).collect(toList())
                    )
                );
                l.onResponse(true);
            })
        );
    }
}
