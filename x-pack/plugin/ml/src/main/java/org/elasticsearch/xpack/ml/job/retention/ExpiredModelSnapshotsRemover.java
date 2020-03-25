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
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
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

    /**
     *  The max number of snapshots to fetch per job. It is set to 10K, the default for an index as
     *  we don't change that in our ML indices. It should be more than enough for most cases. If not,
     *  it will take a few iterations to delete all snapshots, which is OK.
     */
    private static final int MODEL_SNAPSHOT_SEARCH_SIZE = 10000;

    private final OriginSettingClient client;
    private final ThreadPool threadPool;

    public ExpiredModelSnapshotsRemover(OriginSettingClient client, ThreadPool threadPool) {
        super(client);
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    Long getRetentionDays(Job job) {
        return job.getModelSnapshotRetentionDays();
    }

    @Override
    void calcCutoffEpochMs(String jobId, long retentionDays, ActionListener<Long> listener) {
        ThreadedActionListener<Long> threadedActionListener = new ThreadedActionListener<>(LOGGER, threadPool,
                MachineLearning.UTILITY_THREAD_POOL_NAME, listener, false);

        latestSnapshotTimeStamp(jobId, ActionListener.wrap(
                latestTime -> {
                    if (latestTime == null) {
                        threadedActionListener.onResponse(null);
                    } else {
                        long cutoff = latestTime - new TimeValue(retentionDays, TimeUnit.DAYS).getMillis();
                        threadedActionListener.onResponse(cutoff);
                    }
                },
                listener::onFailure
        ));
    }

    private void latestSnapshotTimeStamp(String jobId, ActionListener<Long> listener) {
        SortBuilder<?> sortBuilder = new FieldSortBuilder(ModelSnapshot.TIMESTAMP.getPreferredName()).order(SortOrder.DESC);
        QueryBuilder snapshotQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.existsQuery(ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName()));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.sort(sortBuilder);
        searchSourceBuilder.query(snapshotQuery);
        searchSourceBuilder.size(1);
        searchSourceBuilder.trackTotalHits(false);

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
                        ModelSnapshot snapshot = ModelSnapshot.fromJson(hits[0].getSourceRef());
                        listener.onResponse(snapshot.getTimestamp().getTime());
                    }
                },
                listener::onFailure)
        );
    }

    @Override
    protected void removeDataBefore(Job job, long cutoffEpochMs, ActionListener<Boolean> listener) {
        if (job.getModelSnapshotId() == null) {
            // No snapshot to remove
            listener.onResponse(true);
            return;
        }
        LOGGER.debug("Removing model snapshots of job [{}] that have a timestamp before [{}]", job.getId(), cutoffEpochMs);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(AnomalyDetectorsIndex.jobResultsAliasedName(job.getId()));

        QueryBuilder activeSnapshotFilter = QueryBuilders.termQuery(
                ModelSnapshotField.SNAPSHOT_ID.getPreferredName(), job.getModelSnapshotId());
        QueryBuilder retainFilter = QueryBuilders.termQuery(ModelSnapshot.RETAIN.getPreferredName(), true);
        QueryBuilder query = createQuery(job.getId(), cutoffEpochMs)
                .filter(QueryBuilders.existsQuery(ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName()))
                .mustNot(activeSnapshotFilter)
                .mustNot(retainFilter);

        searchRequest.source(new SearchSourceBuilder().query(query).size(MODEL_SNAPSHOT_SEARCH_SIZE).sort(ElasticsearchMappings.ES_DOC));

        client.execute(SearchAction.INSTANCE, searchRequest, new ThreadedActionListener<>(LOGGER, threadPool,
                MachineLearning.UTILITY_THREAD_POOL_NAME, expiredSnapshotsListener(job.getId(), listener), false));
    }

    private ActionListener<SearchResponse> expiredSnapshotsListener(String jobId, ActionListener<Boolean> listener) {
        return new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                try {
                    List<ModelSnapshot> modelSnapshots = new ArrayList<>();
                    for (SearchHit hit : searchResponse.getHits()) {
                        modelSnapshots.add(ModelSnapshot.fromJson(hit.getSourceRef()));
                    }
                    deleteModelSnapshots(new VolatileCursorIterator<>(modelSnapshots), listener);
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

    private void deleteModelSnapshots(Iterator<ModelSnapshot> modelSnapshotIterator, ActionListener<Boolean> listener) {
        if (modelSnapshotIterator.hasNext() == false) {
            listener.onResponse(true);
            return;
        }
        ModelSnapshot modelSnapshot = modelSnapshotIterator.next();
        DeleteModelSnapshotAction.Request deleteSnapshotRequest = new DeleteModelSnapshotAction.Request(
                modelSnapshot.getJobId(), modelSnapshot.getSnapshotId());
        client.execute(DeleteModelSnapshotAction.INSTANCE, deleteSnapshotRequest, new ActionListener<AcknowledgedResponse>() {
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
                    listener.onFailure(new ElasticsearchException("[" + modelSnapshot.getJobId() +  "] Failed to delete snapshot ["
                            + modelSnapshot.getSnapshotId() + "]", e));
                }
            });
    }
}
