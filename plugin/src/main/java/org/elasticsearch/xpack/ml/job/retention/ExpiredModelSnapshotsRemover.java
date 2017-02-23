/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Deletes all model snapshots that have expired the configured retention time
 * of their respective job with the exception of the currently used snapshot.
 * A snapshot is deleted if its timestamp is earlier than the start of the
 * current day (local time-zone) minus the retention period.
 */
public class ExpiredModelSnapshotsRemover extends AbstractExpiredJobDataRemover {

    private static final Logger LOGGER = Loggers.getLogger(ExpiredModelSnapshotsRemover.class);

    /**
     *  The max number of snapshots to fetch per job. It is set to 10K, the default for an index as
     *  we don't change that in our ML indices. It should be more than enough for most cases. If not,
     *  it will take a few iterations to delete all snapshots, which is OK.
     */
    private static final int MODEL_SNAPSHOT_SEARCH_SIZE = 10000;

    private final Client client;

    public ExpiredModelSnapshotsRemover(Client client, ClusterService clusterService) {
        super(clusterService);
        this.client = Objects.requireNonNull(client);
    }

    @Override
    protected Long getRetentionDays(Job job) {
        return job.getModelSnapshotRetentionDays();
    }

    @Override
    protected void removeDataBefore(Job job, long cutoffEpochMs, Runnable onFinish) {
        if (job.getModelSnapshotId() == null) {
            // No snapshot to remove
            onFinish.run();
            return;
        }
        LOGGER.info("Removing model snapshots of job [{}] that have a timestamp before [{}]", job.getId(), cutoffEpochMs);
        QueryBuilder excludeFilter = QueryBuilders.termQuery(ModelSnapshot.SNAPSHOT_ID.getPreferredName(), job.getModelSnapshotId());
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(AnomalyDetectorsIndex.jobResultsIndexName(job.getId()));
        searchRequest.types(ModelSnapshot.TYPE.getPreferredName());
        QueryBuilder query = createQuery(job.getId(), cutoffEpochMs).mustNot(excludeFilter);
        searchRequest.source(new SearchSourceBuilder().query(query).size(MODEL_SNAPSHOT_SEARCH_SIZE));
        client.execute(SearchAction.INSTANCE, searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                try {
                    List<ModelSnapshot> modelSnapshots = new ArrayList<>();
                    for (SearchHit hit : searchResponse.getHits()) {
                        modelSnapshots.add(ModelSnapshot.fromJson(hit.getSourceRef()));
                    }
                    deleteModelSnapshots(createVolatileCursorIterator(modelSnapshots), onFinish);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.error("[" + job.getId() +  "] Search for expired snapshots failed", e);
                onFinish.run();
            }
        });
    }

    private void deleteModelSnapshots(Iterator<ModelSnapshot> modelSnapshotIterator, Runnable onFinish) {
        if (modelSnapshotIterator.hasNext() == false) {
            onFinish.run();
            return;
        }
        ModelSnapshot modelSnapshot = modelSnapshotIterator.next();
        DeleteModelSnapshotAction.Request deleteSnapshotRequest = new DeleteModelSnapshotAction.Request(
                modelSnapshot.getJobId(), modelSnapshot.getSnapshotId());
        client.execute(DeleteModelSnapshotAction.INSTANCE, deleteSnapshotRequest, new ActionListener<DeleteModelSnapshotAction.Response>() {
                @Override
                public void onResponse(DeleteModelSnapshotAction.Response response) {
                    LOGGER.trace("[{}] Deleted expired snapshot [{}]", modelSnapshot.getJobId(), modelSnapshot.getSnapshotId());
                    try {
                        deleteModelSnapshots(modelSnapshotIterator, onFinish);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error("[" + modelSnapshot.getJobId() +  "] Failed to delete snapshot ["
                            + modelSnapshot.getSnapshotId() + "]", e);
                    deleteModelSnapshots(modelSnapshotIterator, onFinish);
                }
            });
    }
}
