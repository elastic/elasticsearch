/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.downsampling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.snapshots.SnapshotInProgressException;

/**
 * Cluster service task (batched) executor that executes the replacement of data stream backing index with its
 * downsampled index.
 * After the task is executed the executor issues a delete API call for the source index however, it doesn't
 * hold up the task listener (nb we notify the listener before we call the delete API so we don't introduce
 * weird partial failure scenarios - if the delete API fails the
 * {@link org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService} will retry on the next run so the source index will get
 * deleted)
 */
public class ReplaceBackingWithDownsampleIndexExecutor extends SimpleBatchedExecutor<ReplaceSourceWithDownsampleIndexTask, Void> {
    private static final Logger LOGGER = LogManager.getLogger(ReplaceSourceWithDownsampleIndexTask.class);
    private final Client client;

    public ReplaceBackingWithDownsampleIndexExecutor(Client client) {
        this.client = client;
    }

    @Override
    public Tuple<ClusterState, Void> executeTask(ReplaceSourceWithDownsampleIndexTask task, ClusterState clusterState) throws Exception {
        return Tuple.tuple(task.execute(clusterState), null);
    }

    @Override
    public void taskSucceeded(ReplaceSourceWithDownsampleIndexTask task, Void unused) {
        LOGGER.trace(
            "Updated cluster state and replaced index [{}] with index [{}] in data stream [{}]",
            task.getSourceBackingIndex(),
            task.getDownsampleIndex(),
            task.getDataStreamName()
        );
        task.getListener().onResponse(null);

        LOGGER.trace(
            "Issuing request to delete index [{}] as it's not part of data stream [{}] anymore",
            task.getSourceBackingIndex(),
            task.getDataStreamName()
        );
        // chain an optimistic delete of the source index call here (if it fails it'll be retried by the data stream lifecycle loop)
        client.admin().indices().delete(new DeleteIndexRequest(task.getSourceBackingIndex()), new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                if (acknowledgedResponse.isAcknowledged()) {
                    LOGGER.info(
                        "Data stream lifecycle successfully deleted index [{}] due to being replaced by the downsampled index [{}] in"
                            + " data stream [{}]",
                        task.getSourceBackingIndex(),
                        task.getDownsampleIndex(),
                        task.getDataStreamName()
                    );
                } else {
                    LOGGER.trace(
                        "The delete request for index [{}] was not acknowledged. Data stream lifecycle service will retry on the"
                            + " next run if the index still exists",
                        task.getSourceBackingIndex()
                    );
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    // index was already deleted, treat this as a success
                    LOGGER.trace("Did not delete index [{}] as it was already deleted", task.getSourceBackingIndex());
                    return;
                }

                if (e instanceof SnapshotInProgressException) {
                    LOGGER.info(
                        "Data stream lifecycle is unable to delete index [{}] because it's part of an ongoing snapshot. Retrying on "
                            + "the next data stream lifecycle run",
                        task.getSourceBackingIndex()
                    );
                } else {
                    LOGGER.error(
                        () -> Strings.format(
                            "Data stream lifecycle encountered an error trying to delete index [%s]. It will retry on its next run.",
                            task.getSourceBackingIndex()
                        ),
                        e
                    );
                }
            }
        });
    }
}
