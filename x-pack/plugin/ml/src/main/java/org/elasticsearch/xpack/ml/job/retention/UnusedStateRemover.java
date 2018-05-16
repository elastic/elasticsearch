/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.core.ml.MLMetadataField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.ml.job.persistence.BatchedStateDocIdsIterator;

import java.util.Collections;
import java.util.Deque;
import java.util.Objects;
import java.util.Set;

public class UnusedStateRemover implements MlDataRemover {

    private static final Logger LOGGER = Loggers.getLogger(UnusedStateRemover.class);

    private final Client client;
    private final ClusterService clusterService;

    public UnusedStateRemover(Client client, ClusterService clusterService) {
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    @Override
    public void remove(ActionListener<Boolean> listener) {
        try {
            BulkRequestBuilder deleteUnusedStateRequestBuilder = findUnusedStateDocs();
            if (deleteUnusedStateRequestBuilder.numberOfActions() > 0) {
                executeDeleteUnusedStateDocs(deleteUnusedStateRequestBuilder, listener);
            } else {
                listener.onResponse(true);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private BulkRequestBuilder findUnusedStateDocs() {
        Set<String> jobIds = getJobIds();
        BulkRequestBuilder deleteUnusedStateRequestBuilder = client.prepareBulk();
        BatchedStateDocIdsIterator stateDocIdsIterator = new BatchedStateDocIdsIterator(client, AnomalyDetectorsIndex.jobStateIndexName());
        while (stateDocIdsIterator.hasNext()) {
            Deque<String> stateDocIds = stateDocIdsIterator.next();
            for (String stateDocId : stateDocIds) {
                int modelStateSuffixIndex = stateDocId.indexOf("_model_state_");
                if (modelStateSuffixIndex < 0) {
                    // e.g. quantiles, etc.
                    continue;
                }
                String jobId = stateDocId.substring(0, modelStateSuffixIndex);
                if (jobIds.contains(jobId) == false) {
                    deleteUnusedStateRequestBuilder.add(new DeleteRequest(
                            AnomalyDetectorsIndex.jobStateIndexName(), ElasticsearchMappings.DOC_TYPE, stateDocId));
                }
            }
        }
        return deleteUnusedStateRequestBuilder;
    }

    private Set<String> getJobIds() {
        ClusterState clusterState = clusterService.state();
        MlMetadata mlMetadata = clusterState.getMetaData().custom(MLMetadataField.TYPE);
        if (mlMetadata != null) {
            return mlMetadata.getJobs().keySet();
        }
        return Collections.emptySet();
    }

    private void executeDeleteUnusedStateDocs(BulkRequestBuilder deleteUnusedStateRequestBuilder, ActionListener<Boolean> listener) {
        LOGGER.info("Found {} unused model state documents; attempting to delete",
                deleteUnusedStateRequestBuilder.numberOfActions());
        deleteUnusedStateRequestBuilder.execute(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                if (bulkItemResponses.hasFailures()) {
                    LOGGER.error("Some unused model state documents could not be deleted due to failures: {}",
                            bulkItemResponses.buildFailureMessage());
                } else {
                    LOGGER.info("Successfully deleted unused model state documents");
                }
                listener.onResponse(true);
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.error("Error deleting unused model state documents: ", e);
                listener.onFailure(e);
            }
        });
    }
}
