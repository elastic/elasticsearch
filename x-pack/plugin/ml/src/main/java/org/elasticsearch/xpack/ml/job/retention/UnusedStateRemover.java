/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.persistence.BatchedJobsIterator;
import org.elasticsearch.xpack.ml.job.persistence.BatchedStateDocIdsIterator;

import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * If for any reason a job is deleted by some of its state documents
 * are left behind, this class deletes any unused documents stored
 * in the .ml-state index.
 */
public class UnusedStateRemover implements MlDataRemover {

    private static final Logger LOGGER = LogManager.getLogger(UnusedStateRemover.class);

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
                String jobId = JobIdExtractor.extractJobId(stateDocId);
                if (jobId == null) {
                    // not a managed state document id
                    continue;
                }
                if (jobIds.contains(jobId) == false) {
                    deleteUnusedStateRequestBuilder.add(new DeleteRequest(
                            AnomalyDetectorsIndex.jobStateIndexName(), ElasticsearchMappings.DOC_TYPE, stateDocId));
                }
            }
        }
        return deleteUnusedStateRequestBuilder;
    }

    private Set<String> getJobIds() {
        Set<String> jobIds = new HashSet<>();

        // TODO Once at 8.0, we can stop searching for jobs in cluster state
        // and remove cluster service as a member all together.
        jobIds.addAll(MlMetadata.getMlMetadata(clusterService.state()).getJobs().keySet());

        BatchedJobsIterator jobsIterator = new BatchedJobsIterator(client, AnomalyDetectorsIndex.configIndexName());
        while (jobsIterator.hasNext()) {
            Deque<Job.Builder> jobs = jobsIterator.next();
            jobs.stream().map(Job.Builder::getId).forEach(jobIds::add);
        }
        return jobIds;
    }

    private void executeDeleteUnusedStateDocs(BulkRequestBuilder deleteUnusedStateRequestBuilder, ActionListener<Boolean> listener) {
        LOGGER.info("Found [{}] unused state documents; attempting to delete",
                deleteUnusedStateRequestBuilder.numberOfActions());
        deleteUnusedStateRequestBuilder.execute(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                if (bulkItemResponses.hasFailures()) {
                    LOGGER.error("Some unused state documents could not be deleted due to failures: {}",
                            bulkItemResponses.buildFailureMessage());
                } else {
                    LOGGER.info("Successfully deleted all unused state documents");
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

    private static class JobIdExtractor {

        private static List<Function<String, String>> extractors = Arrays.asList(
            ModelState::extractJobId, Quantiles::extractJobId, CategorizerState::extractJobId);

        private static String extractJobId(String docId) {
            String jobId;
            for (Function<String, String> extractor : extractors) {
                jobId = extractor.apply(docId);
                if (jobId != null) {
                    return jobId;
                }
            }
            return null;
        }
    }
}
