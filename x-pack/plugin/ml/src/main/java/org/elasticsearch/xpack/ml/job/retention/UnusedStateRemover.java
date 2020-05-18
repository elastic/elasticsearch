/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.dataframe.StoredProgress;
import org.elasticsearch.xpack.ml.job.persistence.BatchedJobsIterator;
import org.elasticsearch.xpack.ml.job.persistence.BatchedStateDocIdsIterator;
import org.elasticsearch.xpack.ml.utils.persistence.DocIdBatchedDocumentIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * If for any reason a job is deleted but some of its state documents
 * are left behind, this class deletes any unused documents stored
 * in the .ml-state* indices.
 */
public class UnusedStateRemover implements MlDataRemover {

    private static final Logger LOGGER = LogManager.getLogger(UnusedStateRemover.class);

    private final OriginSettingClient client;
    private final ClusterService clusterService;

    public UnusedStateRemover(OriginSettingClient client, ClusterService clusterService) {
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    @Override
    public void remove(float requestsPerSec, ActionListener<Boolean> listener, Supplier<Boolean> isTimedOutSupplier) {
        try {
            List<String> unusedStateDocIds = findUnusedStateDocIds();
            if (isTimedOutSupplier.get()) {
                listener.onResponse(false);
            } else {
                if (unusedStateDocIds.size() > 0) {
                    executeDeleteUnusedStateDocs(unusedStateDocIds, requestsPerSec, listener);
                } else {
                    listener.onResponse(true);
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private List<String> findUnusedStateDocIds() {
        Set<String> jobIds = getJobIds();
        List<String> stateDocIdsToDelete = new ArrayList<>();
        BatchedStateDocIdsIterator stateDocIdsIterator = new BatchedStateDocIdsIterator(client,
            AnomalyDetectorsIndex.jobStateIndexPattern());
        while (stateDocIdsIterator.hasNext()) {
            Deque<String> stateDocIds = stateDocIdsIterator.next();
            for (String stateDocId : stateDocIds) {
                String jobId = JobIdExtractor.extractJobId(stateDocId);
                if (jobId == null) {
                    // not a managed state document id
                    continue;
                }
                if (jobIds.contains(jobId) == false) {
                    stateDocIdsToDelete.add(stateDocId);
                }
            }
        }
        return stateDocIdsToDelete;
    }

    private Set<String> getJobIds() {
        Set<String> jobIds = new HashSet<>();
        jobIds.addAll(getAnomalyDetectionJobIds());
        jobIds.addAll(getDataFrameAnalyticsJobIds());
        return jobIds;
    }

    private Set<String> getAnomalyDetectionJobIds() {
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

    private Set<String> getDataFrameAnalyticsJobIds() {
        Set<String> jobIds = new HashSet<>();

        DocIdBatchedDocumentIterator iterator = new DocIdBatchedDocumentIterator(client, AnomalyDetectorsIndex.configIndexName(),
            QueryBuilders.termQuery(DataFrameAnalyticsConfig.CONFIG_TYPE.getPreferredName(), DataFrameAnalyticsConfig.TYPE));
        while (iterator.hasNext()) {
            Deque<String> docIds = iterator.next();
            docIds.stream().map(DataFrameAnalyticsConfig::extractJobIdFromDocId).filter(Objects::nonNull).forEach(jobIds::add);
        }
        return jobIds;
    }

    private void executeDeleteUnusedStateDocs(List<String> unusedDocIds, float requestsPerSec, ActionListener<Boolean> listener) {
        LOGGER.info("Found [{}] unused state documents; attempting to delete",
                unusedDocIds.size());
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setAbortOnVersionConflict(false)
            .setRequestsPerSecond(requestsPerSec)
            .setQuery(QueryBuilders.idsQuery().addIds(unusedDocIds.toArray(new String[0])));

        // _doc is the most efficient sort order and will also disable scoring
        deleteByQueryRequest.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        client.execute(DeleteByQueryAction.INSTANCE, deleteByQueryRequest, ActionListener.wrap(
            response -> {
                if (response.getBulkFailures().size() > 0 || response.getSearchFailures().size() > 0) {
                    LOGGER.error("Some unused state documents could not be deleted due to failures: {}",
                        Strings.collectionToCommaDelimitedString(response.getBulkFailures()) +
                            "," + Strings.collectionToCommaDelimitedString(response.getSearchFailures()));
                } else {
                    LOGGER.info("Successfully deleted all unused state documents");
                }
                listener.onResponse(true);
            },
            e -> {
                LOGGER.error("Error deleting unused model state documents: ", e);
                listener.onFailure(e);
            }
        ));
    }

    private static class JobIdExtractor {

        private static List<Function<String, String>> extractors = Arrays.asList(
            ModelState::extractJobId,
            Quantiles::extractJobId,
            CategorizerState::extractJobId,
            Classification::extractJobIdFromStateDoc,
            Regression::extractJobIdFromStateDoc,
            StoredProgress::extractJobIdFromDocId
        );

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
