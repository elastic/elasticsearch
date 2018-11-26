/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

/**
 * Migrates job and datafeed configurations from the clusterstate to
 * index documents.
 */
public class MlConfigMigrator {

    private static final Logger logger = LogManager.getLogger(MlConfigMigrator.class);

    public static final String MIGRATED_FROM_VERSION = "migrated from version";

    private final Client client;
    private final ClusterService clusterService;

    public MlConfigMigrator(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    /**
     * Migrate ml job and datafeed configurations from the clusterstate
     * to index documents. Only the configs that do not have an associated
     * persistent task are migrated.
     *
     * Configs to be migrated are read from the cluster state then bulk
     * indexed into .ml-config. Those successfully indexed are then removed
     * from the clusterstate.
     *
     * Migrated jobs have the job version set to v6.6.0 and the custom settings
     * map has an entry added recording the fact the job was migrated and its
     * original version e.g.
     *     "migrated from version" : v6.1.0
     *
     *
     * @param clusterState The current clusterstate
     * @param listener     The success listener
     */
    public void migrateConfigsWithoutTasks(ClusterState clusterState, ActionListener<Boolean> listener) {

        List<DatafeedConfig> datafeedsToMigrate = stoppedDatafeedConfigs(clusterState);
        List<Job> jobsToMigrate = closedJobConfigs(clusterState).stream()
                .map(MlConfigMigrator::updateJobForMigration)
                .collect(Collectors.toList());

        if (datafeedsToMigrate.isEmpty() && jobsToMigrate.isEmpty()) {
            listener.onResponse(Boolean.FALSE);
            return;
        }

        writeConfigToIndex(datafeedsToMigrate, jobsToMigrate, ActionListener.wrap(
                failedDocumentIds -> {
                    List<Job> successfulJobWrites = filterFailedJobConfigWrites(failedDocumentIds, jobsToMigrate);
                    List<DatafeedConfig> successfullDatafeedWrites =
                            filterFailedDatafeedConfigWrites(failedDocumentIds, datafeedsToMigrate);
                    removeFromClusterState(successfulJobWrites, successfullDatafeedWrites, listener);
                },
                listener::onFailure
        ));
    }

    // Exposed for testing
    public void writeConfigToIndex(List<DatafeedConfig> datafeedsToMigrate,
                                   List<Job> jobsToMigrate,
                                   ActionListener<Set<String>> listener) {

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        addJobIndexRequests(jobsToMigrate, bulkRequestBuilder);
        addDatafeedIndexRequests(datafeedsToMigrate, bulkRequestBuilder);
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        bulkRequestBuilder.execute(ActionListener.wrap(
                bulkResponse -> {
                    Set<String> failedDocumentIds = documentsNotWritten(bulkResponse);
                    listener.onResponse(failedDocumentIds);
                },
                listener::onFailure
        ));
    }

    private void removeFromClusterState(List<Job> jobs, List<DatafeedConfig> datafeedConfigs, ActionListener<Boolean> listener) {
        if (jobs.isEmpty() && datafeedConfigs.isEmpty()) {
            listener.onResponse(Boolean.FALSE);
            return;
        }

        clusterService.submitStateUpdateTask("remove-migrated-ml-configs", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                MlMetadata currentMlMetadata = MlMetadata.getMlMetadata(currentState);
                Map<String, Job> currentJobs = currentMlMetadata.getJobs();
                for (Job job : jobs) {
                    currentJobs.remove(job.getId());
                }

                SortedMap<String, DatafeedConfig> currentDatafeeds = currentMlMetadata.getDatafeeds();
                for (DatafeedConfig datafeed : datafeedConfigs) {
                    currentDatafeeds.remove(datafeed.getId());
                }

                MlMetadata.Builder builder = new MlMetadata.Builder();
                builder.setLastMemoryRefreshVersion(currentMlMetadata.getLastMemoryRefreshVersion())
                        .putJobs(currentJobs.values())
                        .putDatafeeds(currentDatafeeds.values());

                ClusterState.Builder newState = ClusterState.builder(currentState);
                newState.metaData(MetaData.builder(currentState.getMetaData())
                        .putCustom(MlMetadata.TYPE, builder.build())
                        .build());
                return newState.build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("ml job configurations migrated: " + jobs);
                logger.info("ml datafeed configurations migrated: " + datafeedConfigs);
                listener.onResponse(Boolean.TRUE);
            }
        });
    }

    private void addJobIndexRequests(List<Job> jobs, BulkRequestBuilder bulkRequestBuilder) {
        ToXContent.Params params = new ToXContent.MapParams(JobConfigProvider.TO_XCONTENT_PARAMS);
        for (Job job : jobs) {
            bulkRequestBuilder.add(indexRequest(job, Job.documentId(job.getId()), params));
        }
    }

    private void addDatafeedIndexRequests(List<DatafeedConfig> datafeedConfigs, BulkRequestBuilder bulkRequestBuilder) {
        ToXContent.Params params = new ToXContent.MapParams(DatafeedConfigProvider.TO_XCONTENT_PARAMS);
        for (DatafeedConfig datafeedConfig : datafeedConfigs) {
            bulkRequestBuilder.add(indexRequest(datafeedConfig, DatafeedConfig.documentId(datafeedConfig.getId()), params));
        }
    }

    private IndexRequest indexRequest(ToXContentObject source, String documentId, ToXContent.Params params) {
        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.configIndexName(), ElasticsearchMappings.DOC_TYPE, documentId);
        // It is an error if there is an existing document
        indexRequest.opType(DocWriteRequest.OpType.CREATE);

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            indexRequest.source(source.toXContent(builder, params));
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialise object [" + documentId + "]", e);
        }
        return indexRequest;
    }

    public static Job updateJobForMigration(Job job) {
        Job.Builder builder = new Job.Builder(job);
        Map<String, Object> custom = job.getCustomSettings() == null ? new HashMap<>() : new HashMap<>(job.getCustomSettings());
        custom.put(MIGRATED_FROM_VERSION, job.getJobVersion());
        builder.setCustomSettings(custom);
        // Pre v5.5 (ml beta) jobs do not have a version.
        // These jobs cannot be opened, we rely on the missing version
        // to indicate this.
        // See TransportOpenJobAction.validate()
        if (job.getJobVersion() != null) {
            builder.setJobVersion(Version.CURRENT);
        }
        return builder.build();
    }

    /**
     * Find the configurations for all closed jobs in the cluster state.
     * Closed jobs are those that do not have an associated persistent task.
     *
     * @param clusterState The cluster state
     * @return The closed job configurations
     */
    public static List<Job> closedJobConfigs(ClusterState clusterState) {
        PersistentTasksCustomMetaData persistentTasks = clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        Set<String> openJobIds = MlTasks.openJobIds(persistentTasks);

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        return mlMetadata.getJobs().values().stream()
                .filter(job -> openJobIds.contains(job.getId()) == false)
                .collect(Collectors.toList());
    }

    /**
     * Find the configurations for stopped datafeeds in the cluster state.
     * Stopped datafeeds are those that do not have an associated persistent task.
     *
     * @param clusterState The cluster state
     * @return The closed job configurations
     */
    public static List<DatafeedConfig> stoppedDatafeedConfigs(ClusterState clusterState) {
        PersistentTasksCustomMetaData persistentTasks = clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        Set<String> startedDatafeedIds = MlTasks.startedDatafeedIds(persistentTasks);

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        return mlMetadata.getDatafeeds().values().stream()
                .filter(datafeedConfig-> startedDatafeedIds.contains(datafeedConfig.getId()) == false)
                .collect(Collectors.toList());
    }

    /**
     * Check for failures in the bulk response and return the
     * Ids of any documents not written to the index
     *
     * If the index operation failed because the document already
     * exists this is not considered an error.
     *
     * @param response BulkResponse
     * @return The set of document Ids not written by the bulk request
     */
    static Set<String> documentsNotWritten(BulkResponse response) {
        Set<String> failedDocumentIds = new HashSet<>();

        for (BulkItemResponse itemResponse : response.getItems()) {
            if (itemResponse.isFailed()) {
                BulkItemResponse.Failure failure = itemResponse.getFailure();
                if (failure.getCause().getClass() == VersionConflictEngineException.class) {
                    // not a failure. The document is already written but perhaps
                    // has not been removed from the clusterstate
                    logger.debug("cannot write ml configuration [" + itemResponse.getFailure().getId() + "] as it already exists");
                } else {
                    failedDocumentIds.add(itemResponse.getFailure().getId());
                    logger.debug("failed to index ml configuration [" + itemResponse.getFailure().getId() + "], " +
                            itemResponse.getFailure().getMessage());
                }
            }
        }
        return failedDocumentIds;
    }

    static List<Job> filterFailedJobConfigWrites(Set<String> failedDocumentIds, List<Job> jobs) {
        return jobs.stream()
                .filter(job -> failedDocumentIds.contains(Job.documentId(job.getId())) == false)
                .collect(Collectors.toList());
    }

    static List<DatafeedConfig> filterFailedDatafeedConfigWrites(Set<String> failedDocumentIds, List<DatafeedConfig> datafeeds) {
        return datafeeds.stream()
                .filter(datafeed -> failedDocumentIds.contains(DatafeedConfig.documentId(datafeed.getId())) == false)
                .collect(Collectors.toList());
    }
}
