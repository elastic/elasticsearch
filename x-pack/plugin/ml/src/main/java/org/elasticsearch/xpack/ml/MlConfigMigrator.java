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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.utils.VoidChainTaskExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Migrates job and datafeed configurations from the clusterstate to
 * index documents.
 *
 * There are 3 steps to the migration process
 * 1. Read config from the clusterstate
 *     - If a job or datafeed is added after this call it will be added to the index
 *     - If deleted then it's possible the config will be copied before it is deleted.
 *       Mitigate against this by filtering out jobs marked as deleting
 * 2. Copy the config to the index
 *     - The index operation could fail, don't delete from clusterstate in this case
 * 3. Remove config from the clusterstate
 *     - Before this happens config is duplicated in index and clusterstate, all ops
 *       must prefer to use the index config at this stage
 *     - If the clusterstate update fails then the config will remain duplicated
 *       and the migration process should try again
 *
 * If there was an error in step 3 and the config is in both the clusterstate and
 * index then when the migrator retries it must not overwrite an existing job config
 * document as once the index document is present all update operations will function
 * on that rather than the clusterstate.
 *
 * The number of configs indexed in each bulk operation is limited by {@link #MAX_BULK_WRITE_SIZE}
 * pairs of datafeeds and jobs are migrated together.
 */
public class MlConfigMigrator {

    private static final Logger logger = LogManager.getLogger(MlConfigMigrator.class);

    public static final String MIGRATED_FROM_VERSION = "migrated from version";

    static final int MAX_BULK_WRITE_SIZE = 100;

    private final Client client;
    private final ClusterService clusterService;
    private final MlConfigMigrationEligibilityCheck migrationEligibilityCheck;

    private final AtomicBoolean migrationInProgress;
    private final AtomicBoolean tookConfigSnapshot;

    public MlConfigMigrator(Settings settings, Client client, ClusterService clusterService) {
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        this.migrationInProgress = new AtomicBoolean(false);
        this.tookConfigSnapshot = new AtomicBoolean(false);
    }

    /**
     * Migrate ml job and datafeed configurations from the clusterstate
     * to index documents.
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
        if (migrationInProgress.compareAndSet(false, true) == false) {
            listener.onResponse(Boolean.FALSE);
            return;
        }

        ActionListener<Boolean> unMarkMigrationInProgress = ActionListener.wrap(
                response -> {
                    migrationInProgress.set(false);
                    listener.onResponse(response);
                },
                e -> {
                    migrationInProgress.set(false);
                    listener.onFailure(e);
                }
        );

        List<JobsAndDatafeeds> batches = splitInBatches(clusterState);
        if (batches.isEmpty()) {
            unMarkMigrationInProgress.onResponse(Boolean.FALSE);
            return;
        }

        if (clusterState.metaData().hasIndex(AnomalyDetectorsIndex.configIndexName()) == false) {
            createConfigIndex(ActionListener.wrap(
                    response -> {
                        unMarkMigrationInProgress.onResponse(Boolean.FALSE);
                    },
                    unMarkMigrationInProgress::onFailure
            ));
            return;
        }

        if (migrationEligibilityCheck.canStartMigration(clusterState) == false) {
            unMarkMigrationInProgress.onResponse(Boolean.FALSE);
            return;
        }

        snapshotMlMeta(MlMetadata.getMlMetadata(clusterState), ActionListener.wrap(
                response -> {
                    // We have successfully snapshotted the ML configs so we don't need to try again
                    tookConfigSnapshot.set(true);
                    migrateBatches(batches, unMarkMigrationInProgress);
                },
                unMarkMigrationInProgress::onFailure
        ));
    }

    private void migrateBatches(List<JobsAndDatafeeds> batches, ActionListener<Boolean> listener) {
        VoidChainTaskExecutor voidChainTaskExecutor = new VoidChainTaskExecutor(EsExecutors.newDirectExecutorService(), true);
        for (JobsAndDatafeeds batch : batches) {
            voidChainTaskExecutor.add(chainedListener -> writeConfigToIndex(batch.datafeedConfigs, batch.jobs, ActionListener.wrap(
                failedDocumentIds -> {
                    List<String> successfulJobWrites = filterFailedJobConfigWrites(failedDocumentIds, batch.jobs);
                    List<String> successfulDatafeedWrites =
                        filterFailedDatafeedConfigWrites(failedDocumentIds, batch.datafeedConfigs);
                    removeFromClusterState(successfulJobWrites, successfulDatafeedWrites, chainedListener);
                },
                chainedListener::onFailure
            )));
        }
        voidChainTaskExecutor.execute(ActionListener.wrap(aVoids -> listener.onResponse(true), listener::onFailure));
    }

    // Exposed for testing
    public void writeConfigToIndex(Collection<DatafeedConfig> datafeedsToMigrate,
                                   Collection<Job> jobsToMigrate,
                                   ActionListener<Set<String>> listener) {

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        addJobIndexRequests(jobsToMigrate, bulkRequestBuilder);
        addDatafeedIndexRequests(datafeedsToMigrate, bulkRequestBuilder);
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, bulkRequestBuilder.request(),
                ActionListener.<BulkResponse>wrap(
                        bulkResponse -> {
                            Set<String> failedDocumentIds = documentsNotWritten(bulkResponse);
                            listener.onResponse(failedDocumentIds);
                        },
                        listener::onFailure),
                client::bulk
        );
    }

    private void removeFromClusterState(List<String> jobsToRemoveIds, List<String> datafeedsToRemoveIds,
                                        ActionListener<Void> listener) {
        if (jobsToRemoveIds.isEmpty() && datafeedsToRemoveIds.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        AtomicReference<RemovalResult> removedConfigs = new AtomicReference<>();

        clusterService.submitStateUpdateTask("remove-migrated-ml-configs", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                RemovalResult removed = removeJobsAndDatafeeds(jobsToRemoveIds, datafeedsToRemoveIds,
                        MlMetadata.getMlMetadata(currentState));
                removedConfigs.set(removed);
                ClusterState.Builder newState = ClusterState.builder(currentState);
                newState.metaData(MetaData.builder(currentState.getMetaData())
                        .putCustom(MlMetadata.TYPE, removed.mlMetadata)
                        .build());
                return newState.build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (removedConfigs.get() != null) {
                    if (removedConfigs.get().removedJobIds.isEmpty() == false) {
                        logger.info("ml job configurations migrated: {}", removedConfigs.get().removedJobIds);
                    }
                    if (removedConfigs.get().removedDatafeedIds.isEmpty() == false) {
                        logger.info("ml datafeed configurations migrated: {}", removedConfigs.get().removedDatafeedIds);
                    }
                }
                listener.onResponse(null);
            }
        });
    }

    static class RemovalResult {
        MlMetadata mlMetadata;
        List<String> removedJobIds;
        List<String> removedDatafeedIds;

        RemovalResult(MlMetadata mlMetadata, List<String> removedJobIds, List<String> removedDatafeedIds) {
            this.mlMetadata = mlMetadata;
            this.removedJobIds = removedJobIds;
            this.removedDatafeedIds = removedDatafeedIds;
        }
    }

    /**
     * Remove the datafeeds and jobs listed in the parameters from
     * mlMetadata if they exist. An account of removed jobs and datafeeds
     * is returned in the result structure alongside a new MlMetadata
     * with the config removed.
     *
     * @param jobsToRemove       Jobs
     * @param datafeedsToRemove  Datafeeds
     * @param mlMetadata         MlMetadata
     * @return Structure tracking which jobs and datafeeds were actually removed
     * and the new MlMetadata
     */
    static RemovalResult removeJobsAndDatafeeds(List<String> jobsToRemove, List<String> datafeedsToRemove, MlMetadata mlMetadata) {
        Map<String, Job> currentJobs = new HashMap<>(mlMetadata.getJobs());
        List<String> removedJobIds = new ArrayList<>();
        for (String jobId : jobsToRemove) {
            if (currentJobs.remove(jobId) != null) {
                removedJobIds.add(jobId);
            }
        }

        Map<String, DatafeedConfig> currentDatafeeds = new HashMap<>(mlMetadata.getDatafeeds());
        List<String> removedDatafeedIds = new ArrayList<>();
        for (String datafeedId : datafeedsToRemove) {
            if (currentDatafeeds.remove(datafeedId) != null) {
                removedDatafeedIds.add(datafeedId);
            }
        }

        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJobs(currentJobs.values())
                .putDatafeeds(currentDatafeeds.values());

        return new RemovalResult(builder.build(), removedJobIds, removedDatafeedIds);
    }

    private void addJobIndexRequests(Collection<Job> jobs, BulkRequestBuilder bulkRequestBuilder) {
        ToXContent.Params params = new ToXContent.MapParams(JobConfigProvider.TO_XCONTENT_PARAMS);
        for (Job job : jobs) {
            logger.debug("adding job to migrate: " + job.getId());
            bulkRequestBuilder.add(indexRequest(job, Job.documentId(job.getId()), params));
        }
    }

    private void addDatafeedIndexRequests(Collection<DatafeedConfig> datafeedConfigs, BulkRequestBuilder bulkRequestBuilder) {
        ToXContent.Params params = new ToXContent.MapParams(DatafeedConfigProvider.TO_XCONTENT_PARAMS);
        for (DatafeedConfig datafeedConfig : datafeedConfigs) {
            logger.debug("adding datafeed to migrate: " + datafeedConfig.getId());
            bulkRequestBuilder.add(indexRequest(datafeedConfig, DatafeedConfig.documentId(datafeedConfig.getId()), params));
        }
    }

    private IndexRequest indexRequest(ToXContentObject source, String documentId, ToXContent.Params params) {
        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.configIndexName(), ElasticsearchMappings.DOC_TYPE, documentId);

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            indexRequest.source(source.toXContent(builder, params));
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialise object [" + documentId + "]", e);
        }
        return indexRequest;
    }

    // public for testing
    public void snapshotMlMeta(MlMetadata mlMetadata, ActionListener<Boolean> listener) {

        if (tookConfigSnapshot.get()) {
            listener.onResponse(true);
            return;
        }

        if (mlMetadata.getJobs().isEmpty() && mlMetadata.getDatafeeds().isEmpty()) {
            listener.onResponse(true);
            return;
        }

        logger.debug("taking a snapshot of ml_metadata");
        String documentId = "ml-config";
        IndexRequestBuilder indexRequest = client.prepareIndex(AnomalyDetectorsIndex.jobStateIndexName(),
                ElasticsearchMappings.DOC_TYPE, documentId)
                .setOpType(DocWriteRequest.OpType.CREATE);

        ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            mlMetadata.toXContent(builder, params);
            builder.endObject();

            indexRequest.setSource(builder);
        } catch (IOException e) {
            logger.error("failed to serialise ml_metadata", e);
            listener.onFailure(e);
            return;
        }

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, indexRequest.request(),
                ActionListener.<IndexResponse>wrap(
                        indexResponse -> {
                            listener.onResponse(indexResponse.getResult() == DocWriteResponse.Result.CREATED);
                        },
                        listener::onFailure),
                client::index
        );
    }

    private void createConfigIndex(ActionListener<Boolean> listener) {
        logger.info("creating the .ml-config index");
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(AnomalyDetectorsIndex.configIndexName());
        try
        {
            createIndexRequest.settings(
                    Settings.builder()
                            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                            .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), AnomalyDetectorsIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW)
            );
            createIndexRequest.mapping(ElasticsearchMappings.DOC_TYPE, ElasticsearchMappings.configMapping());
        } catch (Exception e) {
            logger.error("error writing the .ml-config mappings", e);
            listener.onFailure(e);
            return;
        }

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, createIndexRequest,
                ActionListener.<CreateIndexResponse>wrap(
                        r -> listener.onResponse(r.isAcknowledged()),
                        listener::onFailure
                ), client.admin().indices()::create);
    }

    public static Job updateJobForMigration(Job job) {
        Job.Builder builder = new Job.Builder(job);
        Map<String, Object> custom = job.getCustomSettings() == null ? new HashMap<>() : new HashMap<>(job.getCustomSettings());
        custom.put(MIGRATED_FROM_VERSION, job.getJobVersion());
        builder.setCustomSettings(custom);
        // Increase the model memory limit for 6.1 - 6.3 jobs
        Version jobVersion = job.getJobVersion();
        if (jobVersion != null && jobVersion.onOrAfter(Version.V_6_1_0) && jobVersion.before(Version.V_6_3_0)) {
            // Increase model memory limit if < 512MB
            if (job.getAnalysisLimits() != null && job.getAnalysisLimits().getModelMemoryLimit() != null &&
                    job.getAnalysisLimits().getModelMemoryLimit() < 512L) {
                long updatedModelMemoryLimit = (long) (job.getAnalysisLimits().getModelMemoryLimit() * 1.3);
                AnalysisLimits limits = new AnalysisLimits(updatedModelMemoryLimit,
                        job.getAnalysisLimits().getCategorizationExamplesLimit());
                builder.setAnalysisLimits(limits);
            }
        }
        // Pre v5.5 (ml beta) jobs do not have a version.
        // These jobs cannot be opened, we rely on the missing version
        // to indicate this.
        // See TransportOpenJobAction.validate()
        if (jobVersion != null) {
            builder.setJobVersion(Version.CURRENT);
        }
        return builder.build();
    }

    /**
     * Filter jobs marked as deleting from the list of jobs
     * are not marked as deleting.
     *
     * @param jobs The jobs to filter
     * @return Jobs not marked as deleting
     */
    public static List<Job> nonDeletingJobs(List<Job> jobs) {
        return jobs.stream()
                .filter(job -> job.isDeleting() == false)
                .collect(Collectors.toList());
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

    public static class JobsAndDatafeeds  {
        List<Job> jobs;
        List<DatafeedConfig> datafeedConfigs;

        private JobsAndDatafeeds() {
            jobs = new ArrayList<>();
            datafeedConfigs = new ArrayList<>();
        }

        public int totalCount() {
            return jobs.size() + datafeedConfigs.size();
        }
    }

    public static List<JobsAndDatafeeds> splitInBatches(ClusterState clusterState) {
        Collection<DatafeedConfig> stoppedDatafeeds = stoppedDatafeedConfigs(clusterState);
        Map<String, Job> eligibleJobs = nonDeletingJobs(closedJobConfigs(clusterState)).stream()
            .map(MlConfigMigrator::updateJobForMigration)
            .collect(Collectors.toMap(Job::getId, Function.identity(), (a, b) -> a));

        List<JobsAndDatafeeds> batches = new ArrayList<>();
        while (stoppedDatafeeds.isEmpty() == false || eligibleJobs.isEmpty() == false) {
            JobsAndDatafeeds batch = limitWrites(stoppedDatafeeds, eligibleJobs);
            batches.add(batch);
            stoppedDatafeeds.removeAll(batch.datafeedConfigs);
            batch.jobs.forEach(job -> eligibleJobs.remove(job.getId()));
        }
        return batches;
    }

    /**
     * Return at most {@link #MAX_BULK_WRITE_SIZE} configs favouring
     * datafeed and job pairs so if a datafeed is chosen so is its job.
     *
     * @param datafeedsToMigrate Datafeed configs
     * @param jobsToMigrate      Job configs
     * @return Job and datafeed configs
     */
    public static JobsAndDatafeeds limitWrites(Collection<DatafeedConfig> datafeedsToMigrate, Map<String, Job> jobsToMigrate) {
        JobsAndDatafeeds jobsAndDatafeeds = new JobsAndDatafeeds();

        if (datafeedsToMigrate.size() + jobsToMigrate.size() <= MAX_BULK_WRITE_SIZE) {
            jobsAndDatafeeds.jobs.addAll(jobsToMigrate.values());
            jobsAndDatafeeds.datafeedConfigs.addAll(datafeedsToMigrate);
            return jobsAndDatafeeds;
        }

        int count = 0;

        // prioritise datafeed and job pairs
        for (DatafeedConfig datafeedConfig : datafeedsToMigrate) {
            if (count < MAX_BULK_WRITE_SIZE) {
                jobsAndDatafeeds.datafeedConfigs.add(datafeedConfig);
                count++;
                Job datafeedsJob = jobsToMigrate.remove(datafeedConfig.getJobId());
                if (datafeedsJob != null) {
                    jobsAndDatafeeds.jobs.add(datafeedsJob);
                    count++;
                }
            }
        }

        // are there jobs without datafeeds to migrate
        Iterator<Job> iter = jobsToMigrate.values().iterator();
        while (iter.hasNext() && count < MAX_BULK_WRITE_SIZE) {
            jobsAndDatafeeds.jobs.add(iter.next());
            count++;
        }

        return jobsAndDatafeeds;
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
                failedDocumentIds.add(itemResponse.getFailure().getId());
                logger.info("failed to index ml configuration [" + itemResponse.getFailure().getId() + "], " +
                        itemResponse.getFailure().getMessage());
            } else {
                logger.info("ml configuration [" + itemResponse.getId() + "] indexed");
            }
        }
        return failedDocumentIds;
    }

    static List<String> filterFailedJobConfigWrites(Set<String> failedDocumentIds, List<Job> jobs) {
        return jobs.stream()
                .map(Job::getId)
                .filter(id -> failedDocumentIds.contains(Job.documentId(id)) == false)
                .collect(Collectors.toList());
    }

    static List<String> filterFailedDatafeedConfigWrites(Set<String> failedDocumentIds, Collection<DatafeedConfig> datafeeds) {
        return datafeeds.stream()
                .map(DatafeedConfig::getId)
                .filter(id -> failedDocumentIds.contains(DatafeedConfig.documentId(id)) == false)
                .collect(Collectors.toList());
    }
}
