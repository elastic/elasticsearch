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
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
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

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Migrates job and datafeed configurations from the clusterstate to
 * index documents for closed or unallocated tasks.
 *
 * There are 3 steps to the migration process
 * 1. Read config from the clusterstate
 *     - Find all job and datafeed configs that do not have an associated persistent
 *       task or the persistent task is unallocated
 *     - If a job or datafeed is added after this call it will be added to the index
 *     - If deleted then it's possible the config will be copied before it is deleted.
 *       Mitigate against this by filtering out jobs marked as deleting
 * 2. Copy the config to the index
 *     - The index operation could fail, don't delete from clusterstate in this case
 * 3. Remove config from the clusterstate and update persistent task parameters
 *     - Before this happens config is duplicated in index and clusterstate, all ops
 *       must prefer to use the clusterstate config at this stage
 *     - If the clusterstate update fails then the config will remain duplicated
 *       and the migration process should try again
 *     - Job and datafeed tasks opened prior to v6.6.0 need to be updated with new
 *       parameters
 *
 * If there was an error in step 3 and the config is in both the clusterstate and
 * index. At this point the clusterstate config is preferred and all update
 * operations will function on that rather than the index.
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
    public void migrateConfigs(ClusterState clusterState, ActionListener<Boolean> listener) {
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
                    List<Job> successfulJobWrites = filterFailedJobConfigWrites(failedDocumentIds, batch.jobs);
                    List<DatafeedConfig> successfulDatafeedWrites =
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

    private void removeFromClusterState(List<Job> jobsToRemove, List<DatafeedConfig> datafeedsToRemove,
                                        ActionListener<Void> listener) {
        if (jobsToRemove.isEmpty() && datafeedsToRemove.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        Map<String, Job> jobsMap = jobsToRemove.stream().collect(Collectors.toMap(Job::getId, Function.identity()));
        Map<String, DatafeedConfig> datafeedMap =
                datafeedsToRemove.stream().collect(Collectors.toMap(DatafeedConfig::getId, Function.identity()));

        AtomicReference<RemovalResult> removedConfigs = new AtomicReference<>();

        clusterService.submitStateUpdateTask("remove-migrated-ml-configs", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                RemovalResult removed = removeJobsAndDatafeeds(jobsToRemove, datafeedsToRemove,
                        MlMetadata.getMlMetadata(currentState));
                removedConfigs.set(removed);

                PersistentTasksCustomMetaData updatedTasks = rewritePersistentTaskParams(jobsMap, datafeedMap,
                        currentState.metaData().custom(PersistentTasksCustomMetaData.TYPE), currentState.nodes());

                ClusterState.Builder newState = ClusterState.builder(currentState);
                MetaData.Builder metaDataBuilder = MetaData.builder(currentState.getMetaData())
                    .putCustom(MlMetadata.TYPE, removed.mlMetadata);

                // If there are no tasks in the cluster state metadata to begin with, this could be null.
                if (updatedTasks != null) {
                    metaDataBuilder = metaDataBuilder.putCustom(PersistentTasksCustomMetaData.TYPE, updatedTasks);
                }
                newState.metaData(metaDataBuilder.build());
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

    /**
     * Find any unallocated datafeed and job tasks and update their persistent
     * task parameters if they have missing fields that were added in v6.6. If
     * a task exists with a missing field it must have been created in an earlier
     * version and survived an elasticsearch upgrade.
     *
     * If there are no unallocated tasks the {@code currentTasks} argument is returned.
     *
     * @param jobs          Job configs
     * @param datafeeds     Datafeed configs
     * @param currentTasks  The persistent tasks
     * @param nodes         The nodes in the cluster
     * @return  The updated tasks
     */
    public static PersistentTasksCustomMetaData rewritePersistentTaskParams(Map<String, Job> jobs, Map<String, DatafeedConfig> datafeeds,
                                                                            PersistentTasksCustomMetaData currentTasks,
                                                                            DiscoveryNodes nodes) {

        Collection<PersistentTasksCustomMetaData.PersistentTask<?>> unallocatedJobTasks = MlTasks.unallocatedJobTasks(currentTasks, nodes);
        Collection<PersistentTasksCustomMetaData.PersistentTask<?>> unallocatedDatafeedsTasks =
                MlTasks.unallocatedDatafeedTasks(currentTasks, nodes);

        if (unallocatedJobTasks.isEmpty() && unallocatedDatafeedsTasks.isEmpty()) {
            return currentTasks;
        }

        PersistentTasksCustomMetaData.Builder taskBuilder = PersistentTasksCustomMetaData.builder(currentTasks);

        for (PersistentTasksCustomMetaData.PersistentTask<?> jobTask : unallocatedJobTasks) {
            OpenJobAction.JobParams originalParams = (OpenJobAction.JobParams) jobTask.getParams();
            if (originalParams.getJob() == null) {
                Job job = jobs.get(originalParams.getJobId());
                if (job != null) {
                    logger.debug("updating persistent task params for job [{}]", originalParams.getJobId());

                    // copy and update the job parameters
                    OpenJobAction.JobParams updatedParams = new OpenJobAction.JobParams(originalParams.getJobId());
                    updatedParams.setTimeout(originalParams.getTimeout());
                    updatedParams.setJob(job);

                    // replace with the updated params
                    taskBuilder.removeTask(jobTask.getId());
                    taskBuilder.addTask(jobTask.getId(), jobTask.getTaskName(), updatedParams, jobTask.getAssignment());
                } else {
                    logger.error("cannot find job for task [{}]", jobTask.getId());
                }
            }
        }

        for (PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask : unallocatedDatafeedsTasks) {
            StartDatafeedAction.DatafeedParams originalParams = (StartDatafeedAction.DatafeedParams) datafeedTask.getParams();

            if (originalParams.getJobId() == null) {
                DatafeedConfig datafeedConfig = datafeeds.get(originalParams.getDatafeedId());
                if (datafeedConfig != null) {
                    logger.debug("Updating persistent task params for datafeed [{}]", originalParams.getDatafeedId());

                    StartDatafeedAction.DatafeedParams updatedParams =
                            new StartDatafeedAction.DatafeedParams(originalParams.getDatafeedId(), originalParams.getStartTime());
                    updatedParams.setTimeout(originalParams.getTimeout());
                    updatedParams.setEndTime(originalParams.getEndTime());
                    updatedParams.setJobId(datafeedConfig.getJobId());
                    updatedParams.setDatafeedIndices(datafeedConfig.getIndices());

                    // replace with the updated params
                    taskBuilder.removeTask(datafeedTask.getId());
                    taskBuilder.addTask(datafeedTask.getId(), datafeedTask.getTaskName(), updatedParams, datafeedTask.getAssignment());
                } else {
                    logger.error("cannot find datafeed for task [{}]", datafeedTask.getId());
                }
            }
        }

        return taskBuilder.build();
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
    static RemovalResult removeJobsAndDatafeeds(List<Job> jobsToRemove, List<DatafeedConfig> datafeedsToRemove, MlMetadata mlMetadata) {
        Map<String, Job> currentJobs = new HashMap<>(mlMetadata.getJobs());
        List<String> removedJobIds = new ArrayList<>();
        for (Job job : jobsToRemove) {
            if (currentJobs.remove(job.getId()) != null) {
                removedJobIds.add(job.getId());
            }
        }

        Map<String, DatafeedConfig> currentDatafeeds = new HashMap<>(mlMetadata.getDatafeeds());
        List<String> removedDatafeedIds = new ArrayList<>();
        for (DatafeedConfig datafeed : datafeedsToRemove) {
            if (currentDatafeeds.remove(datafeed.getId()) != null) {
                removedDatafeedIds.add(datafeed.getId());
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
        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.configIndexName()).id(documentId);

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
        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.jobStateIndexWriteAlias())
                .id(documentId)
                .opType(DocWriteRequest.OpType.CREATE);

        ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            mlMetadata.toXContent(builder, params);
            builder.endObject();

            indexRequest.source(builder);
        } catch (IOException e) {
            logger.error("failed to serialise ml_metadata", e);
            listener.onFailure(e);
            return;
        }

        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client, clusterService.state(), ActionListener.wrap(
            r -> {
                executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, indexRequest,
                    ActionListener.<IndexResponse>wrap(
                        indexResponse -> {
                            listener.onResponse(indexResponse.getResult() == DocWriteResponse.Result.CREATED);
                        },
                        e -> {
                            if (e instanceof VersionConflictEngineException) {
                                // the snapshot already exists
                                listener.onResponse(Boolean.TRUE);
                            } else {
                                listener.onFailure(e);
                            }
                        }),
                    client::index
                );
            },
            listener::onFailure
        ));
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
            createIndexRequest.mapping(SINGLE_MAPPING_NAME, ElasticsearchMappings.configMapping());
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
        String version = job.getJobVersion() != null ? job.getJobVersion().toString() : null;
        custom.put(MIGRATED_FROM_VERSION, version);
        builder.setCustomSettings(custom);
        Version jobVersion = job.getJobVersion();
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
     * Find the configurations for all closed jobs and the jobs that
     * do not have an allocation in the cluster state.
     * Closed jobs are those that do not have an associated persistent task,
     * unallocated jobs have a task but no executing node
     *
     * @param clusterState The cluster state
     * @return The closed job configurations
      */
    public static List<Job> closedOrUnallocatedJobs(ClusterState clusterState) {
        PersistentTasksCustomMetaData persistentTasks = clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        Set<String> openJobIds = MlTasks.openJobIds(persistentTasks);
        openJobIds.removeAll(MlTasks.unallocatedJobIds(persistentTasks, clusterState.nodes()));

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        return mlMetadata.getJobs().values().stream()
                .filter(job -> openJobIds.contains(job.getId()) == false)
                .collect(Collectors.toList());
    }

    /**
     * Find the configurations for stopped datafeeds and datafeeds that do
     * not have an allocation in the cluster state.
     * Stopped datafeeds are those that do not have an associated persistent task,
     * unallocated datafeeds have a task but no executing node.
     *
     * @param clusterState The cluster state
     * @return The closed job configurations
     */
    public static List<DatafeedConfig> stopppedOrUnallocatedDatafeeds(ClusterState clusterState) {
        PersistentTasksCustomMetaData persistentTasks = clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        Set<String> startedDatafeedIds = MlTasks.startedDatafeedIds(persistentTasks);
        startedDatafeedIds.removeAll(MlTasks.unallocatedDatafeedIds(persistentTasks, clusterState.nodes()));

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
        Collection<DatafeedConfig> stoppedDatafeeds = stopppedOrUnallocatedDatafeeds(clusterState);
        Map<String, Job> eligibleJobs = nonDeletingJobs(closedOrUnallocatedJobs(clusterState)).stream()
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

    static List<Job> filterFailedJobConfigWrites(Set<String> failedDocumentIds, List<Job> jobs) {
        return jobs.stream()
                .filter(job -> failedDocumentIds.contains(Job.documentId(job.getId())) == false)
                .collect(Collectors.toList());
    }

    static List<DatafeedConfig> filterFailedDatafeedConfigWrites(Set<String> failedDocumentIds, Collection<DatafeedConfig> datafeeds) {
        return datafeeds.stream()
                .filter(datafeed -> failedDocumentIds.contains(DatafeedConfig.documentId(datafeed.getId())) == false)
                .collect(Collectors.toList());
    }
}
