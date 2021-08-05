/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.NativeMemoryCapacity;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.core.ml.MlTasks.RESET_IN_PROGRESS;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.JOB_AUDIT_REQUIRES_MORE_MEMORY_TO_RUN;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_ML_NODE_SIZE;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;
import static org.elasticsearch.xpack.ml.MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

public abstract class AbstractJobPersistentTasksExecutor<Params extends PersistentTaskParams> extends PersistentTasksExecutor<Params> {

    private static final Logger logger = LogManager.getLogger(AbstractJobPersistentTasksExecutor.class);
    public static final PersistentTasksCustomMetadata.Assignment AWAITING_MIGRATION =
        new PersistentTasksCustomMetadata.Assignment(null, "job cannot be assigned until it has been migrated.");

    public static List<String> verifyIndicesPrimaryShardsAreActive(ClusterState clusterState,
                                                                   IndexNameExpressionResolver expressionResolver,
                                                                   boolean allowMissing,
                                                                   String... indicesOfInterest) {
        String[] indices = expressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), indicesOfInterest);
        List<String> unavailableIndices = new ArrayList<>(indices.length);
        for (String index : indices) {
            // Indices are created on demand from templates.
            // It is not an error if the index doesn't exist yet
            if (clusterState.metadata().hasIndex(index) == false) {
                if (allowMissing == false) {
                    unavailableIndices.add(index);
                }
                continue;
            }
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                unavailableIndices.add(index);
            }
        }
        return unavailableIndices;
    }


    protected final MlMemoryTracker memoryTracker;
    protected final IndexNameExpressionResolver expressionResolver;
    protected final Cache<String, Long> auditedJobCapacity = CacheBuilder.<String, Long>builder()
        // Using a TTL cache here so jobs that are awaiting assignment but get killed via the `_stop` API are gracefully removed
        // Also, if a job is awaiting assignment for 30 minutes, writing another audit message is probably acceptable.
        .setExpireAfterWrite(TimeValue.timeValueMinutes(30))
        .build();

    protected volatile int maxConcurrentJobAllocations;
    protected volatile int maxMachineMemoryPercent;
    protected volatile int maxLazyMLNodes;
    protected volatile boolean useAutoMemoryPercentage;
    protected volatile long maxNodeMemory;
    protected volatile int maxOpenJobs;

    protected AbstractJobPersistentTasksExecutor(String taskName,
                                                 String executor,
                                                 Settings settings,
                                                 ClusterService clusterService,
                                                 MlMemoryTracker memoryTracker,
                                                 IndexNameExpressionResolver expressionResolver) {
        super(taskName, executor);
        this.memoryTracker = Objects.requireNonNull(memoryTracker);
        this.expressionResolver = Objects.requireNonNull(expressionResolver);
        this.maxConcurrentJobAllocations = MachineLearning.CONCURRENT_JOB_ALLOCATIONS.get(settings);
        this.maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
        this.maxLazyMLNodes = MachineLearning.MAX_LAZY_ML_NODES.get(settings);
        this.maxOpenJobs = MAX_OPEN_JOBS_PER_NODE.get(settings);
        this.useAutoMemoryPercentage = USE_AUTO_MACHINE_MEMORY_PERCENT.get(settings);
        this.maxNodeMemory = MAX_ML_NODE_SIZE.get(settings).getBytes();
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MachineLearning.CONCURRENT_JOB_ALLOCATIONS, this::setMaxConcurrentJobAllocations);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, this::setMaxMachineMemoryPercent);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_LAZY_ML_NODES, this::setMaxLazyMLNodes);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(USE_AUTO_MACHINE_MEMORY_PERCENT, this::setUseAutoMemoryPercentage);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ML_NODE_SIZE, this::setMaxNodeSize);
    }

    protected String getUniqueId(String jobId) {
        return getTaskName() + "-" + jobId;
    }

    protected void auditRequireMemoryIfNecessary(String jobId,
                                                 AbstractAuditor<?> auditor,
                                                 PersistentTasksCustomMetadata.Assignment assignment,
                                                 JobNodeSelector jobNodeSelector,
                                                 boolean isMemoryTrackerRecentlyRefreshed) {
        if (assignment.equals(AWAITING_LAZY_ASSIGNMENT)) {
            if (isMemoryTrackerRecentlyRefreshed) {
                Tuple<NativeMemoryCapacity, Long> capacityAndFreeMemory = jobNodeSelector.perceivedCapacityAndMaxFreeMemory(
                    maxMachineMemoryPercent,
                    useAutoMemoryPercentage,
                    maxOpenJobs
                );
                Long previouslyAuditedFreeMemory = auditedJobCapacity.get(getUniqueId(jobId));
                if (capacityAndFreeMemory.v2().equals(previouslyAuditedFreeMemory) == false) {
                    auditor.info(jobId,
                        Messages.getMessage(JOB_AUDIT_REQUIRES_MORE_MEMORY_TO_RUN,
                            ByteSizeValue.ofBytes(memoryTracker.getJobMemoryRequirement(getTaskName(), jobId)),
                            ByteSizeValue.ofBytes(capacityAndFreeMemory.v2()),
                            ByteSizeValue.ofBytes(capacityAndFreeMemory.v1().getTier()),
                            ByteSizeValue.ofBytes(capacityAndFreeMemory.v1().getNode())));
                    auditedJobCapacity.put(getUniqueId(jobId), capacityAndFreeMemory.v2());
                }
            }
        } else {
            auditedJobCapacity.invalidate(getUniqueId(jobId));
        }
    }

    protected abstract String[] indicesOfInterest(Params params);
    protected abstract String getJobId(Params params);
    protected boolean allowsMissingIndices() {
        return true;
    }

    public Optional<PersistentTasksCustomMetadata.Assignment> getPotentialAssignment(Params params, ClusterState clusterState,
                                                                                     boolean isMemoryTrackerRecentlyRefreshed) {
        // If we are waiting for an upgrade or reset to complete, we should not assign to a node
        if (MlMetadata.getMlMetadata(clusterState).isUpgradeMode()) {
            return Optional.of(AWAITING_UPGRADE);
        }
        if (MlMetadata.getMlMetadata(clusterState).isResetMode()) {
            return Optional.of(RESET_IN_PROGRESS);
        }
        String jobId = getJobId(params);

        Optional<PersistentTasksCustomMetadata.Assignment> missingIndices = checkRequiredIndices(jobId,
            clusterState,
            indicesOfInterest(params));
        if (missingIndices.isPresent()) {
            return missingIndices;
        }
        Optional<PersistentTasksCustomMetadata.Assignment> staleMemory = checkMemoryFreshness(jobId, isMemoryTrackerRecentlyRefreshed);
        if (staleMemory.isPresent()) {
            return staleMemory;
        }
        return Optional.empty();
    }

    void setMaxConcurrentJobAllocations(int maxConcurrentJobAllocations) {
        this.maxConcurrentJobAllocations = maxConcurrentJobAllocations;
    }

    void setMaxMachineMemoryPercent(int maxMachineMemoryPercent) {
        this.maxMachineMemoryPercent = maxMachineMemoryPercent;
    }

    void setMaxLazyMLNodes(int maxLazyMLNodes) {
        this.maxLazyMLNodes = maxLazyMLNodes;
    }

    void setMaxOpenJobs(int maxOpenJobs) {
        this.maxOpenJobs = maxOpenJobs;
    }

    void setUseAutoMemoryPercentage(boolean useAutoMemoryPercentage) {
        this.useAutoMemoryPercentage = useAutoMemoryPercentage;
    }

    void setMaxNodeSize(ByteSizeValue maxNodeSize) {
        this.maxNodeMemory = maxNodeSize.getBytes();
    }

    public Optional<PersistentTasksCustomMetadata.Assignment> checkRequiredIndices(String jobId,
                                                                                   ClusterState clusterState,
                                                                                   String... indicesOfInterest) {
        List<String> unavailableIndices = verifyIndicesPrimaryShardsAreActive(clusterState,
            expressionResolver,
            allowsMissingIndices(),
            indicesOfInterest);
        if (unavailableIndices.size() != 0) {
            String reason = "Not opening [" + jobId + "], because not all primary shards are active for the following indices [" +
                String.join(",", unavailableIndices) + "]";
            logger.debug(reason);
            return Optional.of(new PersistentTasksCustomMetadata.Assignment(null, reason));
        }
        return Optional.empty();
    }

    public Optional<PersistentTasksCustomMetadata.Assignment> checkMemoryFreshness(String jobId, boolean isMemoryTrackerRecentlyRefreshed) {
        if (isMemoryTrackerRecentlyRefreshed == false) {
            boolean scheduledRefresh = memoryTracker.asyncRefresh();
            if (scheduledRefresh) {
                String reason = "Not opening job [" + jobId + "] because job memory requirements are stale - refresh requested";
                logger.debug(reason);
                return Optional.of(new PersistentTasksCustomMetadata.Assignment(null, reason));
            }
        }
        return Optional.empty();
    }

}
