/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.ActionConfigStats;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.shouldInjectMigrateStepForPhase;

public class IndexLifecycleUsageTransportAction extends XPackUsageFeatureTransportAction {

    @Inject
    public IndexLifecycleUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(XPackUsageFeatureAction.INDEX_LIFECYCLE.name(), transportService, clusterService, threadPool, actionFilters);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        Metadata metadata = state.metadata();
        IndexLifecycleMetadata lifecycleMetadata = metadata.getProject().custom(IndexLifecycleMetadata.TYPE);
        final IndexLifecycleFeatureSetUsage usage;
        if (lifecycleMetadata != null) {
            Map<String, Integer> policyUsage = new HashMap<>();
            metadata.getProject().indices().values().forEach(value -> {
                String policyName = value.getLifecyclePolicyName();
                Integer indicesManaged = policyUsage.get(policyName);
                if (indicesManaged == null) {
                    indicesManaged = 1;
                } else {
                    indicesManaged = indicesManaged + 1;
                }
                policyUsage.put(policyName, indicesManaged);
            });
            List<IndexLifecycleFeatureSetUsage.PolicyStats> policyStats = lifecycleMetadata.getPolicies().values().stream().map(policy -> {
                Map<String, IndexLifecycleFeatureSetUsage.PhaseStats> phaseStats = policy.getPhases().values().stream().map(phase -> {
                    ActionConfigStats.Builder configurations = ActionConfigStats.builder();
                    Stream<String> actionStream = phase.getActions().keySet().stream();
                    if (policy.getType() instanceof TimeseriesLifecycleType && shouldInjectMigrateStepForPhase(phase)) {
                        actionStream = Stream.concat(actionStream, Stream.of("migrate"));
                    }
                    String[] actionNames = actionStream.toArray(String[]::new);
                    phase.getActions().forEach((k, v) -> collectActionConfigurations(k, v, configurations));
                    return new Tuple<>(
                        phase.getName(),
                        new IndexLifecycleFeatureSetUsage.PhaseStats(phase.getMinimumAge(), actionNames, configurations.build())
                    );
                }).collect(Collectors.toMap(Tuple::v1, Tuple::v2));
                return new IndexLifecycleFeatureSetUsage.PolicyStats(phaseStats, policyUsage.getOrDefault(policy.getName(), 0));
            }).toList();
            usage = new IndexLifecycleFeatureSetUsage(policyStats);
        } else {
            usage = new IndexLifecycleFeatureSetUsage();
        }
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }

    private static void collectActionConfigurations(String actionName, LifecycleAction action, ActionConfigStats.Builder consumer) {
        switch (actionName) {
            case AllocateAction.NAME -> {
                AllocateAction allocateAction = (AllocateAction) action;
                consumer.setAllocateNumberOfReplicas(allocateAction.getNumberOfReplicas());
            }
            case ForceMergeAction.NAME -> {
                ForceMergeAction forceMergeAction = (ForceMergeAction) action;
                consumer.setForceMergeMaxNumberOfSegments(forceMergeAction.getMaxNumSegments());
            }
            case RolloverAction.NAME -> {
                RolloverConditions rolloverConditions = ((RolloverAction) action).getConditions();
                consumer.setRolloverMaxAge(rolloverConditions.getMaxAge());
                consumer.setRolloverMaxDocs(rolloverConditions.getMaxDocs());
                consumer.setRolloverMaxPrimaryShardDocs(rolloverConditions.getMaxPrimaryShardDocs());
                consumer.setRolloverMaxPrimaryShardSize(rolloverConditions.getMaxPrimaryShardSize());
                consumer.setRolloverMaxSize(rolloverConditions.getMaxSize());
                consumer.setRolloverMinAge(rolloverConditions.getMinAge());
                consumer.setRolloverMinDocs(rolloverConditions.getMinDocs());
                consumer.setRolloverMinPrimaryShardDocs(rolloverConditions.getMinPrimaryShardDocs());
                consumer.setRolloverMinPrimaryShardSize(rolloverConditions.getMinPrimaryShardSize());
                consumer.setRolloverMinSize(rolloverConditions.getMinSize());
            }
            case SetPriorityAction.NAME -> {
                SetPriorityAction setPriorityAction = (SetPriorityAction) action;
                consumer.setPriority(setPriorityAction.getRecoveryPriority());
            }
            case ShrinkAction.NAME -> {
                ShrinkAction shrinkAction = (ShrinkAction) action;
                consumer.setShrinkMaxPrimaryShardSize(shrinkAction.getMaxPrimaryShardSize());
                consumer.setShrinkNumberOfShards(shrinkAction.getNumberOfShards());
            }
        }
    }
}
