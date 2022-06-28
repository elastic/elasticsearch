/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.ActionConfigStats;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PhaseStats;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PolicyStats;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
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

public class IndexLifecycleFeatureSet implements XPackFeatureSet {

    private ClusterService clusterService;

    @Inject
    public IndexLifecycleFeatureSet(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.INDEX_LIFECYCLE;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        Metadata metadata = clusterService.state().metadata();
        IndexLifecycleMetadata lifecycleMetadata = metadata.custom(IndexLifecycleMetadata.TYPE);
        if (lifecycleMetadata != null) {
            Map<String, Integer> policyUsage = new HashMap<>();
            metadata.indices().forEach(entry -> {
                String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(entry.value.getSettings());
                Integer indicesManaged = policyUsage.get(policyName);
                if (indicesManaged == null) {
                    indicesManaged = 1;
                } else {
                    indicesManaged = indicesManaged + 1;
                }
                policyUsage.put(policyName, indicesManaged);
            });
            List<PolicyStats> policyStats = lifecycleMetadata.getPolicies().values().stream().map(policy -> {
                Map<String, PhaseStats> phaseStats = policy.getPhases().values().stream().map(phase -> {
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
                return new PolicyStats(phaseStats, policyUsage.getOrDefault(policy.getName(), 0));
            }).collect(Collectors.toList());
            listener.onResponse(new IndexLifecycleFeatureSetUsage(policyStats));
        } else {
            listener.onResponse(new IndexLifecycleFeatureSetUsage());
        }
    }

    private void collectActionConfigurations(String actionName, LifecycleAction action, ActionConfigStats.Builder consumer) {
        switch (actionName) {
            case AllocateAction.NAME:
                AllocateAction allocateAction = (AllocateAction) action;
                consumer.setAllocateNumberOfReplicas(allocateAction.getNumberOfReplicas());
                break;
            case ForceMergeAction.NAME:
                ForceMergeAction forceMergeAction = (ForceMergeAction) action;
                consumer.setForceMergeMaxNumberOfSegments(forceMergeAction.getMaxNumSegments());
                break;
            case RolloverAction.NAME:
                RolloverAction rolloverAction = (RolloverAction) action;
                consumer.setRolloverMaxAge(rolloverAction.getMaxAge());
                consumer.setRolloverMaxDocs(rolloverAction.getMaxDocs());
                consumer.setRolloverMaxPrimaryShardSize(rolloverAction.getMaxPrimaryShardSize());
                consumer.setRolloverMaxSize(rolloverAction.getMaxSize());
                break;
            case SetPriorityAction.NAME:
                SetPriorityAction setPriorityAction = (SetPriorityAction) action;
                consumer.setPriority(setPriorityAction.getRecoveryPriority());
                break;
            case ShrinkAction.NAME:
                ShrinkAction shrinkAction = (ShrinkAction) action;
                consumer.setShrinkMaxPrimaryShardSize(shrinkAction.getMaxPrimaryShardSize());
                consumer.setShrinkNumberOfShards(shrinkAction.getNumberOfShards());
                break;
        }
    }
}
