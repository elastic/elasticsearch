/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleFeatureSetUsage.PhaseStats;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleFeatureSetUsage.PolicyStats;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexLifecycleFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private ClusterService clusterService;

    @Inject
    public IndexLifecycleFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.enabled = XPackSettings.INDEX_LIFECYCLE_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.INDEX_LIFECYCLE;
    }

    @Override
    public String description() {
        return "Index lifecycle management for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isIndexLifecycleAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        MetaData metaData = clusterService.state().metaData();
        IndexLifecycleMetadata lifecycleMetadata = metaData.custom(IndexLifecycleMetadata.TYPE);
        if (enabled() && lifecycleMetadata != null) {
            Map<String, Integer> policyUsage = new HashMap<>();
            metaData.indices().forEach(entry -> {
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
                    String[] actionNames = phase.getActions().keySet().toArray(new String[phase.getActions().size()]);
                    return new Tuple<String, PhaseStats>(phase.getName(), new PhaseStats(phase.getMinimumAge(), actionNames));
                }).collect(Collectors.toMap(Tuple::v1, Tuple::v2));
                return new PolicyStats(phaseStats, policyUsage.getOrDefault(policy.getName(), 0));
            }).collect(Collectors.toList());
            listener.onResponse(new IndexLifecycleFeatureSetUsage(available(), enabled(), policyStats));
        } else {
            listener.onResponse(new IndexLifecycleFeatureSetUsage(available(), enabled()));
        }
    }

}
