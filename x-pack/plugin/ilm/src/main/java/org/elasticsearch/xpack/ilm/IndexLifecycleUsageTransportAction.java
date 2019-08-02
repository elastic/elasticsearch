/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexLifecycleUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public IndexLifecycleUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                              ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                              Settings settings, XPackLicenseState licenseState) {
        super(XPackUsageFeatureAction.INDEX_LIFECYCLE.name(), transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver);
        this.enabled = XPackSettings.INDEX_LIFECYCLE_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        boolean available = licenseState.isIndexLifecycleAllowed();
        MetaData metaData = state.metaData();
        IndexLifecycleMetadata lifecycleMetadata = metaData.custom(IndexLifecycleMetadata.TYPE);
        final IndexLifecycleFeatureSetUsage usage;
        if (enabled && lifecycleMetadata != null) {
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
            List<IndexLifecycleFeatureSetUsage.PolicyStats> policyStats = lifecycleMetadata.getPolicies().values().stream().map(policy -> {
                Map<String, IndexLifecycleFeatureSetUsage.PhaseStats> phaseStats = policy.getPhases().values().stream().map(phase -> {
                    String[] actionNames = phase.getActions().keySet().toArray(new String[phase.getActions().size()]);
                    return new Tuple<>(phase.getName(), new IndexLifecycleFeatureSetUsage.PhaseStats(phase.getMinimumAge(), actionNames));
                }).collect(Collectors.toMap(Tuple::v1, Tuple::v2));
                return new IndexLifecycleFeatureSetUsage.PolicyStats(phaseStats, policyUsage.getOrDefault(policy.getName(), 0));
            }).collect(Collectors.toList());
            usage = new IndexLifecycleFeatureSetUsage(available, enabled, policyStats);
        } else {
            usage = new IndexLifecycleFeatureSetUsage(available, enabled);
        }
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
