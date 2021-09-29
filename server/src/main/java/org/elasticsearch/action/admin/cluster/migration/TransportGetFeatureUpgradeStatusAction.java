/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.IndexPatternMatcher;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Transport class for the get feature upgrade status action
 */
public class TransportGetFeatureUpgradeStatusAction extends TransportMasterNodeAction<
        GetFeatureUpgradeStatusRequest,
        GetFeatureUpgradeStatusResponse> {

    private final SystemIndices systemIndices;

    @Inject
    public TransportGetFeatureUpgradeStatusAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            GetFeatureUpgradeStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetFeatureUpgradeStatusRequest::new,
            indexNameExpressionResolver,
            GetFeatureUpgradeStatusResponse::new,
            ThreadPool.Names.SAME
        );
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(Task task, GetFeatureUpgradeStatusRequest request, ClusterState state,
                                   ActionListener<GetFeatureUpgradeStatusResponse> listener) throws Exception {

        List<GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus> features = new ArrayList<>();
        boolean isUpgradeNeeded = false;

        for (Map.Entry<String, SystemIndices.Feature> featureEntry : systemIndices.getFeatures().entrySet()) {
            String featureName = featureEntry.getKey();
            SystemIndices.Feature feature = featureEntry.getValue();

            Version minimumVersion = Version.CURRENT;

            List<GetFeatureUpgradeStatusResponse.IndexVersion> indexVersions = new ArrayList<>();
            List<IndexPatternMatcher> descriptors = Stream.concat(feature.getIndexDescriptors().stream(),
                feature.getAssociatedIndexDescriptors().stream()).collect(Collectors.toList());
            for (IndexPatternMatcher descriptor : descriptors) {
                List<String> concreteIndices = descriptor.getMatchingIndices(state.metadata());
                for (String index : concreteIndices) {
                    IndexMetadata metadata = state.metadata().index(index);
                    indexVersions.add(new GetFeatureUpgradeStatusResponse.IndexVersion(index, metadata.getCreationVersion().toString()));
                    minimumVersion = Version.min(metadata.getCreationVersion(), minimumVersion);
                }
            }

            if (minimumVersion.before(Version.V_7_0_0)) {
                isUpgradeNeeded = true;
            }

            features.add(new GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(
                featureName,
                minimumVersion.toString(),
                minimumVersion.before(Version.V_7_0_0) ? "UPGRADE_NEEDED" : "NO_UPGRADE_NEEDED",
                indexVersions
            ));
        }


        listener.onResponse(new GetFeatureUpgradeStatusResponse(features, isUpgradeNeeded ? "UPGRADE_NEEDED" : "NO_UPGRADE_NEEDED"));
    }

    @Override
    protected ClusterBlockException checkBlock(GetFeatureUpgradeStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
