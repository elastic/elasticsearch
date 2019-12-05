/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.wildcard.WildcardFeatureSetUsage;
import org.elasticsearch.xpack.wildcard.mapper.WildcardFieldMapper;

import java.util.Map;

public class WildcardUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Settings settings;
    private final XPackLicenseState licenseState;

    @Inject
    public WildcardUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                       Settings settings, XPackLicenseState licenseState) {
        super(XPackUsageFeatureAction.WILDCARD.name(), transportService, clusterService,
            threadPool, actionFilters, indexNameExpressionResolver);
        this.settings = settings;
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        boolean wildcardAvailable = licenseState.isWildcardAllowed();
        int numWildcardFields = 0;

        if (wildcardAvailable && state != null) {
            for (IndexMetaData indexMetaData : state.metaData()) {
                MappingMetaData mappingMetaData = indexMetaData.mapping();
                if (mappingMetaData != null) {
                    Map<String, Object> mappings = mappingMetaData.getSourceAsMap();
                    if (mappings.containsKey("properties")) {
                        @SuppressWarnings("unchecked") Map<String, Map<String, Object>> fieldMappings =
                            (Map<String, Map<String, Object>>) mappings.get("properties");
                        for (Map<String, Object> typeDefinition : fieldMappings.values()) {
                            String fieldType = (String) typeDefinition.get("type");
                            if (fieldType != null) {
                                if (fieldType.equals(WildcardFieldMapper.CONTENT_TYPE)) {
                                    numWildcardFields++;
                                }
                            }
                        }
                    }
                }
            }
        }
        WildcardFeatureSetUsage usage = new WildcardFeatureSetUsage(wildcardAvailable, numWildcardFields);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
