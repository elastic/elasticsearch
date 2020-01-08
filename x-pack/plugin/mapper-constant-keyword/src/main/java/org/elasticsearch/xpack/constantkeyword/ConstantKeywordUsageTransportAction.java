/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.constantkeyword;

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
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.constantkeyword.ConstantKeywordFeatureSetUsage;

import java.util.Map;

public class ConstantKeywordUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final XPackLicenseState licenseState;

    @Inject
    public ConstantKeywordUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         Settings settings, XPackLicenseState licenseState) {
        super(XPackUsageFeatureAction.CONSTANT_KEYWORD.name(), transportService, clusterService,
            threadPool, actionFilters, indexNameExpressionResolver);
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        boolean allowed = licenseState.isConstantKeywordAllowed();
        int fieldCount = 0;

        if (allowed && state != null) {
            for (IndexMetaData indexMetaData : state.metaData()) {
                MappingMetaData mappingMetaData = indexMetaData.mapping();

                if (mappingMetaData != null) {
                    Map<String, Object> mappings = mappingMetaData.getSourceAsMap();
                    fieldCount += countConstantKeywordFields(mappings);
                }
            }
        }

        ConstantKeywordFeatureSetUsage usage = new ConstantKeywordFeatureSetUsage(allowed, fieldCount);
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }

    static int countConstantKeywordFields(Map<String, ?> mapping) {
        int count = 0;
        Object properties = mapping.get("properties");
        if (properties != null && properties instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> propertiesAsMap = (Map<String, ?>) properties;
            for (Object v : propertiesAsMap.values()) {
                if (v != null && v instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, ?> fieldMapping = (Map<String, ?>) v;
                    Object fieldType = fieldMapping.get("type");
                    if (fieldType != null && fieldType.equals(ConstantKeywordFieldMapper.CONTENT_TYPE)) {
                        count++;
                    }

                    count += countConstantKeywordFields(fieldMapping);
                }
            }
        }
        return count;
    }
}
