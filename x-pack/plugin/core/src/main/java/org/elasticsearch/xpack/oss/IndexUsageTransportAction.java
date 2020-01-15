/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.oss;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexUsageTransportAction extends XPackUsageFeatureTransportAction {

    @Inject
    public IndexUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(XPackUsageFeatureAction.INDEX.name(), transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver);
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {

        final Set<String> usedFieldTypes = new HashSet<>();
        final Set<String> usedCharFilters = new HashSet<>();
        final Set<String> usedTokenizers = new HashSet<>();
        final Set<String> usedTokenFilters = new HashSet<>();
        final Set<String> usedAnalyzers = new HashSet<>();

        for (IndexMetaData indexMetaData : state.metaData()) {
            MappingMetaData mappingMetaData = indexMetaData.mapping();
            if (mappingMetaData != null) {
                populateFieldTypesFromObject(mappingMetaData.sourceAsMap(), usedFieldTypes);
            }

            Settings indexSettings = indexMetaData.getSettings();

            Map<String, Settings> charFilterSettings = indexSettings.getGroups("index.analysis.char_filter");
            aggregateAnalysisTypes(charFilterSettings.values(), usedCharFilters);

            Map<String, Settings> tokenizerSettings = indexSettings.getGroups("index.analysis.tokenizer");
            aggregateAnalysisTypes(tokenizerSettings.values(), usedTokenizers);

            Map<String, Settings> tokenFilterSettings = indexSettings.getGroups("index.analysis.filter");
            aggregateAnalysisTypes(tokenFilterSettings.values(), usedTokenFilters);

            Map<String, Settings> analyzerSettings = indexSettings.getGroups("index.analysis.analyzer");
            aggregateAnalysisTypes(analyzerSettings.values(), usedAnalyzers);
        }

        listener.onResponse(new XPackUsageFeatureResponse(
                new IndexFeatureSetUsage(usedFieldTypes, usedCharFilters, usedTokenizers, usedTokenFilters, usedAnalyzers)));
    }

    static void populateFieldTypesFromObject(Map<String, ?> mapping, Set<String> fieldTypes) {
        Object properties = mapping.get("properties");
        if (properties != null && properties instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> propertiesAsMap = (Map<String, ?>) properties;
            for (Object v : propertiesAsMap.values()) {
                if (v != null && v instanceof Map) {

                    @SuppressWarnings("unchecked")
                    Map<String, ?> fieldMapping = (Map<String, ?>) v;
                    populateFieldTypesFromField(fieldMapping, fieldTypes);
                    populateFieldTypesFromObject(fieldMapping, fieldTypes);

                    // Multi fields
                    Object fieldsO = fieldMapping.get("fields");
                    if (fieldsO != null && fieldsO instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> fields = (Map<String, ?>) fieldsO;
                        for (Object v2 : fields.values()) {
                            if (v2 instanceof Map) {
                                Map<String, ?> fieldMapping2 = (Map<String, ?>) v2;
                                populateFieldTypesFromField(fieldMapping2, fieldTypes);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void populateFieldTypesFromField(Map<String, ?> mapping, Set<String> fieldTypes) {
        Object fieldType = mapping.get("type");
        if (fieldType != null) {
            fieldTypes.add(fieldType.toString());
        } else if (mapping.containsKey("properties")) {
            fieldTypes.add("object");
        }
    }

    static void aggregateAnalysisTypes(Collection<Settings> analysisComponents, Set<String> usedTypes) {
        for (Settings settings : analysisComponents) {
            String type = settings.get("type");
            if (type != null) {
                usedTypes.add(type);
            }
        }
    }
}
