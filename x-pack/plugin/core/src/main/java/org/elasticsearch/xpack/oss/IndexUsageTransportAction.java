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
import java.util.function.Consumer;

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
        final Set<String> usedBuiltInCharFilters = new HashSet<>();
        final Set<String> usedBuiltInTokenizers = new HashSet<>();
        final Set<String> usedBuiltInTokenFilters = new HashSet<>();
        final Set<String> usedBuiltInAnalyzers = new HashSet<>();

        for (IndexMetaData indexMetaData : state.metaData()) {
            MappingMetaData mappingMetaData = indexMetaData.mapping();
            if (mappingMetaData != null) {
                visitMapping(mappingMetaData.getSourceAsMap(), fieldMapping -> {
                    Object type = fieldMapping.get("type");
                    if (type != null) {
                        usedFieldTypes.add(type.toString());
                    } else if (fieldMapping.containsKey("properties")) {
                        usedFieldTypes.add("object");
                    }

                    for (String key : new String[] { "analyzer", "search_analyzer", "search_quote_analyzer" }) {
                        Object analyzer = fieldMapping.get(key);
                        if (analyzer != null) {
                            usedBuiltInAnalyzers.add(analyzer.toString());
                        }
                    }
                });
            }

            Settings indexSettings = indexMetaData.getSettings();

            Map<String, Settings> analyzerSettings = indexSettings.getGroups("index.analysis.analyzer");
            usedBuiltInAnalyzers.removeAll(analyzerSettings.keySet());
            for (Settings analyzerSetting : analyzerSettings.values()) {
                usedAnalyzers.add(analyzerSetting.get("type", "custom"));
                usedBuiltInCharFilters.addAll(analyzerSetting.getAsList("char_filter"));
                String tokenizer = analyzerSetting.get("tokenizer");
                if (tokenizer != null) {
                    usedBuiltInTokenizers.add(tokenizer);
                }
                usedBuiltInTokenFilters.addAll(analyzerSetting.getAsList("filter"));
            }

            Map<String, Settings> charFilterSettings = indexSettings.getGroups("index.analysis.char_filter");
            usedBuiltInCharFilters.removeAll(charFilterSettings.keySet());
            aggregateAnalysisTypes(charFilterSettings.values(), usedCharFilters);

            Map<String, Settings> tokenizerSettings = indexSettings.getGroups("index.analysis.tokenizer");
            usedBuiltInTokenizers.removeAll(tokenizerSettings.keySet());
            aggregateAnalysisTypes(tokenizerSettings.values(), usedTokenizers);

            Map<String, Settings> tokenFilterSettings = indexSettings.getGroups("index.analysis.filter");
            usedBuiltInTokenFilters.removeAll(tokenFilterSettings.keySet());
            aggregateAnalysisTypes(tokenFilterSettings.values(), usedTokenFilters);
        }

        listener.onResponse(new XPackUsageFeatureResponse(
                new IndexFeatureSetUsage(usedFieldTypes,
                        usedCharFilters, usedTokenizers, usedTokenFilters, usedAnalyzers,
                        usedBuiltInCharFilters, usedBuiltInTokenizers, usedBuiltInTokenFilters, usedBuiltInAnalyzers)));
    }

    static void visitMapping(Map<String, ?> mapping, Consumer<Map<String, ?>> fieldMappingConsumer) {
        Object properties = mapping.get("properties");
        if (properties != null && properties instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> propertiesAsMap = (Map<String, ?>) properties;
            for (Object v : propertiesAsMap.values()) {
                if (v != null && v instanceof Map) {

                    @SuppressWarnings("unchecked")
                    Map<String, ?> fieldMapping = (Map<String, ?>) v;
                    fieldMappingConsumer.accept(fieldMapping);
                    visitMapping(fieldMapping, fieldMappingConsumer);

                    // Multi fields
                    Object fieldsO = fieldMapping.get("fields");
                    if (fieldsO != null && fieldsO instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> fields = (Map<String, ?>) fieldsO;
                        for (Object v2 : fields.values()) {
                            if (v2 instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, ?> fieldMapping2 = (Map<String, ?>) v2;
                                fieldMappingConsumer.accept(fieldMapping2);
                            }
                        }
                    }
                }
            }
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
