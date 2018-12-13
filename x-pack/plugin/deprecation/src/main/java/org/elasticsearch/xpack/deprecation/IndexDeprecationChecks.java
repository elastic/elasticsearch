/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;


import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.DynamicTemplate;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Index-specific deprecation checks
 */
public class IndexDeprecationChecks {

    private static void fieldLevelMappingIssue(IndexMetaData indexMetaData, BiConsumer<MappingMetaData, Map<String, Object>> checker) {
        for (ObjectCursor<MappingMetaData> mappingMetaData : indexMetaData.getMappings().values()) {
            Map<String, Object> sourceAsMap = mappingMetaData.value.sourceAsMap();
            checker.accept(mappingMetaData.value, sourceAsMap);
        }
    }

    /**
     * iterates through the "properties" field of mappings and returns any predicates that match in the
     * form of issue-strings.
     *
     * @param type the document type
     * @param parentMap the mapping to read properties from
     * @param predicate the predicate to check against for issues, issue is returned if predicate evaluates to true
     * @return a list of issues found in fields
     */
    @SuppressWarnings("unchecked")
    private static List<String> findInPropertiesRecursively(String type, Map<String, Object> parentMap,
                                                    Function<Map<?,?>, Boolean> predicate) {
        List<String> issues = new ArrayList<>();
        Map<?, ?> properties = (Map<?, ?>) parentMap.get("properties");
        if (properties == null) {
            return issues;
        }
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            Map<String, Object> valueMap = (Map<String, Object>) entry.getValue();
            if (predicate.apply(valueMap)) {
                issues.add("[type: " + type + ", field: " + entry.getKey() + "]");
            }

            Map<?, ?> values = (Map<?, ?>) valueMap.get("fields");
            if (values != null) {
                for (Map.Entry<?, ?> multifieldEntry : values.entrySet()) {
                    Map<String, Object> multifieldValueMap = (Map<String, Object>) multifieldEntry.getValue();
                    if (predicate.apply(multifieldValueMap)) {
                        issues.add("[type: " + type + ", field: " + entry.getKey() + ", multifield: " + multifieldEntry.getKey() + "]");
                    }
                    if (multifieldValueMap.containsKey("properties")) {
                        issues.addAll(findInPropertiesRecursively(type, multifieldValueMap, predicate));
                    }
                }
            }
            if (valueMap.containsKey("properties")) {
                issues.addAll(findInPropertiesRecursively(type, valueMap, predicate));
            }
        }

        return issues;
    }

    static DeprecationIssue dynamicTemplateWithMatchMappingTypeCheck(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().before(Version.V_6_0_0_alpha1)) {
            List<String> issues = new ArrayList<>();
            fieldLevelMappingIssue(indexMetaData, (mappingMetaData, sourceAsMap) -> {
                List<?> dynamicTemplates = (List<?>) mappingMetaData
                    .getSourceAsMap().getOrDefault("dynamic_templates", Collections.emptyList());
                for (Object template : dynamicTemplates) {
                    for (Map.Entry<?, ?> prop : ((Map<?, ?>) template).entrySet()) {
                        Map<?, ?> val = (Map<?, ?>) prop.getValue();
                        if (val.containsKey("match_mapping_type")) {
                            Object mappingMatchType = val.get("match_mapping_type");
                            boolean isValidMatchType = Arrays.stream(DynamicTemplate.XContentFieldType.values())
                                .anyMatch(v -> v.toString().equals(mappingMatchType));
                            if (isValidMatchType == false) {
                                issues.add("type: " + mappingMetaData.type() + ", dynamicFieldDefinition"
                                    + prop.getKey() + ", unknown match_mapping_type[" + mappingMatchType + "]");
                            }
                        }
                    }
                }
            });
            if (issues.size() > 0) {
                return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                    "Unrecognized match_mapping_type options not silently ignored",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                        "breaking_60_mappings_changes.html" +
                        "#_unrecognized_literal_match_mapping_type_literal_options_not_silently_ignored",
                    issues.toString());
            }
        }
        return null;
    }

    static DeprecationIssue baseSimilarityDefinedCheck(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().before(Version.V_6_0_0_alpha1)) {
            Settings settings = indexMetaData.getSettings().getAsSettings("index.similarity.base");
            if (settings.size() > 0) {
                return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                    "The base similarity is now ignored as coords and query normalization have been removed." +
                        "If provided, this setting will be ignored and issue a deprecation warning",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                        "breaking_60_settings_changes.html#_similarity_settings", null);

            }
        }
        return null;
    }

    static DeprecationIssue delimitedPayloadFilterCheck(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().before(Version.V_7_0_0)) {
            List<String> issues = new ArrayList<>();
                Map<String, Settings> filters = indexMetaData.getSettings().getGroups(AnalysisRegistry.INDEX_ANALYSIS_FILTER);
                for (Map.Entry<String, Settings> entry : filters.entrySet()) {
                if ("delimited_payload_filter".equals(entry.getValue().get("type"))) {
                    issues.add("The filter [" + entry.getKey() + "] is of deprecated 'delimited_payload_filter' type. "
                            + "The filter type should be changed to 'delimited_payload'.");
                }
                }
            if (issues.size() > 0) {
                return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                        "Use of 'delimited_payload_filter'.",
                        "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_analysis_changes.html",
                        issues.toString());
            }
        }
        return null;
    }

    static DeprecationIssue indexStoreTypeCheck(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().before(Version.V_6_0_0_alpha1) &&
            indexMetaData.getSettings().get("index.store.type") != null) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "The default index.store.type has been removed. If you were using it, " +
                    "we advise that you simply remove it from your index settings and Elasticsearch" +
                    "will use the best store implementation for your operating system.",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                    "breaking_60_settings_changes.html#_store_settings", null);

        }
        return null;
    }

    static DeprecationIssue storeThrottleSettingsCheck(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().before(Version.V_6_0_0_alpha1)) {
            Settings settings = indexMetaData.getSettings();
            Settings throttleSettings = settings.getAsSettings("index.store.throttle");
            ArrayList<String> foundSettings = new ArrayList<>();
            if (throttleSettings.get("max_bytes_per_sec") != null) {
                foundSettings.add("index.store.throttle.max_bytes_per_sec");
            }
            if (throttleSettings.get("type") != null) {
                foundSettings.add("index.store.throttle.type");
            }

            if (foundSettings.isEmpty() == false) {
                return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                    "index.store.throttle settings are no longer recognized. these settings should be removed",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                        "breaking_60_settings_changes.html#_store_throttling_settings", "present settings: " + foundSettings);
            }
        }
        return null;
    }

    static DeprecationIssue indexSharedFileSystemCheck(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().before(Version.V_6_0_0_alpha1) &&
            indexMetaData.getSettings().get("index.shared_filesystem") != null) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "[index.shared_filesystem] setting should be removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/6.0/" +
                    "breaking_60_indices_changes.html#_shadow_replicas_have_been_removed", null);

        }
        return null;
    }
}
