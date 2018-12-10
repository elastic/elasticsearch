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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
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

    static DeprecationIssue delimitedPayloadFilterCheck(IndexMetaData indexMetaData) {
        List<String> issues = new ArrayList<>();
        Map<String, Settings> filters = indexMetaData.getSettings().getGroups(AnalysisRegistry.INDEX_ANALYSIS_FILTER);
        for (Map.Entry<String, Settings> entry : filters.entrySet()) {
            if ("delimited_payload_filter".equals(entry.getValue().get("type"))) {
                issues.add("The filter [" + entry.getKey() + "] is of deprecated 'delimited_payload_filter' type. "
                    + "The filter type should be changed to 'delimited_payload'.");
            }
        }
        if (issues.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING, "Use of 'delimited_payload_filter'.",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_analysis_changes.html", issues.toString());
        }
        return null;
    }

    static DeprecationIssue oldIndicesCheck(IndexMetaData indexMetaData) {
        Version createdWith = indexMetaData.getCreationVersion();
        if (createdWith.before(Version.V_6_0_0)) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Index created before 6.0",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                    "breaking-changes-7.0.html",
                "this index was created using version: " + createdWith);

        }
        return null;
    }

    static DeprecationIssue indexNameCheck(IndexMetaData indexMetaData) {
        String clusterName = indexMetaData.getIndex().getName();
        if (clusterName.contains(":")) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Index name cannot contain ':'",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_literal_literal_is_no_longer_allowed_in_index_name",
                "This index is named [" + clusterName + "], which contains the illegal character ':'.");
        }
        return null;
    }

    static DeprecationIssue shardOnStartupCheck(IndexMetaData indexMetaData) {
        String setting = IndexSettings.INDEX_CHECK_ON_STARTUP.getKey();
        String value = indexMetaData.getSettings().get(setting);
        if (Strings.isNullOrEmpty(value) == false) {
            if ("fix".equalsIgnoreCase(value)) {
                return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                    "The value 'fix' for setting index.shard.check_on_startup is no longer valid",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                        "#_literal_fix_literal_value_for_literal_index_shard_check_on_startup_literal_is_removed",
                    "The index [" + indexMetaData.getIndex().getName() + "] has the setting index.shard.check_on_startup = 'fix'. " +
                        "Valid values are 'true', 'false', and 'checksum'");
            }
        }
        return null;
    }
}

