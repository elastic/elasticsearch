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
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.MapperService.DEFAULT_MAPPING;

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
    static List<String> findInPropertiesRecursively(String type, Map<String, Object> parentMap,
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
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_literal_delimited_payload_filter_literal_renaming", issues.toString());
        }
        return null;
    }

    static DeprecationIssue oldIndicesCheck(IndexMetaData indexMetaData) {
        Version createdWith = indexMetaData.getCreationVersion();
        boolean hasDefaultMapping = indexMetaData.getMappings().containsKey(DEFAULT_MAPPING);
        int mappingCount = indexMetaData.getMappings().size();
        if (createdWith.before(Version.V_6_0_0)) {
            if (".tasks".equals(indexMetaData.getIndex().getName())) {
                return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                    ".tasks index must be re-created",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                        "#_indices_created_before_7_0",
                    "The .tasks index was created before version 6.0 and cannot be opened in 7.0. " +
                        "You must delete this index and allow it to be re-created by Elasticsearch. If you wish to preserve task history, "+
                        "reindex this index to a new index before deleting it.");
            } else if (".watches".equals(indexMetaData.getIndex().getName())) {
                return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                    ".watches was not properly upgraded before upgrading to Elasticsearch 6",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/current/migration-api-upgrade.html",
                    "The .watches index was created before version 6.0, and was not properly upgraded in 5.6. " +
                        "Please upgrade this index using the Migration Upgrade API.");
            }
            if ((mappingCount == 2 && !hasDefaultMapping)
                || mappingCount > 2) {
                return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                    "Index has more than one mapping type",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/removal-of-types.html" +
                        "#_migrating_multi_type_indices_to_single_type",
                    "This index has more than one mapping type, which is not supported in 7.0. " +
                        "This index must be reindexed into one or more single-type indices. Mapping types in use: " +
                        indexMetaData.getMappings().keys());
            } else {
                return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                    "Index created before 6.0",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                        "#_indices_created_before_7_0",
                    "This index was created using version: " + createdWith);
            }

        }
        return null;
    }

    static DeprecationIssue indexNameCheck(IndexMetaData indexMetaData) {
        String clusterName = indexMetaData.getIndex().getName();
        if (clusterName.contains(":")) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Index name cannot contain ':'",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_literal_literal_is_no_longer_allowed_in_index_name",
                "This index is named [" + clusterName + "], which contains the illegal character ':'.");
        }
        return null;
    }

    static DeprecationIssue percolatorUnmappedFieldsAsStringCheck(IndexMetaData indexMetaData) {
        if (indexMetaData.getSettings().hasValue("index.percolator.map_unmapped_fields_as_text")) {
            String settingValue = indexMetaData.getSettings().get("index.percolator.map_unmapped_fields_as_text");
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Setting index.percolator.map_unmapped_fields_as_text has been renamed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_percolator",
                "The index setting [index.percolator.map_unmapped_fields_as_text] currently set to [" + settingValue +
                    "] been removed in favor of [index.percolator.map_unmapped_fields_as_text].");
        }
        return null;
    }

    static DeprecationIssue classicSimilarityMappingCheck(IndexMetaData indexMetaData) {
        List<String> issues = new ArrayList<>();
        fieldLevelMappingIssue(indexMetaData, ((mappingMetaData, sourceAsMap) -> issues.addAll(
            findInPropertiesRecursively(mappingMetaData.type(), sourceAsMap,
                property -> "classic".equals(property.get("similarity"))))));
        if (issues.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Classic similarity has been removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_the_literal_classic_literal_similarity_has_been_removed",
                "Fields which use classic similarity: " + issues.toString());
        }
        return null;
    }

    static DeprecationIssue classicSimilaritySettingsCheck(IndexMetaData indexMetaData) {
        Map<String, Settings> similarities = indexMetaData.getSettings().getGroups("index.similarity");
        List<String> classicSimilarities = similarities.entrySet().stream()
            .filter(entry -> "classic".equals(entry.getValue().get("type")))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
        if (classicSimilarities.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Classic similarity has been removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_the_literal_classic_literal_similarity_has_been_removed",
                "Custom similarities defined using classic similarity: " + classicSimilarities.toString());
        }
        return null;
    }

	 static DeprecationIssue nodeLeftDelayedTimeCheck(IndexMetaData indexMetaData) {
        String setting = UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey();
        String value = indexMetaData.getSettings().get(setting);
        if (Strings.isNullOrEmpty(value) == false) {
            TimeValue parsedValue = TimeValue.parseTimeValue(value, setting);
            if (parsedValue.getNanos() < 0) {
                return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                    "Negative values for " + setting + " are deprecated and should be set to 0",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                        "#_literal_index_unassigned_node_left_delayed_timeout_literal_may_no_longer_be_negative",
                    "The index [" + indexMetaData.getIndex().getName() + "] has [" + setting + "] set to [" + value +
                        "], but negative values are not allowed");
            }
        }
        return null;
    }

	static DeprecationIssue shardOnStartupCheck(IndexMetaData indexMetaData) {
        String setting = IndexSettings.INDEX_CHECK_ON_STARTUP.getKey();
        String value = indexMetaData.getSettings().get(setting);
        if (Strings.isNullOrEmpty(value) == false) {
            if ("fix".equalsIgnoreCase(value)) {
                return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                    "The value [fix] for setting [" + setting + "] is no longer valid",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                        "#_literal_fix_literal_value_for_literal_index_shard_check_on_startup_literal_is_removed",
                    "The index [" + indexMetaData.getIndex().getName() + "] has the setting [" + setting + "] set to value [fix]" +
                        ", but [fix] is no longer a valid value. Valid values are true, false, and checksum");
            }
        }
        return null;
    }

    static DeprecationIssue tooManyFieldsCheck(IndexMetaData indexMetaData) {
        if (indexMetaData.getSettings().get(IndexSettings.DEFAULT_FIELD_SETTING_KEY) == null) {
            AtomicInteger fieldCount = new AtomicInteger(0);

            fieldLevelMappingIssue(indexMetaData, ((mappingMetaData, sourceAsMap) -> {
                fieldCount.addAndGet(countFieldsRecursively(mappingMetaData.type(), sourceAsMap));
            }));

            // We can't get to the setting `indices.query.bool.max_clause_count` from here, so just check the default of that setting.
            // It's also much better practice to set `index.query.default_field` than `indices.query.bool.max_clause_count` - there's a
            // reason we introduced the limit.
            if (fieldCount.get() > 1024) {
                return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                    "Number of fields exceeds automatic field expansion limit",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                        "#_limiting_the_number_of_auto_expanded_fields",
                    "This index has [" + fieldCount.get() + "] fields, which exceeds the automatic field expansion limit of 1024 " +
                        "and does not have [" + IndexSettings.DEFAULT_FIELD_SETTING_KEY + "] set, which may cause queries which use " +
                        "automatic field expansion, such as query_string, simple_query_string, and multi_match to fail if fields are not " +
                        "explicitly specified in the query.");
            }
        }
        return null;
    }


    private static final Set<String> TYPES_THAT_DONT_COUNT;
    static {
        HashSet<String> typesThatDontCount = new HashSet<>();
        typesThatDontCount.add("binary");
        typesThatDontCount.add("geo_point");
        typesThatDontCount.add("geo_shape");
        TYPES_THAT_DONT_COUNT = Collections.unmodifiableSet(typesThatDontCount);
    }
    /* Counts the number of fields in a mapping, designed to count the as closely as possible to
     * org.elasticsearch.index.search.QueryParserHelper#checkForTooManyFields
     */
    @SuppressWarnings("unchecked")
    static int countFieldsRecursively(String type, Map<String, Object> parentMap) {
        int fields = 0;
        Map<?, ?> properties = (Map<?, ?>) parentMap.get("properties");
        if (properties == null) {
            return fields;
        }
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            Map<String, Object> valueMap = (Map<String, Object>) entry.getValue();
            if (valueMap.containsKey("type")
                && (valueMap.get("type").equals("object") && valueMap.containsKey("properties") == false) == false
                && (TYPES_THAT_DONT_COUNT.contains(valueMap.get("type")) == false)) {
                fields++;
            }

            Map<?, ?> values = (Map<?, ?>) valueMap.get("fields");
            if (values != null) {
                for (Map.Entry<?, ?> multifieldEntry : values.entrySet()) {
                    Map<String, Object> multifieldValueMap = (Map<String, Object>) multifieldEntry.getValue();
                    if (multifieldValueMap.containsKey("type")
                        && (TYPES_THAT_DONT_COUNT.contains(valueMap.get("type")) == false)) {
                        fields++;
                    }
                    if (multifieldValueMap.containsKey("properties")) {
                        fields += countFieldsRecursively(type, multifieldValueMap);
                    }
                }
            }
            if (valueMap.containsKey("properties")) {
                fields += countFieldsRecursively(type, valueMap);
            }
        }

        return fields;
    }
}
