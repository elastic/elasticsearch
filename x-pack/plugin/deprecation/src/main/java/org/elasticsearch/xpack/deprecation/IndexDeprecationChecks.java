/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.LegacyFormatNames;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Index-specific deprecation checks
 */
public class IndexDeprecationChecks {

    static DeprecationIssue oldIndicesCheck(IndexMetadata indexMetadata) {
        // TODO: this check needs to be revised. It's trivially true right now.
        Version currentCompatibilityVersion = indexMetadata.getCompatibilityVersion();
        if (currentCompatibilityVersion.before(Version.V_7_0_0)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Old index with a compatibility version < 7.0",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/" + "breaking-changes-8.0.html",
                "This index has version: " + currentCompatibilityVersion,
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue translogRetentionSettingCheck(IndexMetadata indexMetadata) {
        final boolean softDeletesEnabled = IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexMetadata.getSettings());
        if (softDeletesEnabled) {
            if (IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexMetadata.getSettings())
                || IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexMetadata.getSettings())) {
                List<String> settingKeys = new ArrayList<>();
                if (IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexMetadata.getSettings())) {
                    settingKeys.add(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey());
                }
                if (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexMetadata.getSettings())) {
                    settingKeys.add(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey());
                }
                Map<String, Object> meta = DeprecationIssue.createMetaMapForRemovableSettings(settingKeys);
                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "translog retention settings are ignored",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-translog.html",
                    "translog retention settings [index.translog.retention.size] and [index.translog.retention.age] are ignored "
                        + "because translog is no longer used in peer recoveries with soft-deletes enabled (default in 7.0 or later)",
                    false,
                    meta
                );
            }
        }
        return null;
    }

    static DeprecationIssue checkIndexDataPath(IndexMetadata indexMetadata) {
        if (IndexMetadata.INDEX_DATA_PATH_SETTING.exists(indexMetadata.getSettings())) {
            final String message = String.format(
                Locale.ROOT,
                "setting [%s] is deprecated and will be removed in a future version",
                IndexMetadata.INDEX_DATA_PATH_SETTING.getKey()
            );
            final String url = "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/"
                + "breaking-changes-7.13.html#deprecate-shared-data-path-setting";
            final String details = "Found index data path configured. Discontinue use of this setting.";
            return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue storeTypeSettingCheck(IndexMetadata indexMetadata) {
        final String storeType = IndexModule.INDEX_STORE_TYPE_SETTING.get(indexMetadata.getSettings());
        if (IndexModule.Type.SIMPLEFS.match(storeType)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "[simplefs] is deprecated and will be removed in future versions",
                "https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-store.html",
                "[simplefs] is deprecated and will be removed in 8.0. Use [niofs] or other file systems instead. "
                    + "Elasticsearch 7.15 or later uses [niofs] for the [simplefs] store type "
                    + "as it offers superior or equivalent performance to [simplefs].",
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue frozenIndexSettingCheck(IndexMetadata indexMetadata) {
        Boolean isIndexFrozen = FrozenEngine.INDEX_FROZEN.get(indexMetadata.getSettings());
        if (Boolean.TRUE.equals(isIndexFrozen)) {
            String indexName = indexMetadata.getIndex().getName();
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "index ["
                    + indexName
                    + "] is a frozen index. The frozen indices feature is deprecated and will be removed in a future version",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/frozen-indices.html",
                "Frozen indices no longer offer any advantages. Consider cold or frozen tiers in place of frozen indices.",
                false,
                null
            );
        }
        return null;
    }

    private static void fieldLevelMappingIssue(IndexMetadata indexMetadata, BiConsumer<MappingMetadata, Map<String, Object>> checker) {
        if (indexMetadata.mapping() != null) {
            Map<String, Object> sourceAsMap = indexMetadata.mapping().sourceAsMap();
            checker.accept(indexMetadata.mapping(), sourceAsMap);
        }
    }

    /**
     * iterates through the "properties" field of mappings and returns any predicates that match in the
     * form of issue-strings.
     *
     * @param type the document type
     * @param parentMap the mapping to read properties from
     * @param predicate the predicate to check against for issues, issue is returned if predicate evaluates to true
     * @param fieldFormatter a function that takes a type and mapping field entry and returns a formatted field representation
     * @return a list of issues found in fields
     */
    @SuppressWarnings("unchecked")
    static List<String> findInPropertiesRecursively(
        String type,
        Map<String, Object> parentMap,
        Function<Map<?, ?>, Boolean> predicate,
        BiFunction<String, Map.Entry<?, ?>, String> fieldFormatter,
        String fieldBeginMarker,
        String fieldEndMarker
    ) {
        List<String> issues = new ArrayList<>();
        Map<?, ?> properties = (Map<?, ?>) parentMap.get("properties");
        if (properties == null) {
            return issues;
        }
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            Map<String, Object> valueMap = (Map<String, Object>) entry.getValue();
            if (predicate.apply(valueMap)) {
                issues.add(fieldBeginMarker + fieldFormatter.apply(type, entry) + fieldEndMarker);
            }

            Map<?, ?> values = (Map<?, ?>) valueMap.get("fields");
            if (values != null) {
                for (Map.Entry<?, ?> multifieldEntry : values.entrySet()) {
                    Map<String, Object> multifieldValueMap = (Map<String, Object>) multifieldEntry.getValue();
                    if (predicate.apply(multifieldValueMap)) {
                        issues.add(
                            fieldBeginMarker
                                + fieldFormatter.apply(type, entry)
                                + ", multifield: "
                                + multifieldEntry.getKey()
                                + fieldEndMarker
                        );
                    }
                    if (multifieldValueMap.containsKey("properties")) {
                        issues.addAll(
                            findInPropertiesRecursively(
                                type,
                                multifieldValueMap,
                                predicate,
                                fieldFormatter,
                                fieldBeginMarker,
                                fieldEndMarker
                            )
                        );
                    }
                }
            }
            if (valueMap.containsKey("properties")) {
                issues.addAll(findInPropertiesRecursively(type, valueMap, predicate, fieldFormatter, fieldBeginMarker, fieldEndMarker));
            }
        }

        return issues;
    }

    static DeprecationIssue deprecatedCamelCasePattern(IndexMetadata indexMetadata) {
        List<String> fields = new ArrayList<>();
        fieldLevelMappingIssue(
            indexMetadata,
            ((mappingMetadata, sourceAsMap) -> fields.addAll(
                findInPropertiesRecursively(
                    mappingMetadata.type(),
                    sourceAsMap,
                    IndexDeprecationChecks::isDateFieldWithCamelCasePattern,
                    IndexDeprecationChecks::changeFormatToSnakeCase,
                    "",
                    ""
                )
            ))
        );

        if (fields.size() > 0) {
            String detailsMessageBeginning = fields.stream().collect(Collectors.joining(" "));
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Date fields use deprecated camel case formats",
                "https://ela.st/es-deprecation-7-camel-case-format",
                detailsMessageBeginning,
                false,
                null
            );
        }
        return null;
    }

    private static boolean isDateFieldWithCamelCasePattern(Map<?, ?> property) {
        if ("date".equals(property.get("type")) && property.containsKey("format")) {
            List<String> patterns = DateFormatter.splitCombinedPatterns((String) property.get("format"));
            for (String pattern : patterns) {
                LegacyFormatNames format = LegacyFormatNames.forName(pattern);
                return format != null && format.isCamelCase(pattern);
            }
        }
        return false;
    }

    private static String changeFormatToSnakeCase(String type, Map.Entry<?, ?> entry) {
        Map<?, ?> value = (Map<?, ?>) entry.getValue();
        final String formatFieldValue = (String) value.get("format");
        List<String> patterns = DateFormatter.splitCombinedPatterns(formatFieldValue);
        StringBuilder sb = new StringBuilder(
            "Convert [" + entry.getKey() + "] format [" + formatFieldValue + "] " + "which contains deprecated camel case to snake case. "
        );
        for (String pattern : patterns) {
            LegacyFormatNames format = LegacyFormatNames.forName(pattern);
            if (format != null && format.isCamelCase(pattern)) {
                sb.append("[" + pattern + "] to [" + format.getSnakeCaseName() + "]. ");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
