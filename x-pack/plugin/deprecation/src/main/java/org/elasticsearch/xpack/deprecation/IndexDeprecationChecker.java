/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.LegacyFormatNames;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.deprecation.DeprecatedIndexPredicate;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_COMMON_DETAIL;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_HELP_URL;

/**
 * Index-specific deprecation checks
 */
public class IndexDeprecationChecker implements ResourceDeprecationChecker {

    public static final String NAME = "index_settings";

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final List<TriFunction<IndexMetadata, ClusterState, Map<String, List<String>>, DeprecationIssue>> checks = List.of(
        this::oldIndicesCheck,
        this::ignoredOldIndicesCheck,
        this::translogRetentionSettingCheck,
        this::checkIndexDataPath,
        this::storeTypeSettingCheck,
        this::deprecatedCamelCasePattern,
        this::legacyRoutingSettingCheck
    );

    public IndexDeprecationChecker(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public Map<String, List<DeprecationIssue>> check(
        ClusterState clusterState,
        DeprecationInfoAction.Request request,
        TransportDeprecationInfoAction.PrecomputedData precomputedData
    ) {
        Map<String, List<DeprecationIssue>> indexSettingsIssues = new HashMap<>();
        String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        Map<String, List<String>> indexToTransformIds = indexToTransformIds(precomputedData.transformConfigs());
        for (String concreteIndex : concreteIndexNames) {
            IndexMetadata indexMetadata = clusterState.getMetadata().getProject().index(concreteIndex);
            List<DeprecationIssue> singleIndexIssues = checks.stream()
                .map(c -> c.apply(indexMetadata, clusterState, indexToTransformIds))
                .filter(Objects::nonNull)
                .toList();
            if (singleIndexIssues.isEmpty() == false) {
                indexSettingsIssues.put(concreteIndex, singleIndexIssues);
            }
        }
        if (indexSettingsIssues.isEmpty()) {
            return Map.of();
        }
        return indexSettingsIssues;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private DeprecationIssue oldIndicesCheck(
        IndexMetadata indexMetadata,
        ClusterState clusterState,
        Map<String, List<String>> indexToTransformIds
    ) {
        // TODO: this check needs to be revised. It's trivially true right now.
        IndexVersion currentCompatibilityVersion = indexMetadata.getCompatibilityVersion();
        // We intentionally exclude indices that are in data streams because they will be picked up by DataStreamDeprecationChecks
        if (DeprecatedIndexPredicate.reindexRequired(indexMetadata, false, false) && isNotDataStreamIndex(indexMetadata, clusterState)) {
            var transforms = transformIdsForIndex(indexMetadata, indexToTransformIds);
            if (transforms.isEmpty() == false) {
                return new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "One or more Transforms write to this index with a compatibility version < " + Version.CURRENT.major + ".0",
                    "https://ela.st/es-deprecation-9-transform-destination-index",
                    Strings.format(
                        "This index was created in version [%s] and requires action before upgrading to %d.0. The following transforms are "
                            + "configured to write to this index: [%s]. Refer to the migration guide to learn more about how to prepare "
                            + "transforms destination indices for your upgrade.",
                        currentCompatibilityVersion.toReleaseVersion(),
                        Version.CURRENT.major,
                        String.join(", ", transforms)
                    ),
                    false,
                    Map.of("reindex_required", true, "transform_ids", transforms)
                );
            } else {
                return new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "Old index with a compatibility version < " + Version.CURRENT.major + ".0",
                    "https://ela.st/es-deprecation-9-index-version",
                    "This index has version: " + currentCompatibilityVersion.toReleaseVersion(),
                    false,
                    Map.of("reindex_required", true)
                );
            }
        }
        return null;
    }

    private List<String> transformIdsForIndex(IndexMetadata indexMetadata, Map<String, List<String>> indexToTransformIds) {
        return indexToTransformIds.getOrDefault(indexMetadata.getIndex().getName(), List.of());
    }

    private DeprecationIssue ignoredOldIndicesCheck(
        IndexMetadata indexMetadata,
        ClusterState clusterState,
        Map<String, List<String>> indexToTransformIds
    ) {
        IndexVersion currentCompatibilityVersion = indexMetadata.getCompatibilityVersion();
        // We intentionally exclude indices that are in data streams because they will be picked up by DataStreamDeprecationChecks
        if (DeprecatedIndexPredicate.reindexRequired(indexMetadata, true, false) && isNotDataStreamIndex(indexMetadata, clusterState)) {
            var transforms = transformIdsForIndex(indexMetadata, indexToTransformIds);
            if (transforms.isEmpty() == false) {
                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "One or more Transforms write to this old index with a compatibility version < " + Version.CURRENT.major + ".0",
                    "https://ela.st/es-deprecation-9-transform-destination-index",
                    Strings.format(
                        "This index was created in version [%s] and will be supported as a read-only index in %d.0. The following "
                            + "transforms are no longer able to write to this index: [%s]. Refer to the migration guide to learn more "
                            + "about how to handle your transforms destination indices.",
                        currentCompatibilityVersion.toReleaseVersion(),
                        Version.CURRENT.major,
                        String.join(", ", transforms)
                    ),
                    false,
                    Map.of("reindex_required", true, "transform_ids", transforms)
                );
            } else {
                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Old index with a compatibility version < " + Version.CURRENT.major + ".0 has been ignored",
                    "https://ela.st/es-deprecation-9-index-version",
                    "This read-only index has version: "
                        + currentCompatibilityVersion.toReleaseVersion()
                        + " and will be supported as read-only in "
                        + Version.CURRENT.major
                        + ".0",
                    false,
                    Map.of("reindex_required", true)
                );
            }
        }
        return null;
    }

    private boolean isNotDataStreamIndex(IndexMetadata indexMetadata, ClusterState clusterState) {
        return clusterState.metadata().getProject().findDataStreams(indexMetadata.getIndex().getName()).isEmpty();
    }

    private DeprecationIssue translogRetentionSettingCheck(
        IndexMetadata indexMetadata,
        ClusterState clusterState,
        Map<String, List<String>> ignored
    ) {
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
                    "https://ela.st/es-deprecation-7-translog-retention",
                    "translog retention settings [index.translog.retention.size] and [index.translog.retention.age] are ignored "
                        + "because translog is no longer used in peer recoveries with soft-deletes enabled (default in 7.0 or later)",
                    false,
                    meta
                );
            }
        }
        return null;
    }

    private DeprecationIssue checkIndexDataPath(IndexMetadata indexMetadata, ClusterState clusterState, Map<String, List<String>> ignored) {
        if (IndexMetadata.INDEX_DATA_PATH_SETTING.exists(indexMetadata.getSettings())) {
            final String message = String.format(
                Locale.ROOT,
                "setting [%s] is deprecated and will be removed in a future version",
                IndexMetadata.INDEX_DATA_PATH_SETTING.getKey()
            );
            final String url = "https://ela.st/es-deprecation-7-index-data-path";
            final String details = "Found index data path configured. Discontinue use of this setting.";
            return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
        }
        return null;
    }

    private DeprecationIssue storeTypeSettingCheck(
        IndexMetadata indexMetadata,
        ClusterState clusterState,
        Map<String, List<String>> ignored
    ) {
        final String storeType = IndexModule.INDEX_STORE_TYPE_SETTING.get(indexMetadata.getSettings());
        if (IndexModule.Type.SIMPLEFS.match(storeType)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "[simplefs] is deprecated and will be removed in future versions",
                "https://ela.st/es-deprecation-7-simplefs",
                "[simplefs] is deprecated and will be removed in 8.0. Use [niofs] or other file systems instead. "
                    + "Elasticsearch 7.15 or later uses [niofs] for the [simplefs] store type "
                    + "as it offers superior or equivalent performance to [simplefs].",
                false,
                null
            );
        }
        return null;
    }

    private DeprecationIssue legacyRoutingSettingCheck(
        IndexMetadata indexMetadata,
        ClusterState clusterState,
        Map<String, List<String>> ignored
    ) {
        List<String> deprecatedSettings = LegacyTiersDetection.getDeprecatedFilteredAllocationSettings(indexMetadata.getSettings());
        if (deprecatedSettings.isEmpty()) {
            return null;
        }
        String indexName = indexMetadata.getIndex().getName();
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "index [" + indexName + "] is configuring tiers via filtered allocation which is not recommended.",
            DEPRECATION_HELP_URL,
            "One or more of your indices is configured with 'index.routing.allocation.*.data' settings. " + DEPRECATION_COMMON_DETAIL,
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(deprecatedSettings)
        );
    }

    private void fieldLevelMappingIssue(IndexMetadata indexMetadata, BiConsumer<MappingMetadata, Map<String, Object>> checker) {
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
    private List<String> findInPropertiesRecursively(
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

    private DeprecationIssue deprecatedCamelCasePattern(
        IndexMetadata indexMetadata,
        ClusterState clusterState,
        Map<String, List<String>> ignored
    ) {
        List<String> fields = new ArrayList<>();
        fieldLevelMappingIssue(
            indexMetadata,
            ((mappingMetadata, sourceAsMap) -> fields.addAll(
                findInPropertiesRecursively(
                    mappingMetadata.type(),
                    sourceAsMap,
                    this::isDateFieldWithCamelCasePattern,
                    this::changeFormatToSnakeCase,
                    "",
                    ""
                )
            ))
        );

        if (fields.size() > 0) {
            String detailsMessageBeginning = String.join(" ", fields);
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

    private boolean isDateFieldWithCamelCasePattern(Map<?, ?> property) {
        if ("date".equals(property.get("type")) && property.containsKey("format")) {
            String[] patterns = DateFormatter.splitCombinedPatterns((String) property.get("format"));
            for (String pattern : patterns) {
                LegacyFormatNames format = LegacyFormatNames.forName(pattern);
                return format != null && format.isCamelCase(pattern);
            }
        }
        return false;
    }

    private String changeFormatToSnakeCase(String type, Map.Entry<?, ?> entry) {
        Map<?, ?> value = (Map<?, ?>) entry.getValue();
        final String formatFieldValue = (String) value.get("format");
        String[] patterns = DateFormatter.splitCombinedPatterns(formatFieldValue);
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

    private Map<String, List<String>> indexToTransformIds(List<TransformConfig> transformConfigs) {
        return transformConfigs.stream()
            .collect(
                Collectors.groupingBy(
                    config -> config.getDestination().getIndex(),
                    Collectors.mapping(TransformConfig::getId, Collectors.toList())
                )
            );
    }
}
