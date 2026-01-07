/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
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
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_COMMON_DETAIL;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_HELP_URL;

/**
 * Index-specific deprecation checks
 */
public class IndexDeprecationChecker implements ResourceDeprecationChecker {

    public static final String NAME = "index_settings";

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final List<TriFunction<IndexMetadata, ProjectMetadata, Map<String, List<String>>, DeprecationIssue>> checks = List.of(
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
        ProjectMetadata project,
        DeprecationInfoAction.Request request,
        TransportDeprecationInfoAction.PrecomputedData precomputedData
    ) {
        Map<String, List<DeprecationIssue>> indexSettingsIssues = new HashMap<>();
        String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(project, request);
        Map<String, List<String>> indexToTransformIds = indexToTransformIds(precomputedData.transformConfigs());
        for (String concreteIndex : concreteIndexNames) {
            IndexMetadata indexMetadata = project.index(concreteIndex);
            List<DeprecationIssue> singleIndexIssues = checks.stream()
                .map(c -> c.apply(indexMetadata, project, indexToTransformIds))
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
        ProjectMetadata project,
        Map<String, List<String>> indexToTransformIds
    ) {
        // We intentionally exclude indices that are in data streams because they will be picked up by DataStreamDeprecationChecks
        if (isNotDataStreamIndex(indexMetadata, project)) {
            // We check for percolator indices first as that will include potentially older indices as well.
            List<String> percolatorIncompatibleFieldMappings = DeprecatedIndexPredicate.reindexRequiredForPecolatorFields(
                indexMetadata,
                false,
                false
            );
            if (percolatorIncompatibleFieldMappings.isEmpty() == false) {
                return new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "Field mappings with incompatible percolator type",
                    "https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/percolator#_reindexing_your_percolator_queries",
                    "The index was created before 9.latest and contains mappings that must be reindexed due to containing percolator "
                        + "fields. "
                        + String.join(", ", percolatorIncompatibleFieldMappings),
                    false,
                    Map.of("reindex_required", true, "excluded_actions", List.of("readOnly"))
                );
            }
            if (DeprecatedIndexPredicate.reindexRequired(indexMetadata, false, false)) {
                IndexVersion currentCompatibilityVersion = indexMetadata.getCompatibilityVersion();
                var transforms = transformIdsForIndex(indexMetadata, indexToTransformIds);
                if (transforms.isEmpty() == false) {
                    return new DeprecationIssue(
                        DeprecationIssue.Level.CRITICAL,
                        "One or more Transforms write to this index with a compatibility version < " + Version.CURRENT.major + ".0",
                        "https://www.elastic.co/docs/deploy-manage/upgrade/prepare-to-upgrade#transform-migration",
                        Strings.format(
                            "This index was created in version [%s] and requires action before upgrading to %d.0. The following "
                                + "transforms are configured to write to this index: [%s]. Refer to the migration guide to learn more "
                                + "about how to prepare transforms destination indices for your upgrade.",
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
        }
        return null;
    }

    private List<String> transformIdsForIndex(IndexMetadata indexMetadata, Map<String, List<String>> indexToTransformIds) {
        return indexToTransformIds.getOrDefault(indexMetadata.getIndex().getName(), List.of());
    }

    private DeprecationIssue ignoredOldIndicesCheck(
        IndexMetadata indexMetadata,
        ProjectMetadata project,
        Map<String, List<String>> indexToTransformIds
    ) {
        // We intentionally exclude indices that are in data streams because they will be picked up by DataStreamDeprecationChecks
        if (isNotDataStreamIndex(indexMetadata, project)) {
            // We check for percolator indices first as that will include potentially older indices as well.
            List<String> percolatorIncompatibleFieldMappings = DeprecatedIndexPredicate.reindexRequiredForPecolatorFields(
                indexMetadata,
                true,
                false
            );
            if (percolatorIncompatibleFieldMappings.isEmpty() == false) {
                return new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "Field mappings with incompatible percolator type",
                    "https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/percolator#_reindexing_your_percolator_queries",
                    "The index was created before 9.latest and contains mappings that must be reindexed due to containing percolator "
                        + "fields. "
                        + String.join(", ", percolatorIncompatibleFieldMappings),
                    false,
                    Map.of("reindex_required", true, "excluded_actions", List.of("readOnly"))
                );
            }
            if (DeprecatedIndexPredicate.reindexRequired(indexMetadata, true, false) && isNotDataStreamIndex(indexMetadata, project)) {
                IndexVersion currentCompatibilityVersion = indexMetadata.getCompatibilityVersion();
                var transforms = transformIdsForIndex(indexMetadata, indexToTransformIds);
                if (transforms.isEmpty() == false) {
                    return new DeprecationIssue(
                        DeprecationIssue.Level.WARNING,
                        "One or more Transforms write to this old index with a compatibility version < " + Version.CURRENT.major + ".0",
                        "https://www.elastic.co/docs/deploy-manage/upgrade/prepare-to-upgrade#transform-migration",
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
        }
        return null;
    }

    private boolean isNotDataStreamIndex(IndexMetadata indexMetadata, ProjectMetadata project) {
        return project.findDataStreams(indexMetadata.getIndex().getName()).isEmpty();
    }

    private DeprecationIssue translogRetentionSettingCheck(
        IndexMetadata indexMetadata,
        ProjectMetadata project,
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

    private DeprecationIssue checkIndexDataPath(IndexMetadata indexMetadata, ProjectMetadata project, Map<String, List<String>> ignored) {
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
        ProjectMetadata project,
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
        ProjectMetadata project,
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

    private DeprecationIssue deprecatedCamelCasePattern(
        IndexMetadata indexMetadata,
        ProjectMetadata project,
        Map<String, List<String>> ignored
    ) {
        List<String> fields = new ArrayList<>();
        fieldLevelMappingIssue(
            indexMetadata,
            ((mappingMetadata, sourceAsMap) -> fields.addAll(
                DeprecatedIndexPredicate.findInPropertiesRecursively(
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
