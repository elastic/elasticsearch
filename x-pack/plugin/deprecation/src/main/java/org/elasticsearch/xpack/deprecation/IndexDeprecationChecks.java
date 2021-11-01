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
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.joda.JodaDeprecationPatterns;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingSlowLog;
import org.elasticsearch.index.SearchSlowLog;
import org.elasticsearch.index.SlowLogLevel;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_REQUIRE_SETTING;

/**
 * Index-specific deprecation checks
 */
public class IndexDeprecationChecks {

    static final String JODA_TIME_DEPRECATION_DETAILS_SUFFIX = " See https://www.elastic.co/guide/en/elasticsearch/reference/master"
        + "/migrate-to-java-time.html for details. Failure to update custom data formats to java.time could cause inconsistentencies in "
        + "your data. Failure to properly convert x (week-year) to Y could result in data loss.";

    private static void fieldLevelMappingIssue(IndexMetadata indexMetadata, BiConsumer<MappingMetadata, Map<String, Object>> checker) {
        for (MappingMetadata mappingMetadata : indexMetadata.getMappings().values()) {
            Map<String, Object> sourceAsMap = mappingMetadata.sourceAsMap();
            checker.accept(mappingMetadata, sourceAsMap);
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

    private static String formatDateField(String type, Map.Entry<?, ?> entry) {
        Map<?, ?> value = (Map<?, ?>) entry.getValue();
        return String.format(Locale.ROOT, "Convert [%s] format %s to java.time.", entry.getKey(), value.get("format"));
    }

    private static String formatField(String type, Map.Entry<?, ?> entry) {
        return entry.getKey().toString();
    }

    static DeprecationIssue oldIndicesCheck(IndexMetadata indexMetadata) {
        Version createdWith = indexMetadata.getCreationVersion();
        if (createdWith.before(Version.V_7_0_0)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Index created before 7.0",
                "https://ela.st/es-deprecation-7-reindex",
                "This index was created with version "
                    + createdWith
                    + " and is not compatible with 8.0. Reindex or remove the index "
                    + "before upgrading.",
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue tooManyFieldsCheck(IndexMetadata indexMetadata) {
        if (indexMetadata.getSettings().get(IndexSettings.DEFAULT_FIELD_SETTING.getKey()) == null) {
            AtomicInteger fieldCount = new AtomicInteger(0);

            fieldLevelMappingIssue(
                indexMetadata,
                ((mappingMetadata, sourceAsMap) -> { fieldCount.addAndGet(countFieldsRecursively(mappingMetadata.type(), sourceAsMap)); })
            );

            // We can't get to the setting `indices.query.bool.max_clause_count` from here, so just check the default of that setting.
            // It's also much better practice to set `index.query.default_field` than `indices.query.bool.max_clause_count` - there's a
            // reason we introduced the limit.
            if (fieldCount.get() > 1024) {
                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Number of fields exceeds automatic field expansion limit",
                    "https://ela.st/es-deprecation-7-number-of-auto-expanded-fields",
                    "This index has "
                        + fieldCount.get()
                        + " fields, which exceeds the automatic field expansion limit (1024). Set "
                        + IndexSettings.DEFAULT_FIELD_SETTING.getKey()
                        + " to prevent queries that support automatic field expansion from "
                        + "failing if no fields are specified. Otherwise, you must explicitly specify fields in all query_string, "
                        + "simple_query_string, and multi_match queries.",
                    false,
                    null
                );
            }
        }
        return null;
    }

    static DeprecationIssue deprecatedDateTimeFormat(IndexMetadata indexMetadata) {
        Version createdWith = indexMetadata.getCreationVersion();
        if (createdWith.before(Version.V_7_0_0)) {
            List<String> fields = new ArrayList<>();

            fieldLevelMappingIssue(
                indexMetadata,
                ((mappingMetadata, sourceAsMap) -> fields.addAll(
                    findInPropertiesRecursively(
                        mappingMetadata.type(),
                        sourceAsMap,
                        IndexDeprecationChecks::isDateFieldWithDeprecatedPattern,
                        IndexDeprecationChecks::formatDateField,
                        "",
                        ""
                    )
                ))
            );

            if (fields.size() > 0) {
                String detailsMessageBeginning = fields.stream().collect(Collectors.joining(" "));
                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Date fields use deprecated Joda time formats",
                    "https://ela.st/es-deprecation-7-java-time",
                    detailsMessageBeginning + JODA_TIME_DEPRECATION_DETAILS_SUFFIX,
                    false,
                    null
                );
            }
        }
        return null;
    }

    private static boolean isDateFieldWithDeprecatedPattern(Map<?, ?> property) {
        return "date".equals(property.get("type"))
            && property.containsKey("format")
            && JodaDeprecationPatterns.isDeprecatedPattern((String) property.get("format"));
    }

    static DeprecationIssue chainedMultiFieldsCheck(IndexMetadata indexMetadata) {
        List<String> issues = new ArrayList<>();
        fieldLevelMappingIssue(
            indexMetadata,
            ((mappingMetadata, sourceAsMap) -> issues.addAll(
                findInPropertiesRecursively(
                    mappingMetadata.type(),
                    sourceAsMap,
                    IndexDeprecationChecks::containsChainedMultiFields,
                    IndexDeprecationChecks::formatField,
                    "",
                    ""
                )
            ))
        );
        if (issues.size() > 0) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Defining multi-fields within multi-fields is deprecated",
                "https://ela.st/es-deprecation-7-chained-multi-fields",
                String.format(
                    Locale.ROOT,
                    "Remove chained multi-fields from the \"%s\" mapping%s. Multi-fields within multi-fields "
                        + "are not supported in 8.0.",
                    issues.stream().collect(Collectors.joining(",")),
                    issues.size() > 1 ? "s" : ""
                ),
                false,
                null
            );
        }
        return null;
    }

    private static boolean containsChainedMultiFields(Map<?, ?> property) {
        if (property.containsKey("fields")) {
            Map<?, ?> fields = (Map<?, ?>) property.get("fields");
            for (Object rawSubField : fields.values()) {
                Map<?, ?> subField = (Map<?, ?>) rawSubField;
                if (subField.containsKey("fields")) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * warn about existing explicit "_field_names" settings in existing mappings
     */
    static DeprecationIssue fieldNamesDisabledCheck(IndexMetadata indexMetadata) {
        MappingMetadata mapping = indexMetadata.mapping();
        if ((mapping != null) && ClusterDeprecationChecks.mapContainsFieldNamesDisabled(mapping.getSourceAsMap())) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Disabling the \"_field_names\" field in the index mappings is deprecated",
                "https://ela.st/es-deprecation-7-field_names-settings",
                "Remove the \"field_names\" mapping that configures the enabled setting. There's no longer a need to disable this "
                    + "field to reduce index overhead if you have a lot of fields.",
                false,
                null
            );
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
                    if (multifieldValueMap.containsKey("type") && (TYPES_THAT_DONT_COUNT.contains(valueMap.get("type")) == false)) {
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

    static DeprecationIssue translogRetentionSettingCheck(IndexMetadata indexMetadata) {
        final boolean softDeletesEnabled = IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexMetadata.getSettings());
        if (softDeletesEnabled) {
            if (IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexMetadata.getSettings())
                || IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexMetadata.getSettings())) {
                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Translog retention settings are deprecated",
                    "https://ela.st/es-deprecation-7-translog-settings",
                    "Remove the translog retention settings: \"index.translog.retention.size\" and \"index.translog.retention.age\". The "
                        + "translog has not been used in peer recoveries with soft-deletes enabled since 7.0 and these settings have no "
                        + "effect.",
                    false,
                    null
                );
            }
        }
        return null;
    }

    static DeprecationIssue checkIndexDataPath(IndexMetadata indexMetadata) {
        if (IndexMetadata.INDEX_DATA_PATH_SETTING.exists(indexMetadata.getSettings())) {
            final String message = String.format(
                Locale.ROOT,
                "Setting [index.data_path] is deprecated",
                IndexMetadata.INDEX_DATA_PATH_SETTING.getKey()
            );
            final String url = "https://ela.st/es-deprecation-7-shared-path-settings";
            final String details = String.format(
                Locale.ROOT,
                "Remove the [%s] setting. This setting has had no effect since 6.0.",
                IndexMetadata.INDEX_DATA_PATH_SETTING.getKey()
            );
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue indexingSlowLogLevelSettingCheck(IndexMetadata indexMetadata) {
        return slowLogSettingCheck(indexMetadata, IndexingSlowLog.INDEX_INDEXING_SLOWLOG_LEVEL_SETTING);
    }

    static DeprecationIssue searchSlowLogLevelSettingCheck(IndexMetadata indexMetadata) {
        return slowLogSettingCheck(indexMetadata, SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL);
    }

    private static DeprecationIssue slowLogSettingCheck(IndexMetadata indexMetadata, Setting<SlowLogLevel> setting) {
        if (setting.exists(indexMetadata.getSettings())) {
            final String message = String.format(Locale.ROOT, "Setting [%s] is deprecated", setting.getKey());
            final String url = "https://ela.st/es-deprecation-7-slowlog-settings";

            final String details = String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use the [index.*.slowlog.threshold] settings to " + "set the log levels.",
                setting.getKey()
            );
            return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue storeTypeSettingCheck(IndexMetadata indexMetadata) {
        final String storeType = IndexModule.INDEX_STORE_TYPE_SETTING.get(indexMetadata.getSettings());
        if (IndexModule.Type.SIMPLEFS.match(storeType)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Setting [index.store.type] to [simplefs] is deprecated",
                "https://ela.st/es-deprecation-7-simplefs-store-type",
                "Use [niofs] (the default) or one of the other FS types. This is an expert-only setting that might be removed in the "
                    + "future.",
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkRemovedSetting(
        final Settings settings,
        final Setting<?> removedSetting,
        final String url,
        final String additionalDetail,
        DeprecationIssue.Level deprecationLevel
    ) {
        return checkRemovedSetting(settings, removedSetting, url, "Setting [%s] is deprecated", additionalDetail, deprecationLevel);
    }

    static DeprecationIssue checkRemovedSetting(
        final Settings settings,
        final Setting<?> removedSetting,
        final String url,
        final String messagePattern,
        final String additionalDetail,
        DeprecationIssue.Level deprecationLevel
    ) {
        if (removedSetting.exists(settings) == false) {
            return null;
        }
        final String removedSettingKey = removedSetting.getKey();
        final String value = removedSetting.get(settings).toString();
        final String message = String.format(Locale.ROOT, messagePattern, removedSettingKey);
        final String details = String.format(Locale.ROOT, "Remove the [%s] setting. %s", removedSettingKey, additionalDetail);
        return new DeprecationIssue(deprecationLevel, message, url, details, false, null);
    }

    static DeprecationIssue checkIndexRoutingRequireSetting(IndexMetadata indexMetadata) {
        return checkRemovedSetting(
            indexMetadata.getSettings(),
            INDEX_ROUTING_REQUIRE_SETTING,
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            "Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkIndexRoutingIncludeSetting(IndexMetadata indexMetadata) {
        return checkRemovedSetting(
            indexMetadata.getSettings(),
            INDEX_ROUTING_INCLUDE_SETTING,
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            "Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkIndexRoutingExcludeSetting(IndexMetadata indexMetadata) {
        return checkRemovedSetting(
            indexMetadata.getSettings(),
            INDEX_ROUTING_EXCLUDE_SETTING,
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            "Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
            DeprecationIssue.Level.CRITICAL
        );
    }

    static DeprecationIssue checkIndexMatrixFiltersSetting(IndexMetadata indexMetadata) {
        return checkRemovedSetting(
            indexMetadata.getSettings(),
            IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING,
            "https://ela.st/es-deprecation-7-adjacency-matrix-filters-setting",
            String.format(Locale.ROOT, "Setting [%s] is deprecated", IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey()),
            String.format(
                Locale.ROOT,
                "Set [%s] to [%s]. [%s] will be ignored in 8.0.",
                SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING.getKey(),
                IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.get(indexMetadata.getSettings()),
                IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey()
            ),
            DeprecationIssue.Level.WARNING
        );
    }

    protected static boolean isGeoShapeFieldWithDeprecatedParam(Map<?, ?> property) {
        return GeoShapeFieldMapper.CONTENT_TYPE.equals(property.get("type"))
            && GeoShapeFieldMapper.DEPRECATED_PARAMETERS.stream()
                .anyMatch(deprecatedParameter -> property.containsKey(deprecatedParameter));
    }

    protected static String formatDeprecatedGeoShapeParamMessage(String type, Map.Entry<?, ?> entry) {
        String fieldName = entry.getKey().toString();
        Map<?, ?> value = (Map<?, ?>) entry.getValue();
        return GeoShapeFieldMapper.DEPRECATED_PARAMETERS.stream()
            .filter(deprecatedParameter -> value.containsKey(deprecatedParameter))
            .map(deprecatedParameter -> String.format(Locale.ROOT, "parameter [%s] in field [%s]", deprecatedParameter, fieldName))
            .collect(Collectors.joining("; "));
    }

    @SuppressWarnings("unchecked")
    static DeprecationIssue checkGeoShapeMappings(IndexMetadata indexMetadata) {
        if (indexMetadata == null || indexMetadata.mapping() == null) {
            return null;
        }
        Map<String, Object> sourceAsMap = indexMetadata.mapping().getSourceAsMap();
        List<String> messages = findInPropertiesRecursively(
            GeoShapeFieldMapper.CONTENT_TYPE,
            sourceAsMap,
            IndexDeprecationChecks::isGeoShapeFieldWithDeprecatedParam,
            IndexDeprecationChecks::formatDeprecatedGeoShapeParamMessage,
            "[",
            "]"
        );
        if (messages.isEmpty()) {
            return null;
        } else {
            String message = String.format(
                Locale.ROOT,
                "[%s] index uses deprecated geo_shape properties",
                indexMetadata.getIndex().getName()
            );
            String details = String.format(
                Locale.ROOT,
                "The following geo_shape parameters must be removed from %s: [%s]",
                indexMetadata.getIndex().getName(),
                messages.stream().collect(Collectors.joining("; "))
            );
            String url = "https://ela.st/es-deprecation-7-geo-shape-mappings";
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
    }

    static DeprecationIssue frozenIndexSettingCheck(IndexMetadata indexMetadata) {
        Boolean isIndexFrozen = FrozenEngine.INDEX_FROZEN.get(indexMetadata.getSettings());
        if (Boolean.TRUE.equals(isIndexFrozen)) {
            String indexName = indexMetadata.getIndex().getName();
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Freezing indices is deprecated",
                "https://ela.st/es-deprecation-7-frozen-indices",
                "Index ["
                    + indexName
                    + "] is frozen. Frozen indices no longer offer any advantages. Instead, unfreeze the index, make it"
                    + " read-only, and move it to the cold or frozen tier.",
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue emptyDataTierPreferenceCheck(ClusterState clusterState, IndexMetadata indexMetadata) {
        if (DataTier.dataNodesWithoutAllDataRoles(clusterState).isEmpty() == false) {
            final List<String> tierPreference = DataTier.parseTierList(DataTier.TIER_PREFERENCE_SETTING.get(indexMetadata.getSettings()));
            if (tierPreference.isEmpty()) {
                String indexName = indexMetadata.getIndex().getName();
                return new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "index ["
                        + indexName
                        + "] does not have a ["
                        + DataTier.TIER_PREFERENCE
                        + "] setting, "
                        + "in 8.0 this setting will be required for all indices and may not be empty or null.",
                    "https://ela.st/es-deprecation-7-empty-tier-preference",
                    "Update the settings for this index to specify an appropriate tier preference.",
                    false,
                    null
                );
            }
        }
        return null;
    }
}
