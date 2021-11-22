/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING;
import static org.elasticsearch.search.SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING;
import static org.elasticsearch.xpack.deprecation.NodeDeprecationChecks.checkRemovedSetting;

public class ClusterDeprecationChecks {
    private static final Logger logger = LogManager.getLogger(ClusterDeprecationChecks.class);

    private static final String SPARSE_VECTOR = "sparse_vector";

    @SuppressWarnings("unchecked")
    static DeprecationIssue checkUserAgentPipelines(ClusterState state) {
        List<PipelineConfiguration> pipelines = IngestService.getPipelines(state);

        List<String> pipelinesWithDeprecatedEcsConfig = pipelines.stream().filter(Objects::nonNull).filter(pipeline -> {
            Map<String, Object> pipelineConfig = pipeline.getConfigAsMap();

            List<Map<String, Map<String, Object>>> processors = (List<Map<String, Map<String, Object>>>) pipelineConfig.get("processors");
            return processors.stream()
                .filter(Objects::nonNull)
                .filter(processor -> processor.containsKey("user_agent"))
                .map(processor -> processor.get("user_agent"))
                .anyMatch(processorConfig -> processorConfig.containsKey("ecs"));
        })
            .map(PipelineConfiguration::getId)
            .sorted() // Make the warning consistent for testing purposes
            .collect(Collectors.toList());
        if (pipelinesWithDeprecatedEcsConfig.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "The User-Agent ingest processor's ecs parameter is deprecated",
                "https://ela.st/es-deprecation-7-ingest-pipeline-ecs-option",
                "Remove the ecs parameter from your ingest pipelines. The User-Agent ingest processor always returns Elastic Common "
                    + "Schema (ECS) fields in 8.0.",
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkTemplatesWithTooManyFields(ClusterState state) {
        Integer maxClauseCount = INDICES_MAX_CLAUSE_COUNT_SETTING.get(state.getMetadata().settings());
        List<String> templatesOverLimit = new ArrayList<>();
        state.getMetadata().getTemplates().forEach((templateCursor) -> {
            AtomicInteger maxFields = new AtomicInteger(0);
            String templateName = templateCursor.key;
            boolean defaultFieldSet = templateCursor.value.settings().get(IndexSettings.DEFAULT_FIELD_SETTING.getKey()) != null;
            templateCursor.value.getMappings().forEach((mappingCursor) -> {
                MappingMetadata mappingMetadata = new MappingMetadata(mappingCursor.value);
                if (mappingMetadata != null && defaultFieldSet == false) {
                    maxFields.set(IndexDeprecationChecks.countFieldsRecursively(mappingMetadata.type(), mappingMetadata.sourceAsMap()));
                }
                if (maxFields.get() > maxClauseCount) {
                    templatesOverLimit.add(templateName);
                }
            });
        });

        if (templatesOverLimit.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Fields in index template exceed automatic field expansion limit",
                "https://ela.st/es-deprecation-7-number-of-auto-expanded-fields",
                "Index templates "
                    + templatesOverLimit
                    + " have a number of fields which exceeds the automatic field expansion "
                    + "limit of ["
                    + maxClauseCount
                    + "] and does not have ["
                    + IndexSettings.DEFAULT_FIELD_SETTING.getKey()
                    + "] set, "
                    + "which may cause queries which use automatic field expansion, such as query_string, simple_query_string, and "
                    + "multi_match to fail if fields are not explicitly specified in the query.",
                false,
                null
            );
        }
        return null;
    }

    /**
     * Check templates that use `_field_names` explicitly, which was deprecated in https://github.com/elastic/elasticsearch/pull/42854
     * and will throw an error on new indices in 8.0
     */
    @SuppressWarnings("unchecked")
    static DeprecationIssue checkTemplatesWithFieldNamesDisabled(ClusterState state) {
        Set<String> templatesContainingFieldNames = new HashSet<>();
        state.getMetadata().getTemplates().forEach((templateCursor) -> {
            String templateName = templateCursor.key;
            templateCursor.value.getMappings().forEach((mappingCursor) -> {
                String type = mappingCursor.key;
                // there should be the type name at this level, but there was a bug where mappings could be stored without a type (#45120)
                // to make sure, we try to detect this like we try to do in MappingMetadata#sourceAsMap()
                Map<String, Object> mapping = XContentHelper.convertToMap(mappingCursor.value.compressedReference(), true).v2();
                if (mapping.size() == 1 && mapping.containsKey(type)) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get(type);
                }
                if (mapContainsFieldNamesDisabled(mapping)) {
                    templatesContainingFieldNames.add(templateName);
                }
            });
        });

        if (templatesContainingFieldNames.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Disabling the \"_field_names\" field in a template's index " + "mappings is deprecated",
                "https://ela.st/es-deprecation-7-field_names-settings",
                String.format(
                    Locale.ROOT,
                    "Remove the \"%s\" mapping that configures the enabled setting from the following templates: \"%s\". "
                        + "There's no longer a need to disable this field to reduce index overhead if you have a lot of fields.",
                    FieldNamesFieldMapper.NAME,
                    templatesContainingFieldNames.stream().collect(Collectors.joining(","))
                ),
                false,
                null
            );
        }
        return null;
    }

    /**
     * check for "_field_names" entries in the map that contain another property "enabled" in the sub-map
     */
    static boolean mapContainsFieldNamesDisabled(Map<?, ?> map) {
        Object fieldNamesMapping = map.get(FieldNamesFieldMapper.NAME);
        if (fieldNamesMapping != null) {
            if (((Map<?, ?>) fieldNamesMapping).keySet().contains("enabled")) {
                return true;
            }
        }
        return false;
    }

    static DeprecationIssue checkPollIntervalTooLow(ClusterState state) {
        String pollIntervalString = state.metadata().settings().get(LIFECYCLE_POLL_INTERVAL_SETTING.getKey());
        if (Strings.isNullOrEmpty(pollIntervalString)) {
            return null;
        }

        TimeValue pollInterval;
        try {
            pollInterval = TimeValue.parseTimeValue(pollIntervalString, LIFECYCLE_POLL_INTERVAL_SETTING.getKey());
        } catch (IllegalArgumentException e) {
            logger.error("Failed to parse [{}] value: [{}]", LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), pollIntervalString);
            return null;
        }

        if (pollInterval.compareTo(TimeValue.timeValueSeconds(1)) < 0) {
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Index Lifecycle Management poll interval is set too low",
                "https://ela.st/es-deprecation-7-indices-lifecycle-poll-interval-setting",
                String.format(
                    Locale.ROOT,
                    "The ILM [%s] setting is set to [%s]. Set the interval to at least 1s.",
                    LIFECYCLE_POLL_INTERVAL_SETTING.getKey(),
                    pollIntervalString
                ),
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue checkTemplatesWithCustomAndMultipleTypes(ClusterState state) {
        Set<String> templatesWithMultipleTypes = new TreeSet<>();
        Set<String> templatesWithCustomTypes = new TreeSet<>();
        state.getMetadata().getTemplates().forEach((templateCursor) -> {
            String templateName = templateCursor.key;
            ImmutableOpenMap<String, CompressedXContent> mappings = templateCursor.value.mappings();
            if (mappings != null) {
                if (mappings.size() > 1) {
                    templatesWithMultipleTypes.add(templateName);
                }
                boolean hasCustomType = mappings.stream().anyMatch(mapping -> {
                    String typeName = mapping.getKey();
                    return MapperService.SINGLE_MAPPING_NAME.equals(typeName) == false;
                });
                if (hasCustomType) {
                    templatesWithCustomTypes.add(templateName);
                }
            }
        });
        final DeprecationIssue deprecationIssue;
        if (templatesWithMultipleTypes.isEmpty() && templatesWithCustomTypes.isEmpty()) {
            deprecationIssue = null;
        } else if (templatesWithMultipleTypes.isEmpty()) {
            deprecationIssue = new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Custom mapping types in index templates are deprecated",
                "https://ela.st/es-deprecation-7-custom-types",
                "Update or remove the following index templates before upgrading to 8.0: "
                    + templatesWithCustomTypes
                    + ". See https://ela.st/es-deprecation-7-removal-of-types for alternatives to mapping types.",
                false,
                null
            );
        } else {
            // There were multiple mapping types, so at least one of them had to be a custom type as well
            Set<String> allBadTemplates = new TreeSet<>();
            allBadTemplates.addAll(templatesWithMultipleTypes);
            allBadTemplates.addAll(templatesWithCustomTypes);
            deprecationIssue = new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Multiple mapping types and custom mapping types in index templates and indices are deprecated",
                "https://ela.st/es-deprecation-7-multiple-types",
                "Update or remove the following index templates before upgrading to 8.0: "
                    + allBadTemplates
                    + ". See https://ela.st/es-deprecation-7-removal-of-types for alternatives to mapping types.",
                false,
                null
            );
        }
        return deprecationIssue;
    }

    static DeprecationIssue checkClusterRoutingAllocationIncludeRelocationsSetting(final ClusterState clusterState) {
        return checkRemovedSetting(
            clusterState.metadata().settings(),
            CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING,
            "https://ela.st/es-deprecation-7-cluster-routing-allocation-disk-include-relocations-setting",
            "Relocating shards are always taken into account in 8.0.",
            DeprecationIssue.Level.WARNING
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<String>> getComponentTemplatesWithDeprecatedGeoShapeProperties(
        Map<String, ComponentTemplate> componentTemplates
    ) {
        Map<String, List<String>> detailsForComponentTemplates = componentTemplates.entrySet().stream().map((templateCursor) -> {
            String templateName = templateCursor.getKey();
            ComponentTemplate componentTemplate = templateCursor.getValue();
            CompressedXContent mappings = componentTemplate.template().mappings();
            if (mappings != null) {
                Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(
                    mappings.uncompressed(),
                    true,
                    XContentType.JSON
                );
                Map<String, Object> mappingAsMap = tuple.v2();
                List<String> messages = mappingAsMap == null
                    ? Collections.emptyList()
                    : IndexDeprecationChecks.findInPropertiesRecursively(
                        GeoShapeFieldMapper.CONTENT_TYPE,
                        mappingAsMap,
                        IndexDeprecationChecks::isGeoShapeFieldWithDeprecatedParam,
                        IndexDeprecationChecks::formatDeprecatedGeoShapeParamMessage,
                        "[",
                        "]"
                    );
                return Tuple.tuple(templateName, messages);
            }
            return null;
        })
            .filter(templateToMessagesTuple -> templateToMessagesTuple != null && templateToMessagesTuple.v2().isEmpty() == false)
            .collect(Collectors.toMap(Tuple<String, List<String>>::v1, Tuple<String, List<String>>::v2));
        return detailsForComponentTemplates;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<String>> getIndexTemplatesWithDeprecatedGeoShapeProperties(
        ImmutableOpenMap<String, IndexTemplateMetadata> indexTemplates
    ) {
        Map<String, List<String>> detailsForIndexTemplates = StreamSupport.stream(indexTemplates.spliterator(), false)
            .map((templateCursor) -> {
                String templateName = templateCursor.key;
                IndexTemplateMetadata indexTemplateMetadata = templateCursor.value;
                List<String> messagesForTemplate = StreamSupport.stream(indexTemplateMetadata.getMappings().spliterator(), false)
                    .map((mappingCursor) -> {
                        CompressedXContent mapping = mappingCursor.value;
                        Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(
                            mapping.uncompressed(),
                            true,
                            XContentType.JSON
                        );
                        Map<String, Object> mappingAsMap = (Map<String, Object>) tuple.v2().get("_doc");
                        List<String> messages = mappingAsMap == null
                            ? Collections.emptyList()
                            : IndexDeprecationChecks.findInPropertiesRecursively(
                                GeoShapeFieldMapper.CONTENT_TYPE,
                                mappingAsMap,
                                IndexDeprecationChecks::isGeoShapeFieldWithDeprecatedParam,
                                IndexDeprecationChecks::formatDeprecatedGeoShapeParamMessage,
                                "[",
                                "]"
                            );
                        return messages;
                    })
                    .filter(messages -> messages.isEmpty() == false)
                    .flatMap(x -> x.stream())
                    .collect(Collectors.toList());
                return Tuple.tuple(templateName, messagesForTemplate);
            })
            .filter(templateToMessagesTuple -> templateToMessagesTuple != null && templateToMessagesTuple.v2().isEmpty() == false)
            .collect(Collectors.toMap(Tuple<String, List<String>>::v1, Tuple<String, List<String>>::v2));
        return detailsForIndexTemplates;
    }

    private static String getDetailsMessageForTemplatesWithDeprecations(
        Map<String, List<String>> templateToMessages,
        boolean forceIncludeTemplateName
    ) {
        final boolean includeTemplateName = forceIncludeTemplateName || templateToMessages.keySet().size() > 1;
        return templateToMessages.entrySet().stream().filter(entry -> entry.getValue().isEmpty() == false).map(entry -> {
            StringBuilder message = new StringBuilder();
            if (includeTemplateName) {
                message.append("[");
                message.append(entry.getKey());
                message.append(": ");
            }
            message.append(entry.getValue().stream().collect(Collectors.joining("; ")));
            if (includeTemplateName) {
                message.append("]");
            }
            return message;
        }).collect(Collectors.joining("; "));
    }

    @SuppressWarnings("unchecked")
    static DeprecationIssue checkGeoShapeTemplates(final ClusterState clusterState) {
        Map<String, List<String>> componentTemplatesToMessagesMap = getComponentTemplatesWithDeprecatedGeoShapeProperties(
            clusterState.getMetadata().componentTemplates()
        );
        Map<String, List<String>> indexTemplatesToMessagesMap = getIndexTemplatesWithDeprecatedGeoShapeProperties(
            clusterState.getMetadata().getTemplates()
        );
        boolean deprecationInComponentTemplates = componentTemplatesToMessagesMap.isEmpty() == false;
        boolean deprecationInIndexTemplates = indexTemplatesToMessagesMap.isEmpty() == false;
        String url = "https://ela.st/es-deprecation-7-geo-shape-mappings";
        if (deprecationInComponentTemplates && deprecationInIndexTemplates) {
            String message = String.format(
                Locale.ROOT,
                "[%s] component template%s and [%s] index template%s use deprecated geo_shape " + "properties",
                componentTemplatesToMessagesMap.keySet().stream().collect(Collectors.joining(",")),
                componentTemplatesToMessagesMap.keySet().size() > 1 ? "s" : "",
                indexTemplatesToMessagesMap.keySet().stream().collect(Collectors.joining(",")),
                indexTemplatesToMessagesMap.keySet().size() > 1 ? "s" : ""
            );
            String details = String.format(
                Locale.ROOT,
                "Remove the following deprecated geo_shape properties from the mappings: %s; %s.",
                getDetailsMessageForTemplatesWithDeprecations(componentTemplatesToMessagesMap, true),
                getDetailsMessageForTemplatesWithDeprecations(indexTemplatesToMessagesMap, true)
            );
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
        if (deprecationInComponentTemplates == false && deprecationInIndexTemplates) {
            String message = String.format(
                Locale.ROOT,
                "[%s] index template%s use%s deprecated geo_shape properties",
                indexTemplatesToMessagesMap.keySet().stream().collect(Collectors.joining(",")),
                indexTemplatesToMessagesMap.keySet().size() > 1 ? "s" : "",
                indexTemplatesToMessagesMap.keySet().size() > 1 ? "" : "s"
            );
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                message,
                url,
                String.format(
                    Locale.ROOT,
                    "Remove the following deprecated geo_shape properties from the mappings: %s.",
                    getDetailsMessageForTemplatesWithDeprecations(indexTemplatesToMessagesMap, false)
                ),
                false,
                null
            );
        } else if (deprecationInIndexTemplates == false && deprecationInComponentTemplates) {
            String message = String.format(
                Locale.ROOT,
                "[%s] component template%s use%s deprecated geo_shape properties",
                componentTemplatesToMessagesMap.keySet().stream().collect(Collectors.joining(",")),
                componentTemplatesToMessagesMap.keySet().size() > 1 ? "s" : "",
                componentTemplatesToMessagesMap.keySet().size() > 1 ? "" : "s"
            );
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                message,
                url,
                String.format(
                    Locale.ROOT,
                    "Remove the following deprecated geo_shape properties from the mappings: %s.",
                    getDetailsMessageForTemplatesWithDeprecations(componentTemplatesToMessagesMap, false)
                ),
                false,
                null
            );
        } else {
            return null;
        }
    }

    protected static boolean isSparseVector(Map<?, ?> property) {
        return SPARSE_VECTOR.equals(property.get("type"));
    }

    protected static String formatDeprecatedSparseVectorMessage(String type, Map.Entry<?, ?> entry) {
        return entry.getKey().toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<String>> getComponentTemplatesWithDeprecatedSparseVectorProperties(
        Map<String, ComponentTemplate> componentTemplates
    ) {
        Map<String, List<String>> detailsForComponentTemplates = componentTemplates.entrySet().stream().map((templateCursor) -> {
            String templateName = templateCursor.getKey();
            ComponentTemplate componentTemplate = templateCursor.getValue();
            CompressedXContent mappings = componentTemplate.template().mappings();
            if (mappings != null) {
                Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(
                    mappings.uncompressed(),
                    true,
                    XContentType.JSON
                );
                Map<String, Object> mappingAsMap = tuple.v2();
                List<String> messages = mappingAsMap == null
                    ? Collections.emptyList()
                    : IndexDeprecationChecks.findInPropertiesRecursively(
                        SPARSE_VECTOR,
                        mappingAsMap,
                        ClusterDeprecationChecks::isSparseVector,
                        ClusterDeprecationChecks::formatDeprecatedSparseVectorMessage,
                        "[",
                        "]"
                    );
                if (messages.isEmpty() == false) {
                    return Tuple.tuple(templateName, messages);
                }
            }
            return null;
        })
            .filter(templateToMessagesTuple -> templateToMessagesTuple != null && templateToMessagesTuple.v2().isEmpty() == false)
            .collect(Collectors.toMap(Tuple<String, List<String>>::v1, Tuple<String, List<String>>::v2));
        return detailsForComponentTemplates;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<String>> getIndexTemplatesWithDeprecatedSparseVectorProperties(
        ImmutableOpenMap<String, IndexTemplateMetadata> indexTemplates
    ) {
        Map<String, List<String>> detailsForIndexTemplates = StreamSupport.stream(indexTemplates.spliterator(), false)
            .map((templateCursor) -> {
                String templateName = templateCursor.key;
                IndexTemplateMetadata indexTemplateMetadata = templateCursor.value;
                List<String> messagesForTemplate = StreamSupport.stream(indexTemplateMetadata.getMappings().spliterator(), false)
                    .map((mappingCursor) -> {
                        CompressedXContent mapping = mappingCursor.value;
                        Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(
                            mapping.uncompressed(),
                            true,
                            XContentType.JSON
                        );
                        Map<String, Object> mappingAsMap = (Map<String, Object>) tuple.v2().get("_doc");
                        List<String> messages = mappingAsMap == null
                            ? Collections.emptyList()
                            : IndexDeprecationChecks.findInPropertiesRecursively(
                                SPARSE_VECTOR,
                                mappingAsMap,
                                ClusterDeprecationChecks::isSparseVector,
                                ClusterDeprecationChecks::formatDeprecatedSparseVectorMessage,
                                "[",
                                "]"
                            );
                        return messages;
                    })
                    .filter(messages -> messages.isEmpty() == false)
                    .flatMap(x -> x.stream())
                    .collect(Collectors.toList());
                return Tuple.tuple(templateName, messagesForTemplate);
            })
            .filter(templateToMessagesTuple -> templateToMessagesTuple != null && templateToMessagesTuple.v2().isEmpty() == false)
            .collect(Collectors.toMap(Tuple<String, List<String>>::v1, Tuple<String, List<String>>::v2));
        return detailsForIndexTemplates;
    }

    @SuppressWarnings("unchecked")
    static DeprecationIssue checkSparseVectorTemplates(final ClusterState clusterState) {
        Map<String, List<String>> componentTemplatesToMessagesMap = getComponentTemplatesWithDeprecatedSparseVectorProperties(
            clusterState.getMetadata().componentTemplates()
        );
        Map<String, List<String>> indexTemplatesToMessagesMap = getIndexTemplatesWithDeprecatedSparseVectorProperties(
            clusterState.getMetadata().getTemplates()
        );
        boolean deprecationInComponentTemplates = componentTemplatesToMessagesMap.isEmpty() == false;
        boolean deprecationInIndexTemplates = indexTemplatesToMessagesMap.isEmpty() == false;
        String url = "https://ela.st/es-deprecation-7-sparse-vector";
        if (deprecationInComponentTemplates && deprecationInIndexTemplates) {
            String message = String.format(
                Locale.ROOT,
                "[%s] component template%s and [%s] index template%s use deprecated sparse_vector " + "properties",
                componentTemplatesToMessagesMap.keySet().stream().collect(Collectors.joining(",")),
                componentTemplatesToMessagesMap.keySet().size() > 1 ? "s" : "",
                indexTemplatesToMessagesMap.keySet().stream().collect(Collectors.joining(",")),
                indexTemplatesToMessagesMap.keySet().size() > 1 ? "s" : ""
            );
            String details = String.format(
                Locale.ROOT,
                "Remove the following deprecated sparse_vector properties from the mappings: %s; %s.",
                getDetailsMessageForTemplatesWithDeprecations(componentTemplatesToMessagesMap, true),
                getDetailsMessageForTemplatesWithDeprecations(indexTemplatesToMessagesMap, true)
            );
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL, message, url, details, false, null);
        }
        if (deprecationInComponentTemplates == false && deprecationInIndexTemplates) {
            String message = String.format(
                Locale.ROOT,
                "[%s] index template%s use%s deprecated sparse_vector properties",
                indexTemplatesToMessagesMap.keySet().stream().collect(Collectors.joining(",")),
                indexTemplatesToMessagesMap.keySet().size() > 1 ? "s" : "",
                indexTemplatesToMessagesMap.keySet().size() > 1 ? "" : "s"
            );
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                message,
                url,
                String.format(
                    Locale.ROOT,
                    "Remove the following deprecated sparse_vector properties from the mappings: %s.",
                    getDetailsMessageForTemplatesWithDeprecations(indexTemplatesToMessagesMap, false)
                ),
                false,
                null
            );
        } else if (deprecationInIndexTemplates == false && deprecationInComponentTemplates) {
            String message = String.format(
                Locale.ROOT,
                "[%s] component template%s use%s deprecated sparse_vector properties",
                componentTemplatesToMessagesMap.keySet().stream().collect(Collectors.joining(",")),
                componentTemplatesToMessagesMap.keySet().size() > 1 ? "s" : "",
                componentTemplatesToMessagesMap.keySet().size() > 1 ? "" : "s"
            );
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                message,
                url,
                String.format(
                    Locale.ROOT,
                    "Remove the following deprecated sparse_vector properties from the mappings: %s.",
                    getDetailsMessageForTemplatesWithDeprecations(componentTemplatesToMessagesMap, false)
                ),
                false,
                null
            );
        } else {
            return null;
        }
    }

    static DeprecationIssue checkILMFreezeActions(ClusterState state) {
        IndexLifecycleMetadata indexLifecycleMetadata = state.getMetadata().custom("index_lifecycle");
        if (indexLifecycleMetadata != null) {
            List<String> policiesWithFreezeActions = indexLifecycleMetadata.getPolicies()
                .entrySet()
                .stream()
                .filter(
                    nameAndPolicy -> nameAndPolicy.getValue()
                        .getPhases()
                        .values()
                        .stream()
                        .anyMatch(phase -> phase != null && phase.getActions() != null && phase.getActions().containsKey(FreezeAction.NAME))
                )
                .map(nameAndPolicy -> nameAndPolicy.getKey())
                .collect(Collectors.toList());
            if (policiesWithFreezeActions.isEmpty() == false) {
                String details = String.format(
                    Locale.ROOT,
                    "Remove the freeze action from ILM policies: [%s]",
                    policiesWithFreezeActions.stream().sorted().collect(Collectors.joining(","))
                );
                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "ILM policies use the deprecated freeze action",
                    "https://ela.st/es-deprecation-7-frozen-indices",
                    details,
                    false,
                    null
                );
            }
        }
        return null;
    }

    static DeprecationIssue checkTransientSettingsExistence(ClusterState state) {
        if (state.metadata().transientSettings().isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Transient cluster settings are deprecated",
                "https://ela.st/es-deprecation-7-transient-cluster-settings",
                "Use persistent settings to configure your cluster.",
                false,
                null
            );
        }
        return null;
    }
}
