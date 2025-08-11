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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

import static org.elasticsearch.search.SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING;

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
     * Check templates that use `fields` within `fields` blocks
     */
    @SuppressWarnings("unchecked")
    static DeprecationIssue checkTemplatesWithChainedMultiFields(ClusterState state) {
        Map<String, List<String>> templatesContainingChainedMultiFields = new HashMap<>();
        state.getMetadata().getTemplates().forEach((templateCursor) -> {
            String templateName = templateCursor.key;
            templateCursor.value.getMappings().forEach((mappingCursor) -> {
                String type = mappingCursor.key;
                // There should be the type name at this level, but there was a bug where mappings could be stored without a type (#45120)
                // to make sure, we try to detect this like we try to do in MappingMetadata#sourceAsMap()
                Map<String, Object> mapping = XContentHelper.convertToMap(mappingCursor.value.compressedReference(), true).v2();
                if (mapping.size() == 1 && mapping.containsKey(type)) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get(type);
                }
                List<String> mappingIssues = IndexDeprecationChecks.findInPropertiesRecursively(
                    type,
                    mapping,
                    IndexDeprecationChecks::containsChainedMultiFields,
                    IndexDeprecationChecks::formatField,
                    "",
                    ""
                );
                if (mappingIssues.size() > 0) {
                    templatesContainingChainedMultiFields.put(templateName, mappingIssues);
                }
            });
        });
        if (templatesContainingChainedMultiFields.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Defining multi-fields within multi-fields on index template mappings is deprecated",
                "https://ela.st/es-deprecation-7-chained-multi-fields",
                String.format(
                    Locale.ROOT,
                    "Remove chained multi-fields from the \"%s\" template%s. Multi-fields within multi-fields "
                        + "are not supported in 8.0.",
                    String.join(",", templatesContainingChainedMultiFields.keySet()),
                    templatesContainingChainedMultiFields.size() > 1 ? "s" : ""
                ),
                false,
                null
            );
        }
        return null;
    }

    /**
     * Check templates that use `fields` within `fields` blocks of dynamic templates
     */
    @SuppressWarnings("unchecked")
    static DeprecationIssue checkTemplatesWithChainedMultiFieldsInDynamicTemplates(ClusterState state) {
        Map<String, List<String>> templatesContainingChainedMultiFields = new HashMap<>();
        state.getMetadata().getTemplates().forEach((templateCursor) -> {
            String templateName = templateCursor.key;
            templateCursor.value.getMappings().forEach((mappingCursor) -> {
                String type = mappingCursor.key;
                // There should be the type name at this level, but there was a bug where mappings could be stored without a type (#45120)
                // to make sure, we try to detect this like we try to do in MappingMetadata#sourceAsMap()
                Map<String, Object> mapping = XContentHelper.convertToMap(mappingCursor.value.compressedReference(), true).v2();
                if (mapping.size() == 1 && mapping.containsKey(type)) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get(type);
                }
                List<String> mappingIssues = IndexDeprecationChecks.findInDynamicTemplates(
                    type,
                    mapping,
                    IndexDeprecationChecks::containsMappingWithChainedMultiFields,
                    IndexDeprecationChecks::formatField,
                    "",
                    ""
                );
                if (mappingIssues.size() > 0) {
                    templatesContainingChainedMultiFields.put(templateName, mappingIssues);
                }
            });
        });
        if (templatesContainingChainedMultiFields.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Defining multi-fields within multi-fields on index template dynamic_templates is deprecated",
                "https://ela.st/es-deprecation-7-chained-multi-fields",
                String.format(
                    Locale.ROOT,
                    "Remove chained multi-fields from the \"%s\" template%s. Multi-fields within multi-fields "
                        + "are not supported in 8.0.",
                    String.join(",", templatesContainingChainedMultiFields.keySet()),
                    templatesContainingChainedMultiFields.size() > 1 ? "s" : ""
                ),
                false,
                null
            );
        }
        return null;
    }

    /**
     * Check component templates that use `fields` within `fields` blocks
     */
    @SuppressWarnings("unchecked")
    static DeprecationIssue checkComponentTemplatesWithChainedMultiFields(ClusterState state) {
        Map<String, List<String>> templatesContainingChainedMultiFields = new HashMap<>();
        state.getMetadata().componentTemplates().forEach((templateName, componentTemplate) -> {
            CompressedXContent mappings = componentTemplate.template().mappings();
            if (mappings != null) {
                // Component templates root their mapping data under the "_doc" mapping type. Unpack it if that is the case.
                Map<String, Object> mapping = XContentHelper.convertToMap(mappings.compressedReference(), true).v2();
                if (mapping.size() == 1 && mapping.containsKey("_doc")) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get("_doc");
                }
                List<String> mappingIssues = IndexDeprecationChecks.findInPropertiesRecursively(
                    "_doc",
                    mapping,
                    IndexDeprecationChecks::containsChainedMultiFields,
                    IndexDeprecationChecks::formatField,
                    "",
                    ""
                );
                if (mappingIssues.size() > 0) {
                    templatesContainingChainedMultiFields.put(templateName, mappingIssues);
                }
            }
        });
        if (templatesContainingChainedMultiFields.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Defining multi-fields within multi-fields on component templates is deprecated",
                "https://ela.st/es-deprecation-7-chained-multi-fields",
                String.format(
                    Locale.ROOT,
                    "Remove chained multi-fields from the \"%s\" component template%s. Multi-fields within multi-fields "
                        + "are not supported in 8.0.",
                    String.join(",", templatesContainingChainedMultiFields.keySet()),
                    templatesContainingChainedMultiFields.size() > 1 ? "s" : ""
                ),
                false,
                null
            );
        }
        return null;
    }

    /**
     * Check component templates that use `fields` within `fields` blocks of dynamic templates
     */
    @SuppressWarnings("unchecked")
    static DeprecationIssue checkComponentTemplatesWithChainedMultiFieldsInDynamicTemplates(ClusterState state) {
        Map<String, List<String>> templatesContainingChainedMultiFields = new HashMap<>();
        state.getMetadata().componentTemplates().forEach((templateName, componentTemplate) -> {
            CompressedXContent mappings = componentTemplate.template().mappings();
            if (mappings != null) {
                // Component templates root their mapping data under the "_doc" mapping type. Unpack it if that is the case.
                Map<String, Object> mapping = XContentHelper.convertToMap(mappings.compressedReference(), true).v2();
                if (mapping.size() == 1 && mapping.containsKey("_doc")) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get("_doc");
                }
                List<String> mappingIssues = IndexDeprecationChecks.findInDynamicTemplates(
                    "_doc",
                    mapping,
                    IndexDeprecationChecks::containsMappingWithChainedMultiFields,
                    IndexDeprecationChecks::formatField,
                    "",
                    ""
                );
                if (mappingIssues.size() > 0) {
                    templatesContainingChainedMultiFields.put(templateName, mappingIssues);
                }
            }
        });
        if (templatesContainingChainedMultiFields.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Defining multi-fields within multi-fields on component template dynamic_templates is deprecated",
                "https://ela.st/es-deprecation-7-chained-multi-fields",
                String.format(
                    Locale.ROOT,
                    "Remove chained multi-fields from the \"%s\" component template%s. Multi-fields within multi-fields "
                        + "are not supported in 8.0.",
                    String.join(",", templatesContainingChainedMultiFields.keySet()),
                    templatesContainingChainedMultiFields.size() > 1 ? "s" : ""
                ),
                false,
                null
            );
        }
        return null;
    }

    /**
     * Check templates that use fields with `boost` values
     */
    @SuppressWarnings("unchecked")
    static DeprecationIssue checkTemplatesWithBoostedFields(ClusterState state) {
        Map<String, List<String>> templatesContainingBoostedFields = new HashMap<>();
        state.getMetadata().getTemplates().forEach((templateCursor) -> {
            String templateName = templateCursor.key;
            templateCursor.value.getMappings().forEach((mappingCursor) -> {
                String type = mappingCursor.key;
                // There should be the type name at this level, but there was a bug where mappings could be stored without a type (#45120)
                // to make sure, we try to detect this like we try to do in MappingMetadata#sourceAsMap()
                Map<String, Object> mapping = XContentHelper.convertToMap(mappingCursor.value.compressedReference(), true).v2();
                if (mapping.size() == 1 && mapping.containsKey(type)) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get(type);
                }
                List<String> mappingIssues = IndexDeprecationChecks.findInPropertiesRecursively(
                    type,
                    mapping,
                    IndexDeprecationChecks::containsBoostedFields,
                    IndexDeprecationChecks::formatField,
                    "",
                    ""
                );
                if (mappingIssues.size() > 0) {
                    templatesContainingBoostedFields.put(templateName, mappingIssues);
                }
            });
        });
        if (templatesContainingBoostedFields.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Defining boosted fields on index template mappings is deprecated",
                "https://ela.st/es-deprecation-7-boost-fields",
                String.format(
                    Locale.ROOT,
                    "Remove boost fields from the \"%s\" template%s. Configuring a boost value on mapping fields "
                        + "is not supported in 8.0.",
                    String.join(",", templatesContainingBoostedFields.keySet()),
                    templatesContainingBoostedFields.size() > 1 ? "s" : ""
                ),
                false,
                null
            );
        }
        return null;
    }

    /**
     * Check templates that use fields with `boost` values in dynamic templates
     */
    @SuppressWarnings("unchecked")
    static DeprecationIssue checkTemplatesWithBoostFieldsInDynamicTemplates(ClusterState state) {
        Map<String, List<String>> templatesContainingBoostedFields = new HashMap<>();
        state.getMetadata().getTemplates().forEach((templateCursor) -> {
            String templateName = templateCursor.key;
            templateCursor.value.getMappings().forEach((mappingCursor) -> {
                String type = mappingCursor.key;
                // There should be the type name at this level, but there was a bug where mappings could be stored without a type (#45120)
                // to make sure, we try to detect this like we try to do in MappingMetadata#sourceAsMap()
                Map<String, Object> mapping = XContentHelper.convertToMap(mappingCursor.value.compressedReference(), true).v2();
                if (mapping.size() == 1 && mapping.containsKey(type)) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get(type);
                }
                List<String> mappingIssues = IndexDeprecationChecks.findInDynamicTemplates(
                    type,
                    mapping,
                    IndexDeprecationChecks::containsMappingWithBoostedFields,
                    IndexDeprecationChecks::formatField,
                    "",
                    ""
                );
                if (mappingIssues.size() > 0) {
                    templatesContainingBoostedFields.put(templateName, mappingIssues);
                }
            });
        });
        if (templatesContainingBoostedFields.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Defining boosted fields on index template dynamic_templates is deprecated",
                "https://ela.st/es-deprecation-7-boost-fields",
                String.format(
                    Locale.ROOT,
                    "Remove boost fields from the \"%s\" template%s. Configuring a boost value on mapping fields "
                        + "is not supported in 8.0.",
                    String.join(",", templatesContainingBoostedFields.keySet()),
                    templatesContainingBoostedFields.size() > 1 ? "s" : ""
                ),
                false,
                null
            );
        }
        return null;
    }

    /**
     * Check component templates that use fields with `boost` values
     */
    @SuppressWarnings("unchecked")
    static DeprecationIssue checkComponentTemplatesWithBoostedFields(ClusterState state) {
        Map<String, List<String>> templatesContainingBoostedFields = new HashMap<>();
        state.getMetadata().componentTemplates().forEach((templateName, componentTemplate) -> {
            CompressedXContent mappings = componentTemplate.template().mappings();
            if (mappings != null) {
                // Component templates root their mapping data under the "_doc" mapping type. Unpack it if that is the case.
                Map<String, Object> mapping = XContentHelper.convertToMap(mappings.compressedReference(), true).v2();
                if (mapping.size() == 1 && mapping.containsKey("_doc")) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get("_doc");
                }
                List<String> mappingIssues = IndexDeprecationChecks.findInPropertiesRecursively(
                    "_doc",
                    mapping,
                    IndexDeprecationChecks::containsBoostedFields,
                    IndexDeprecationChecks::formatField,
                    "",
                    ""
                );
                if (mappingIssues.size() > 0) {
                    templatesContainingBoostedFields.put(templateName, mappingIssues);
                }
            }
        });
        if (templatesContainingBoostedFields.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Defining boosted fields on component templates is deprecated",
                "https://ela.st/es-deprecation-7-boost-fields",
                String.format(
                    Locale.ROOT,
                    "Remove boost fields from the \"%s\" component template%s. Configuring a boost value on mapping fields "
                        + "is not supported in 8.0.",
                    String.join(",", templatesContainingBoostedFields.keySet()),
                    templatesContainingBoostedFields.size() > 1 ? "s" : ""
                ),
                false,
                null
            );
        }
        return null;
    }

    /**
     * Check component templates that use fields with `boost` values in dynamic templates
     */
    @SuppressWarnings("unchecked")
    static DeprecationIssue checkComponentTemplatesWithBoostedFieldsInDynamicTemplates(ClusterState state) {
        Map<String, List<String>> templatesContainingBoostedFields = new HashMap<>();
        state.getMetadata().componentTemplates().forEach((templateName, componentTemplate) -> {
            CompressedXContent mappings = componentTemplate.template().mappings();
            if (mappings != null) {
                // Component templates root their mapping data under the "_doc" mapping type. Unpack it if that is the case.
                Map<String, Object> mapping = XContentHelper.convertToMap(mappings.compressedReference(), true).v2();
                if (mapping.size() == 1 && mapping.containsKey("_doc")) {
                    // the type name is the root value, reduce it
                    mapping = (Map<String, Object>) mapping.get("_doc");
                }
                List<String> mappingIssues = IndexDeprecationChecks.findInDynamicTemplates(
                    "_doc",
                    mapping,
                    IndexDeprecationChecks::containsMappingWithBoostedFields,
                    IndexDeprecationChecks::formatField,
                    "",
                    ""
                );
                if (mappingIssues.size() > 0) {
                    templatesContainingBoostedFields.put(templateName, mappingIssues);
                }
            }
        });
        if (templatesContainingBoostedFields.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "Defining boosted fields on component template dynamic_templates is deprecated",
                "https://ela.st/es-deprecation-7-boost-fields",
                String.format(
                    Locale.ROOT,
                    "Remove boost fields from the \"%s\" component template%s. Configuring a boost value on mapping fields "
                        + "is not supported in 8.0.",
                    String.join(",", templatesContainingBoostedFields.keySet()),
                    templatesContainingBoostedFields.size() > 1 ? "s" : ""
                ),
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

    static DeprecationIssue checkTemplatesWithCustomAndMultipleTypes(ClusterState state) {
        Set<String> templatesWithMultipleTypes = new TreeSet<>();
        Set<String> templatesWithCustomTypes = new TreeSet<>();
        // See https://github.com/elastic/elasticsearch/issues/82109#issuecomment-1006143687 for details:
        Set<String> systemTemplatesWithCustomTypes = Sets.newHashSet(".triggered_watches", ".watch-history-9", ".watches");
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
                    if (systemTemplatesWithCustomTypes.contains(templateName) == false) {
                        templatesWithCustomTypes.add(templateName);
                    }
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

    /**
     * Upgrading can require the addition of one ore more small indices. This method checks that based on configuration we have the room
     * to add a small number of additional shards to the cluster. The goal is to prevent a failure during upgrade.
     * @param clusterState The cluster state, used to get settings and information about nodes
     * @return A deprecation issue if there is not enough room in this cluster to add a few more shards, or null otherwise
     */
    static DeprecationIssue checkShards(ClusterState clusterState) {
        // Make sure we have room to add a small non-frozen index if needed
        final int shardsInFutureNewSmallIndex = 5;
        final int replicasForFutureIndex = 1;
        if (ShardLimitValidator.canAddShardsToCluster(shardsInFutureNewSmallIndex, replicasForFutureIndex, clusterState, false)) {
            return null;
        } else {
            final int totalShardsToAdd = shardsInFutureNewSmallIndex * (1 + replicasForFutureIndex);
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "The cluster has too many shards to be able to upgrade",
                "https://ela.st/es-deprecation-7-shard-limit",
                String.format(
                    Locale.ROOT,
                    "Upgrading requires adding a small number of new shards. There is not enough room for %d more "
                        + "shards. Increase the cluster.max_shards_per_node setting, or remove indices "
                        + "to clear up resources.",
                    totalShardsToAdd
                ),
                false,
                null
            );
        }
    }

    static DeprecationIssue emptyDataTierPreferenceCheck(ClusterState clusterState) {
        if (DataTier.dataNodesWithoutAllDataRoles(clusterState).isEmpty() == false) {
            List<String> indices = new ArrayList<>();
            for (IndexMetadata indexMetadata : clusterState.metadata().getIndices().values()) {
                List<String> tierPreference = DataTier.parseTierList(DataTier.TIER_PREFERENCE_SETTING.get(indexMetadata.getSettings()));
                if (tierPreference.isEmpty()) {
                    String indexName = indexMetadata.getIndex().getName();
                    indices.add(indexName);
                }
            }

            if (indices.isEmpty() == false) {
                // this is a bit of a hassle, but the String sort order puts .someindex before someindex, and we
                // don't want to give the users a list of only just all .ds-somebackingindex-blah indices -- on the other
                // hand, if that's all that exists, then we don't have much choice. this next little block splits out
                // all the leading-dot indices and sorts them *after* all the non-leading-dot indices
                Map<Boolean, List<String>> groups = indices.stream().collect(Collectors.partitioningBy(s -> s.startsWith(".")));
                List<String> noLeadingPeriod = new ArrayList<>(groups.get(false));
                List<String> leadingPeriod = new ArrayList<>(groups.get(true));
                Collections.sort(noLeadingPeriod);
                Collections.sort(leadingPeriod);
                noLeadingPeriod.addAll(leadingPeriod);
                indices = noLeadingPeriod;

                // if there's more than a few indices, or their names are surprisingly long, then we need to cut off the list.
                // this is not ideal, but our message here is displayed unmodified in the UA, so we have to think about this.
                StringBuilder builder = new StringBuilder();
                Strings.collectionToDelimitedStringWithLimit(indices, ", ", "", "", 256, builder);

                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "No [" + DataTier.TIER_PREFERENCE + "] is set for indices [" + builder + "].",
                    "https://ela.st/es-deprecation-7-empty-tier-preference",
                    "Specify a data tier preference for these indices.",
                    false,
                    null
                );
            }
        }
        return null;
    }

}
