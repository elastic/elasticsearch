/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.profileschema;

import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.xpack.esql.action.EsqlQueryProfile;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.TimeSpan;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.orc.OrcDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;
import org.elasticsearch.xpack.esql.plugin.EsqlProfileSchemaRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Assembles module-owned profile descriptors into the standalone public schema document.
 */
public final class EsqlProfileSchemaDocument {

    static final String SCHEMA_ID = "https://www.elastic.co/schema/elasticsearch/esql-profile-response.json";

    private EsqlProfileSchemaDocument() {}

    /**
     * Build the complete deterministic draft-7 schema document.
     * @return the schema document
     */
    public static Map<String, Object> create() {
        Map<String, Object> definitions = new LinkedHashMap<>();

        Map<String, Object> esqlProfile = copy(EsqlQueryResponse.PROFILE_SCHEMA.schema());
        Map<String, Object> nullableTimeSpan = ProfileSchema.nullable(ProfileSchema.ref("TimeSpan"));
        for (String property : List.of(
            EsqlQueryProfile.QUERY,
            EsqlQueryProfile.PLANNING,
            EsqlQueryProfile.PARSING,
            EsqlQueryProfile.VIEW_RESOLUTION,
            EsqlQueryProfile.DATASET_RESOLUTION,
            EsqlQueryProfile.PRE_ANALYSIS,
            EsqlQueryProfile.INDICES_RESOLUTION,
            EsqlQueryProfile.ENRICH_RESOLUTION,
            EsqlQueryProfile.INFERENCE_RESOLUTION,
            EsqlQueryProfile.ANALYSIS
        )) {
            properties(esqlProfile).put(property, nullableTimeSpan);
        }
        definitions.put("EsqlProfile", esqlProfile);
        definitions.put("TimeSpan", copy(TimeSpan.PROFILE_SCHEMA.schema()));

        Map<String, Object> driverSleeps = copy(DriverSleeps.PROFILE_SCHEMA.schema());
        properties(driverSleeps).put("first", ProfileSchema.array(ProfileSchema.ref("DriverSleep")));
        properties(driverSleeps).put("last", ProfileSchema.array(ProfileSchema.ref("DriverSleep")));
        definitions.put("DriverSleeps", driverSleeps);
        definitions.put("DriverSleep", copy(DriverSleeps.Sleep.PROFILE_SCHEMA.schema()));
        definitions.put("PlanProfile", copy(PlanProfile.PROFILE_SCHEMA.schema()));

        Map<String, Object> driverProfile = copy(DriverProfile.PROFILE_SCHEMA.schema());
        properties(driverProfile).put("operators", ProfileSchema.array(ProfileSchema.ref("OperatorStatus")));
        properties(driverProfile).put("sleeps", ProfileSchema.ref("DriverSleeps"));
        definitions.put("DriverProfile", driverProfile);

        List<ProfileSchema.Variant<Operator.Status>> operatorVariants = EsqlProfileSchemaRegistry.operatorStatusVariants()
            .stream()
            .sorted(Comparator.comparing(ProfileSchema.Variant::name))
            .toList();
        for (ProfileSchema.Variant<Operator.Status> variant : operatorVariants) {
            Map<String, Object> variantSchema = copy(variant.profileSchema().schema());
            if ("async_external_source".equals(variant.name())) {
                properties(variantSchema).put("format_reader", ProfileSchema.ref("FormatReaderStatus"));
            }
            definitions.put(operatorDefinition(variant.name()), variantSchema);
        }
        definitions.put("OperatorStatus", operatorStatusSchema(operatorVariants));

        List<ProfileSchema.Variant<FormatReaderStatus>> formatVariants = formatVariants().stream()
            .sorted(Comparator.comparing(ProfileSchema.Variant::name))
            .toList();
        for (ProfileSchema.Variant<FormatReaderStatus> variant : formatVariants) {
            definitions.put(formatDefinition(variant.name()), copy(variant.profileSchema().schema()));
        }
        definitions.put("FormatReaderStatus", formatReaderStatusSchema(formatVariants));

        // Extra statuses are inlined by their owning operator, but include their named definitions
        // so registry completeness is visible to downstream schema tooling.
        for (ProfileSchema.Variant<Operator.Status.ExtraStatus> variant : EsqlProfileSchemaRegistry.extraStatusVariants()) {
            Map<String, Object> fragment = copy(variant.profileSchema().schema());
            definitions.put(extraStatusDefinition(variant.name()), strictObject(properties(fragment), required(fragment)));
        }

        Map<String, Object> document = new LinkedHashMap<>();
        document.put("$schema", "http://json-schema.org/draft-07/schema#");
        document.put("$id", SCHEMA_ID);
        document.put("title", "ES|QL profile response");
        document.put("$ref", "#/definitions/EsqlProfile");
        document.put("definitions", definitions);
        return document;
    }

    private static List<ProfileSchema.Variant<FormatReaderStatus>> formatVariants() {
        List<ProfileSchema.Variant<FormatReaderStatus>> variants = new ArrayList<>();
        variants.addAll(new CsvDataSourcePlugin().formatReaderProfileVariants());
        variants.addAll(new NdJsonDataSourcePlugin().formatReaderProfileVariants());
        variants.addAll(new OrcDataSourcePlugin().formatReaderProfileVariants());
        variants.addAll(new ParquetDataSourcePlugin().formatReaderProfileVariants());
        return List.copyOf(variants);
    }

    private static Map<String, Object> operatorStatusSchema(List<ProfileSchema.Variant<Operator.Status>> variants) {
        List<Map<String, Object>> statusAlternatives = new ArrayList<>();
        for (ProfileSchema.Variant<Operator.Status> variant : variants) {
            statusAlternatives.add(ProfileSchema.ref(operatorDefinition(variant.name())));
        }
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("anyOf", statusAlternatives);
        Map<String, Object> withStatusProperties = new LinkedHashMap<>();
        withStatusProperties.put("operator", ProfileSchema.string());
        withStatusProperties.put("status", status);
        return Map.of(
            "anyOf",
            List.of(
                strictObject(Map.of("operator", ProfileSchema.string()), List.of("operator")),
                strictObject(withStatusProperties, List.of("operator", "status"))
            )
        );
    }

    private static Map<String, Object> formatReaderStatusSchema(List<ProfileSchema.Variant<FormatReaderStatus>> variants) {
        List<Map<String, Object>> alternatives = new ArrayList<>();
        alternatives.add(strictObject(Map.of(), List.of()));
        for (ProfileSchema.Variant<FormatReaderStatus> variant : variants) {
            alternatives.add(ProfileSchema.ref(formatDefinition(variant.name())));
        }
        return Map.of("oneOf", alternatives);
    }

    private static Map<String, Object> strictObject(Map<String, Object> properties, List<String> required) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", new LinkedHashMap<>(properties));
        schema.put("required", required);
        schema.put("additionalProperties", false);
        return schema;
    }

    private static String operatorDefinition(String name) {
        return "OperatorStatus_" + safeDefinitionName(name);
    }

    private static String formatDefinition(String name) {
        return "FormatReaderStatus_" + safeDefinitionName(name);
    }

    private static String extraStatusDefinition(String name) {
        return "OperatorExtraStatus_" + safeDefinitionName(name);
    }

    private static String safeDefinitionName(String name) {
        return name.replaceAll("[^A-Za-z0-9_]", "_");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> properties(Map<String, Object> schema) {
        return (Map<String, Object>) schema.get("properties");
    }

    @SuppressWarnings("unchecked")
    private static List<String> required(Map<String, Object> schema) {
        return (List<String>) schema.get("required");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> copy(Map<String, Object> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> map) {
                value = copy((Map<String, Object>) map);
            } else if (value instanceof List<?> list) {
                value = copyList(list);
            }
            copy.put(entry.getKey(), value);
        }
        return copy;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> copyList(List<?> source) {
        List<Object> copy = new ArrayList<>(source.size());
        for (Object value : source) {
            if (value instanceof Map<?, ?> map) {
                value = copy((Map<String, Object>) map);
            } else if (value instanceof List<?> list) {
                value = copyList(list);
            }
            copy.add(value);
        }
        return copy;
    }
}
