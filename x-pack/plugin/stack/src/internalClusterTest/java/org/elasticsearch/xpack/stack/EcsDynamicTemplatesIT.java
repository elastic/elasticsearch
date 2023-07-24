/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.stack.StackTemplateRegistry.TEMPLATE_VERSION_VARIABLE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@SuppressWarnings("unchecked")
@ESTestCase.WithoutSecurityManager
@ESIntegTestCase.SuiteScopeTestCase
public class EcsDynamicTemplatesIT extends ESIntegTestCase {

    // The dynamic templates we test against
    public static final String ECS_DYNAMIC_TEMPLATES_FILE = "/ecs-dynamic-mappings.json";

    // The current ECS state (branch main) as a containing all fields in flattened form
    private static final String ECS_FLAT_FILE_URL = "https://raw.githubusercontent.com/elastic/ecs/main/generated/ecs/ecs_flat.yml";

    private static final Set<String> OMIT_FIELD_TYPES = Set.of("object", "flattened", "nested");

    private static final Set<String> OMIT_FIELDS = Set.of("data_stream.dataset", "data_stream.namespace", "data_stream.type");

    private static Map<String, Object> ecsDynamicTemplates;
    private static Map<String, Map<String, Object>> ecsFlatFieldDefinitions;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MapperExtrasPlugin.class, Wildcard.class);
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        prepareEcsDynamicTemplates();
        prepareEcsDefinitions();
    }

    private static void prepareEcsDynamicTemplates() throws JsonProcessingException {
        String rawEcsComponentTemplate = TemplateUtils.loadTemplate(
            ECS_DYNAMIC_TEMPLATES_FILE,
            Integer.toString(1),
            TEMPLATE_VERSION_VARIABLE,
            Collections.emptyMap()
        );
        ObjectMapper jsonObjectMapper = new ObjectMapper();
        Map<?, ?> ecsDynamicTemplatesRaw = jsonObjectMapper.readValue(rawEcsComponentTemplate, Map.class);
        String errorMessage = String.format(
            Locale.ENGLISH,
            "ECS mappings component template '%s' structure has changed, this test needs to be adjusted",
            ECS_DYNAMIC_TEMPLATES_FILE
        );
        assertFalse(errorMessage, rawEcsComponentTemplate.isEmpty());
        Object mappings = ecsDynamicTemplatesRaw.get("template");
        assertNotNull(errorMessage, mappings);
        assertThat(errorMessage, mappings, instanceOf(Map.class));
        Object dynamicTemplates = ((Map<?, ?>) mappings).get("mappings");
        assertNotNull(errorMessage, dynamicTemplates);
        assertThat(errorMessage, dynamicTemplates, instanceOf(Map.class));
        assertTrue(errorMessage, ((Map<?, ?>) dynamicTemplates).containsKey("dynamic_templates"));
        ecsDynamicTemplates = (Map<String, Object>) dynamicTemplates;
    }

    private static void prepareEcsDefinitions() throws IOException {
        ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());
        Map<?, ?> ecsFlatFieldsRawMap = yamlObjectMapper.readValue(new URL(ECS_FLAT_FILE_URL), Map.class);
        String errorMessage = String.format(
            Locale.ENGLISH,
            "ECS flat mapping file at %s has changed, this test needs to be adjusted",
            ECS_FLAT_FILE_URL
        );
        assertFalse(errorMessage, ecsFlatFieldsRawMap.isEmpty());
        Map.Entry<?, ?> fieldEntry = ecsFlatFieldsRawMap.entrySet().iterator().next();
        assertThat(errorMessage, fieldEntry.getKey(), instanceOf(String.class));
        assertThat(errorMessage, fieldEntry.getValue(), instanceOf(Map.class));
        Map<?, ?> fieldProperties = (Map<?, ?>) fieldEntry.getValue();
        assertFalse(errorMessage, fieldProperties.isEmpty());
        Map.Entry<?, ?> fieldProperty = fieldProperties.entrySet().iterator().next();
        assertThat(errorMessage, fieldProperty.getKey(), instanceOf(String.class));

        OMIT_FIELDS.forEach(ecsFlatFieldsRawMap::remove);

        // noinspection CastCanBeRemovedNarrowingVariableType
        ecsFlatFieldDefinitions = (Map<String, Map<String, Object>>) ecsFlatFieldsRawMap;
        Iterator<Map.Entry<String, Map<String, Object>>> iterator = ecsFlatFieldDefinitions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String, Object>> entry = iterator.next();
            Map<String, Object> definitions = entry.getValue();
            String type = (String) definitions.get("type");
            if (OMIT_FIELD_TYPES.contains(type)) {
                iterator.remove();
            } else if ("bool".equals(type)) {
                // for some reason, 'container.security_context.privileged' has a type "bool" in ECS definitions so we are standardizing
                definitions.put("type", "boolean");
            }
        }
    }

    public void testFlattenedFields() {
        ESIntegTestCase.indicesAdmin()
            .prepareCreate("test-flattened-fields")
            .setSettings(Settings.builder().put("index.mapping.total_fields.limit", 10000))
            .setMapping(ecsDynamicTemplates)
            .get();
        ensureGreen();

        Map<String, Object> flattenedFieldsMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> fieldEntry : ecsFlatFieldDefinitions.entrySet()) {
            String fieldName = fieldEntry.getKey();
            Map<String, Object> fieldDefinitions = fieldEntry.getValue();
            String type = (String) fieldDefinitions.get("type");
            assertNotNull(
                String.format(Locale.ENGLISH, "Can't find type for field '%s' in %s file", fieldName, ECS_DYNAMIC_TEMPLATES_FILE),
                type
            );
            Object testValue = generateTestValue(type);
            flattenedFieldsMap.put(fieldName, testValue);
        }
        IndexResponse indexResponse = index("test-flattened-fields", "1", flattenedFieldsMap);
        assertEquals(indexResponse.getResult(), DocWriteResponse.Result.CREATED);

        GetMappingsResponse mappingsResponse = indicesAdmin().prepareGetMappings("test-flattened-fields").get();
        assertThat(mappingsResponse.mappings().size(), equalTo(1));
        Map<String, Object> mappings = mappingsResponse.mappings().entrySet().iterator().next().getValue().getSourceAsMap();
        assertTrue(mappings.containsKey("properties"));
        Map<String, String> flattenedMappings = processMappings((Map<String, Object>) mappings.get("properties"));
        verifyEcsMappings(flattenedMappings);
    }

    private Object generateTestValue(String type) {
        switch (type) {
            case "geo_point" -> {
                return new double[] { randomDouble(), randomDouble() };
            }
            case "long" -> {
                return randomLong();
            }
            case "int" -> {
                return randomInt();
            }
            case "float", "scaled_float" -> {
                return randomFloat();
            }
            case "keyword", "wildcard", "text", "match_only_text" -> {
                return randomAlphaOfLength(20);
            }
            case "constant_keyword" -> {
                return "test";
            }
            case "date" -> {
                return DateFormatter.forPattern("strict_date_optional_time").formatMillis(System.currentTimeMillis());
            }
            case "ip" -> {
                return randomIp(true).getHostAddress();
            }
            case "boolean", "bool" -> {
                return randomBoolean();
            }
        }
        throw new IllegalArgumentException("Unknown field type: " + type);
    }

    private Map<String, String> processMappings(final Map<String, Object> rawMappings) {
        Map<String, String> processedMappings = new HashMap<>();
        processRawMappingsSubtree(rawMappings, processedMappings, "");
        return processedMappings;
    }

    private void processRawMappingsSubtree(
        final Map<String, Object> fieldSubtrees,
        final Map<String, String> processedMappings,
        final String subtreePrefix
    ) {
        fieldSubtrees.forEach((fieldName, fieldMappings) -> {
            String fieldFullPath = subtreePrefix + fieldName;
            Map<String, Object> fieldMappingsMap = ((Map<String, Object>) fieldMappings);
            String type = (String) fieldMappingsMap.get("type");
            if (type != null) {
                processedMappings.put(fieldFullPath, type);
            }
            Map<String, Object> subfields = (Map<String, Object>) fieldMappingsMap.get("properties");
            if (subfields != null) {
                processRawMappingsSubtree(subfields, processedMappings, fieldFullPath + ".");
            }
        });
    }

    private void verifyEcsMappings(Map<String, String> flattenedActualMappings) {
        HashMap<String, Map<String, Object>> shallowCopy = new HashMap<>(ecsFlatFieldDefinitions);
        flattenedActualMappings.forEach((fieldName, fieldType) -> {
            Map<String, Object> actualMappings = shallowCopy.remove(fieldName);
            if (actualMappings == null) {
                // todo - replace with counting
                System.out.println("Field " + fieldName + " doesn't have mappings");
            } else {
                String actualType = (String) actualMappings.get("type");
                if (fieldType.equals(actualType) == false) {
                    // todo - replace with counting
                    System.out.printf("Field %s should have type %s but has type %s\n", fieldName, fieldType, actualType);
                }
            }
        });
        if (shallowCopy.isEmpty() == false) {
            shallowCopy.keySet().forEach(field -> System.out.println("field " + field + " doesn't have ECS definitions"));
        }
        // todo - count all misses and prepare a single report, only then assert for all misses
        // todo - look for the "multi_fields" entry in ecsFlatFieldDefinitions and verify that as well
    }
}
