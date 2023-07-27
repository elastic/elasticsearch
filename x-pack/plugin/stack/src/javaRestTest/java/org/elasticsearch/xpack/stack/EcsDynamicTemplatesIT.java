/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@SuppressWarnings("unchecked")
public class EcsDynamicTemplatesIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("mapper-extras")
        .module("wildcard")
        .build();

    // The dynamic templates we test against
    public static final String ECS_DYNAMIC_TEMPLATES_FILE = "/ecs-dynamic-mappings.json";

    // The current ECS state (branch main) as a containing all fields in flattened form
    private static final String ECS_FLAT_FILE_URL = "https://raw.githubusercontent.com/elastic/ecs/main/generated/ecs/ecs_flat.yml";

    private static final Set<String> OMIT_FIELD_TYPES = Set.of("object", "flattened", "nested");

    private static final Set<String> OMIT_FIELDS = Set.of("data_stream.dataset", "data_stream.namespace", "data_stream.type");

    private static Map<String, Object> ecsDynamicTemplates;
    private static Map<String, Map<String, Object>> ecsFlatFieldDefinitions;

    @BeforeClass
    public static void setupSuiteScopeCluster() throws Exception {
        prepareEcsDynamicTemplates();
        prepareEcsDefinitions();
    }

    private static void prepareEcsDynamicTemplates() throws IOException {
        String rawEcsComponentTemplate = TemplateUtils.loadTemplate(
            ECS_DYNAMIC_TEMPLATES_FILE,
            Integer.toString(1),
            StackTemplateRegistry.TEMPLATE_VERSION_VARIABLE,
            Collections.emptyMap()
        );
        Map<String, Object> ecsDynamicTemplatesRaw;
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, rawEcsComponentTemplate)
        ) {
            ecsDynamicTemplatesRaw = parser.map();
        }

        String errorMessage = String.format(
            Locale.ENGLISH,
            "ECS mappings component template '%s' structure has changed, this test needs to be adjusted",
            ECS_DYNAMIC_TEMPLATES_FILE
        );
        Assert.assertFalse(errorMessage, rawEcsComponentTemplate.isEmpty());
        Object mappings = ecsDynamicTemplatesRaw.get("template");
        Assert.assertNotNull(errorMessage, mappings);
        Assert.assertThat(errorMessage, mappings, instanceOf(Map.class));
        Object dynamicTemplates = ((Map<?, ?>) mappings).get("mappings");
        Assert.assertNotNull(errorMessage, dynamicTemplates);
        Assert.assertThat(errorMessage, dynamicTemplates, instanceOf(Map.class));
        Assert.assertEquals(errorMessage, 1, ((Map<?, ?>) dynamicTemplates).size());
        Assert.assertTrue(errorMessage, ((Map<?, ?>) dynamicTemplates).containsKey("dynamic_templates"));
        ecsDynamicTemplates = (Map<String, Object>) dynamicTemplates;
    }

    @SuppressForbidden(reason = "Opening socket connection to read ECS definitions from ECS GitHub repo")
    private static void prepareEcsDefinitions() throws IOException {
        Map<String, ?> ecsFlatFieldsRawMap;
        URL ecsDefinitionsFlatFileUrl = new URL(ECS_FLAT_FILE_URL);
        try (InputStream ecsDynamicTemplatesIS = ecsDefinitionsFlatFileUrl.openStream()) {
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.YAML)
                    .createParser(XContentParserConfiguration.EMPTY, ecsDynamicTemplatesIS)
            ) {
                ecsFlatFieldsRawMap = parser.map();
            }
        }
        String errorMessage = String.format(
            Locale.ENGLISH,
            "ECS flat mapping file at %s has changed, this test needs to be adjusted",
            ECS_FLAT_FILE_URL
        );
        Assert.assertFalse(errorMessage, ecsFlatFieldsRawMap.isEmpty());
        Map.Entry<String, ?> fieldEntry = ecsFlatFieldsRawMap.entrySet().iterator().next();
        Assert.assertThat(errorMessage, fieldEntry.getValue(), instanceOf(Map.class));
        Map<?, ?> fieldProperties = (Map<?, ?>) fieldEntry.getValue();
        Assert.assertFalse(errorMessage, fieldProperties.isEmpty());
        Map.Entry<?, ?> fieldProperty = fieldProperties.entrySet().iterator().next();
        Assert.assertThat(errorMessage, fieldProperty.getKey(), instanceOf(String.class));

        OMIT_FIELDS.forEach(ecsFlatFieldsRawMap::remove);

        // noinspection
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

    public void testFlattenedFields() throws IOException {
        String indexName = "test-flattened-fields";
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
            bodyBuilder.startObject();
            bodyBuilder.startObject("settings");
            bodyBuilder.field("index.mapping.total_fields.limit", 10000);
            bodyBuilder.endObject();
            bodyBuilder.field("mappings", ecsDynamicTemplates);
            bodyBuilder.endObject();

            Request createIndexRequest = new Request("PUT", "/" + indexName);
            createIndexRequest.setJsonEntity(Strings.toString(bodyBuilder));
            // noinspection resource
            Response response = ESRestTestCase.client().performRequest(createIndexRequest);
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        }
        // ensureGreen(indexName);

        Map<String, Object> flattenedFieldsMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> fieldEntry : ecsFlatFieldDefinitions.entrySet()) {
            String fieldName = fieldEntry.getKey();
            Map<String, Object> fieldDefinitions = fieldEntry.getValue();
            String type = (String) fieldDefinitions.get("type");
            Assert.assertNotNull(
                String.format(Locale.ENGLISH, "Can't find type for field '%s' in %s file", fieldName, ECS_DYNAMIC_TEMPLATES_FILE),
                type
            );
            Object testValue = generateTestValue(type);
            flattenedFieldsMap.put(fieldName, testValue);
        }

        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
            Request indexRequest = new Request("POST", "/" + indexName + "/_doc");
            indexRequest.setJsonEntity(Strings.toString(bodyBuilder.map(flattenedFieldsMap)));
            // noinspection resource
            Response response = ESRestTestCase.client().performRequest(indexRequest);
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusLine().getStatusCode());
        }

        Request getMappingRequest = new Request("GET", "/" + indexName + "/_mapping");
        // noinspection resource
        Response response = ESRestTestCase.client().performRequest(getMappingRequest);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
        Map<String, Object> mappingResponse;
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, EntityUtils.toByteArray(response.getEntity()))
        ) {
            mappingResponse = parser.map();
        }
        Assert.assertThat(mappingResponse.size(), equalTo(1));
        Map<String, Object> indexMap = (Map<String, Object>) mappingResponse.get(indexName);
        assertNotNull(indexMap);
        Map<String, Object> mappings = (Map<String, Object>) indexMap.get("mappings");
        assertNotNull(mappings);
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
                return NetworkAddress.format(randomIp(true));
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
                logger.error("Field " + fieldName + " doesn't have mappings");
            } else {
                String actualType = (String) actualMappings.get("type");
                if (fieldType.equals(actualType) == false) {
                    // todo - replace with counting
                    logger.error("Field {} should have type {} but has type {}", fieldName, fieldType, actualType);
                }
            }
        });
        if (shallowCopy.isEmpty() == false) {
            shallowCopy.keySet().forEach(field -> logger.error("field " + field + " doesn't have ECS definitions"));
        }
        // todo - count all misses and prepare a single report, only then assert for all misses
        // todo - look for the "multi_fields" entry in ecsFlatFieldDefinitions and verify that as well
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
