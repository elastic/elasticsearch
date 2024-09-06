/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.apache.http.HttpStatus;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
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
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@SuppressWarnings("unchecked")
public class EcsDynamicTemplatesIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().module("mapper-extras").module("wildcard").build();

    // The dynamic templates we test against
    public static final String ECS_DYNAMIC_TEMPLATES_FILE = "ecs@mappings.json";

    // The current ECS state (branch main) containing all fields in flattened form
    private static final String ECS_FLAT_FILE_URL = "https://raw.githubusercontent.com/elastic/ecs/main/generated/ecs/ecs_flat.yml";

    private static final Set<String> OMIT_FIELD_TYPES = Set.of("object", "nested");

    private static final Set<String> OMIT_FIELDS = Set.of("data_stream.dataset", "data_stream.namespace", "data_stream.type");

    private static Map<String, Object> ecsDynamicTemplates;
    private static Map<String, Map<String, Object>> ecsFlatFieldDefinitions;
    private static Map<String, Map<String, Object>> ecsFlatMultiFieldDefinitions;

    @BeforeClass
    public static void setupSuiteScopeCluster() throws Exception {
        prepareEcsDynamicTemplates();
        prepareEcsDefinitions();
    }

    private static void prepareEcsDynamicTemplates() throws IOException {
        String rawEcsComponentTemplate = TemplateUtils.loadTemplate(
            "/" + ECS_DYNAMIC_TEMPLATES_FILE,
            Integer.toString(1),
            StackTemplateRegistry.TEMPLATE_VERSION_VARIABLE,
            StackTemplateRegistry.ADDITIONAL_TEMPLATE_VARIABLES
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
        assertFalse(errorMessage, rawEcsComponentTemplate.isEmpty());
        Object mappings = ecsDynamicTemplatesRaw.get("template");
        assertNotNull(errorMessage, mappings);
        assertThat(errorMessage, mappings, instanceOf(Map.class));
        Object dynamicTemplates = ((Map<?, ?>) mappings).get("mappings");
        assertNotNull(errorMessage, dynamicTemplates);
        assertThat(errorMessage, dynamicTemplates, instanceOf(Map.class));
        assertEquals(errorMessage, 1, ((Map<?, ?>) dynamicTemplates).size());
        assertTrue(errorMessage, ((Map<?, ?>) dynamicTemplates).containsKey("dynamic_templates"));
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
        assertFalse(errorMessage, ecsFlatFieldsRawMap.isEmpty());
        Map.Entry<String, ?> fieldEntry = ecsFlatFieldsRawMap.entrySet().iterator().next();
        assertThat(errorMessage, fieldEntry.getValue(), instanceOf(Map.class));
        Map<?, ?> fieldProperties = (Map<?, ?>) fieldEntry.getValue();
        assertFalse(errorMessage, fieldProperties.isEmpty());
        Map.Entry<?, ?> fieldProperty = fieldProperties.entrySet().iterator().next();
        assertThat(errorMessage, fieldProperty.getKey(), instanceOf(String.class));

        OMIT_FIELDS.forEach(ecsFlatFieldsRawMap::remove);

        // noinspection
        ecsFlatFieldDefinitions = (Map<String, Map<String, Object>>) ecsFlatFieldsRawMap;
        ecsFlatMultiFieldDefinitions = new HashMap<>();
        Iterator<Map.Entry<String, Map<String, Object>>> iterator = ecsFlatFieldDefinitions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String, Object>> entry = iterator.next();
            Map<String, Object> definitions = entry.getValue();
            String type = (String) definitions.get("type");
            if (OMIT_FIELD_TYPES.contains(type)) {
                iterator.remove();
            }

            List<Map<String, Object>> multiFields = (List<Map<String, Object>>) definitions.get("multi_fields");
            if (multiFields != null) {
                multiFields.forEach(multiFieldsDefinitions -> {
                    String subfieldFlatName = (String) Objects.requireNonNull(multiFieldsDefinitions.get("flat_name"));
                    ecsFlatMultiFieldDefinitions.put(subfieldFlatName, multiFieldsDefinitions);
                });
            }
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testFlattenedFields() throws IOException {
        String indexName = "test-flattened-fields";
        createTestIndex(indexName);
        Map<String, Object> flattenedFieldsMap = createTestDocument(true);
        indexDocument(indexName, flattenedFieldsMap);
        verifyEcsMappings(indexName);
    }

    public void testFlattenedFieldsWithinAttributes() throws IOException {
        String indexName = "test-flattened-attributes";
        createTestIndex(indexName);
        Map<String, Object> flattenedFieldsMap = createTestDocument(true);
        indexDocument(indexName, Map.of("attributes", flattenedFieldsMap));
        verifyEcsMappings(indexName, "attributes.");
    }

    public void testFlattenedFieldsWithinResourceAttributes() throws IOException {
        String indexName = "test-flattened-attributes";
        createTestIndex(indexName);
        Map<String, Object> flattenedFieldsMap = createTestDocument(true);
        indexDocument(indexName, Map.of("resource.attributes", flattenedFieldsMap));
        verifyEcsMappings(indexName, "resource.attributes.");
    }

    public void testFlattenedFieldsWithoutSubobjects() throws IOException {
        String indexName = "test_flattened_fields_subobjects_false";
        createTestIndex(indexName, Map.of("subobjects", false));
        Map<String, Object> flattenedFieldsMap = createTestDocument(true);
        indexDocument(indexName, flattenedFieldsMap);
        verifyEcsMappings(indexName);
    }

    public void testNestedFields() throws IOException {
        String indexName = "test-nested-fields";
        createTestIndex(indexName);
        Map<String, Object> nestedFieldsMap = createTestDocument(false);
        indexDocument(indexName, nestedFieldsMap);
        verifyEcsMappings(indexName);
    }

    public void testNumericMessage() throws IOException {
        String indexName = "test-numeric-message";
        createTestIndex(indexName);
        Map<String, Object> fieldsMap = createTestDocument(false);
        fieldsMap.put("message", 123); // Should be mapped as match_only_text
        indexDocument(indexName, fieldsMap);
        verifyEcsMappings(indexName);
    }

    private void assertType(String expectedType, Map<String, Object> actualMappings) {
        assertNotNull("expected to get non-null mappings for field", actualMappings);
        assertEquals(expectedType, actualMappings.get("type"));
    }

    public void testUsage() throws IOException {
        String indexName = "test-usage";
        createTestIndex(indexName);
        Map<String, Object> fieldsMap = createTestDocument(false);
        // Only non-root numeric (or coercable to numeric) "usage" fields should match
        // ecs_usage_*_scaled_float; root fields and intermediate object fields should not match.
        fieldsMap.put("host.cpu.usage", 123); // should be mapped as scaled_float
        fieldsMap.put("string.usage", "123"); // should also be mapped as scale_float
        fieldsMap.put("usage", 123);
        fieldsMap.put("root.usage.long", 123);
        fieldsMap.put("root.usage.float", 123.456);
        indexDocument(indexName, fieldsMap);

        final Map<String, Object> rawMappings = getMappings(indexName);
        final Map<String, Map<String, Object>> flatFieldMappings = new HashMap<>();
        processRawMappingsSubtree(rawMappings, flatFieldMappings, new HashMap<>(), "");
        assertType("scaled_float", flatFieldMappings.get("host.cpu.usage"));
        assertType("scaled_float", flatFieldMappings.get("string.usage"));
        assertType("long", flatFieldMappings.get("usage"));
        assertType("long", flatFieldMappings.get("root.usage.long"));
        assertType("float", flatFieldMappings.get("root.usage.float"));
    }

    public void testOnlyMatchLeafFields() throws IOException {
        // tests that some of the match conditions only apply to leaf fields, not intermediate objects
        String indexName = "test";
        createTestIndex(indexName);
        Map<String, Object> fieldsMap = createTestDocument(false);
        fieldsMap.put("foo.message.bar", 123);
        fieldsMap.put("foo.url.path.bar", 123);
        fieldsMap.put("foo.url.full.bar", 123);
        fieldsMap.put("foo.stack_trace.bar", 123);
        fieldsMap.put("foo.user_agent.original.bar", 123);
        fieldsMap.put("foo.created.bar", 123);
        fieldsMap.put("foo._score.bar", 123);
        fieldsMap.put("foo.structured_data", 123);
        indexDocument(indexName, fieldsMap);

        final Map<String, Object> rawMappings = getMappings(indexName);
        final Map<String, Map<String, Object>> flatFieldMappings = new HashMap<>();
        processRawMappingsSubtree(rawMappings, flatFieldMappings, new HashMap<>(), "");
        assertType("long", flatFieldMappings.get("foo.message.bar"));
        assertType("long", flatFieldMappings.get("foo.url.path.bar"));
        assertType("long", flatFieldMappings.get("foo.url.full.bar"));
        assertType("long", flatFieldMappings.get("foo.stack_trace.bar"));
        assertType("long", flatFieldMappings.get("foo.user_agent.original.bar"));
        assertType("long", flatFieldMappings.get("foo.created.bar"));
        assertType("float", flatFieldMappings.get("foo._score.bar"));
        assertType("long", flatFieldMappings.get("foo.structured_data"));
    }

    private static void indexDocument(String indexName, Map<String, Object> flattenedFieldsMap) throws IOException {
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
            Request indexRequest = new Request("POST", "/" + indexName + "/_doc");
            indexRequest.setJsonEntity(Strings.toString(bodyBuilder.map(flattenedFieldsMap)));
            // noinspection resource
            Response response = ESRestTestCase.client().performRequest(indexRequest);
            assertEquals(HttpStatus.SC_CREATED, response.getStatusLine().getStatusCode());
        }
    }

    private Map<String, Object> createTestDocument(boolean flattened) {
        Map<String, Object> testFieldsMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> fieldEntry : ecsFlatFieldDefinitions.entrySet()) {
            String flattenedFieldName = fieldEntry.getKey();
            Map<String, Object> fieldDefinitions = fieldEntry.getValue();
            String type = (String) fieldDefinitions.get("type");
            assertNotNull(
                String.format(Locale.ENGLISH, "Can't find type for field '%s' in %s file", flattenedFieldName, ECS_DYNAMIC_TEMPLATES_FILE),
                type
            );
            Object testValue = generateTestValue(type);
            if (flattened) {
                testFieldsMap.put(flattenedFieldName, testValue);
            } else {
                Map<String, Object> currentField = testFieldsMap;
                Iterator<String> fieldPathPartsIterator = Arrays.stream(flattenedFieldName.split("\\.")).iterator();
                String subfield = fieldPathPartsIterator.next();
                while (fieldPathPartsIterator.hasNext()) {
                    currentField = (Map<String, Object>) currentField.computeIfAbsent(subfield, ignore -> new HashMap<>());
                    subfield = fieldPathPartsIterator.next();
                }
                currentField.put(subfield, testValue);
            }
        }
        return testFieldsMap;
    }

    private static void createTestIndex(String indexName) throws IOException {
        createTestIndex(indexName, null);
    }

    private static void createTestIndex(String indexName, @Nullable Map<String, Object> customMappings) throws IOException {
        final Map<String, Object> indexMappings;
        if (customMappings != null) {
            indexMappings = Stream.concat(ecsDynamicTemplates.entrySet().stream(), customMappings.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        } else {
            indexMappings = ecsDynamicTemplates;
        }
        try (XContentBuilder bodyBuilder = JsonXContent.contentBuilder()) {
            bodyBuilder.startObject();
            bodyBuilder.startObject("settings");
            bodyBuilder.field("index.mapping.total_fields.limit", 10000);
            bodyBuilder.endObject();
            bodyBuilder.field("mappings", indexMappings);
            bodyBuilder.endObject();

            Request createIndexRequest = new Request("PUT", "/" + indexName);
            createIndexRequest.setJsonEntity(Strings.toString(bodyBuilder));
            // noinspection resource
            Response response = ESRestTestCase.client().performRequest(createIndexRequest);
            assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        }
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
            case "boolean" -> {
                return randomBoolean();
            }
            case "flattened" -> {
                // creating multiple subfields
                return Map.of("subfield1", randomAlphaOfLength(20), "subfield2", randomAlphaOfLength(20));
            }
        }
        throw new IllegalArgumentException("Unknown field type: " + type);
    }

    private Map<String, Object> getMappings(String indexName) throws IOException {
        Request getMappingRequest = new Request("GET", "/" + indexName + "/_mapping");
        // noinspection resource
        Response response = ESRestTestCase.client().performRequest(getMappingRequest);
        assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
        Map<String, Object> mappingResponse;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, response.getEntity().getContent())) {
            mappingResponse = parser.map();
        }
        assertThat(mappingResponse.size(), equalTo(1));
        Map<String, Object> indexMap = (Map<String, Object>) mappingResponse.get(indexName);
        assertNotNull(indexMap);
        Map<String, Object> mappings = (Map<String, Object>) indexMap.get("mappings");
        assertNotNull(mappings);
        return (Map<String, Object>) mappings.get("properties");
    }

    private void processRawMappingsSubtree(
        final Map<String, Object> fieldSubtrees,
        final Map<String, Map<String, Object>> flatFieldMappings,
        final Map<String, Map<String, Object>> flatMultiFieldsMappings,
        final String subtreePrefix
    ) {
        fieldSubtrees.forEach((fieldName, fieldMappings) -> {
            String fieldFullPath = subtreePrefix + fieldName;
            Map<String, Object> fieldMappingsMap = ((Map<String, Object>) fieldMappings);
            if (fieldMappingsMap.get("type") != null) {
                flatFieldMappings.put(fieldFullPath, fieldMappingsMap);
            }
            Map<String, Object> subfields = (Map<String, Object>) fieldMappingsMap.get("properties");
            if (subfields != null) {
                processRawMappingsSubtree(subfields, flatFieldMappings, flatMultiFieldsMappings, fieldFullPath + ".");
            }

            Map<String, Map<String, Object>> fields = (Map<String, Map<String, Object>>) fieldMappingsMap.get("fields");
            if (fields != null) {
                fields.forEach((subFieldName, multiFieldMappings) -> {
                    String subFieldFullPath = fieldFullPath + "." + subFieldName;
                    flatMultiFieldsMappings.put(subFieldFullPath, multiFieldMappings);
                });
            }
        });
    }

    private void verifyEcsMappings(String indexName) throws IOException {
        verifyEcsMappings(indexName, "");
    }

    private void verifyEcsMappings(String indexName, String fieldPrefix) throws IOException {
        final Map<String, Object> rawMappings = getMappings(indexName);
        final Map<String, Map<String, Object>> flatFieldMappings = new HashMap<>();
        final Map<String, Map<String, Object>> flatMultiFieldsMappings = new HashMap<>();
        processRawMappingsSubtree(rawMappings, flatFieldMappings, flatMultiFieldsMappings, "");

        Map<String, Map<String, Object>> shallowFieldMapCopy = ecsFlatFieldDefinitions.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> fieldPrefix + e.getKey(), Map.Entry::getValue));

        logger.info("Testing mapping of {} ECS fields", shallowFieldMapCopy.size());
        List<String> nonEcsFields = new ArrayList<>();
        Map<String, String> fieldToWrongMappingType = new HashMap<>();
        List<String> wronglyIndexedFields = new ArrayList<>();
        List<String> wronglyDocValuedFields = new ArrayList<>();
        flatFieldMappings.forEach((fieldName, actualMappings) -> {
            Map<String, Object> expectedMappings = shallowFieldMapCopy.remove(fieldName);
            if (expectedMappings == null) {
                nonEcsFields.add(fieldName);
            } else {
                compareExpectedToActualMappings(
                    fieldName,
                    actualMappings,
                    expectedMappings,
                    fieldToWrongMappingType,
                    wronglyIndexedFields,
                    wronglyDocValuedFields
                );
            }
        });

        Map<String, Map<String, Object>> shallowMultiFieldMapCopy = ecsFlatMultiFieldDefinitions.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> fieldPrefix + e.getKey(), Map.Entry::getValue));
        logger.info("Testing mapping of {} ECS multi-fields", shallowMultiFieldMapCopy.size());
        flatMultiFieldsMappings.forEach((fieldName, actualMappings) -> {
            Map<String, Object> expectedMultiFieldMappings = shallowMultiFieldMapCopy.remove(fieldName);
            if (expectedMultiFieldMappings != null) {
                // not finding an entry in the expected multi-field mappings map is acceptable: our dynamic templates are required to
                // ensure multi-field mapping for all fields with such ECS definitions. However, the patterns in these templates may lead
                // to multi-field mapping for ECS fields for which such are not defined
                compareExpectedToActualMappings(
                    fieldName,
                    actualMappings,
                    expectedMultiFieldMappings,
                    fieldToWrongMappingType,
                    wronglyIndexedFields,
                    wronglyDocValuedFields
                );
            }
        });

        shallowFieldMapCopy.forEach(
            (fieldName, expectedMappings) -> logger.error(
                "ECS field '{}' is not covered by the current dynamic templates. Update {} so that this field is mapped to type '{}'.",
                fieldName,
                ECS_DYNAMIC_TEMPLATES_FILE,
                expectedMappings.get("type")
            )
        );
        shallowMultiFieldMapCopy.keySet().forEach(field -> {
            int lastDotIndex = field.lastIndexOf('.');
            String parentField = field.substring(0, lastDotIndex);
            String subfield = field.substring(lastDotIndex + 1);
            logger.error(
                "ECS field '{}' is expected to have a multi-field mapping with subfield '{}'. Fix {} accordingly.",
                parentField,
                subfield,
                ECS_DYNAMIC_TEMPLATES_FILE
            );
        });
        fieldToWrongMappingType.forEach((fieldName, actualMappingType) -> {
            Map<String, Object> fieldMappings = ecsFlatFieldDefinitions.get(fieldName);
            if (fieldMappings == null) {
                fieldMappings = ecsFlatMultiFieldDefinitions.get(fieldName);
            }
            String ecsExpectedType = (String) fieldMappings.get("type");
            logger.error(
                "ECS field '{}' should be mapped to type '{}' but is mapped to type '{}'. Update {} accordingly.",
                fieldName,
                ecsExpectedType,
                actualMappingType,
                ECS_DYNAMIC_TEMPLATES_FILE
            );
        });
        nonEcsFields.forEach(field -> logger.error("The test document contains '{}', which is not an ECS field", field));
        wronglyIndexedFields.forEach(fieldName -> logger.error("ECS field '{}' should be mapped with \"index: false\"", fieldName));
        wronglyDocValuedFields.forEach(fieldName -> logger.error("ECS field '{}' should be mapped with \"doc_values: false\"", fieldName));

        assertTrue("ECS is not fully covered by the current ECS dynamic templates, see details above", shallowFieldMapCopy.isEmpty());
        assertTrue(
            "ECS is not fully covered by the current ECS dynamic templates' multi-fields definitions, see details above",
            shallowMultiFieldMapCopy.isEmpty()
        );
        assertTrue(
            "At least one field was mapped with a type that mismatches the ECS definitions, see details above",
            fieldToWrongMappingType.isEmpty()
        );
        assertTrue("The test document contains non-ECS fields, see details above", nonEcsFields.isEmpty());
        assertTrue(
            "At least one field was not mapped with \"index: false\" as it should according to its ECS definitions, see details above",
            wronglyIndexedFields.isEmpty()
        );
        assertTrue(
            "At least one field was not mapped with \"doc_values: false\" as it should according to its ECS definitions, see "
                + "details above",
            wronglyDocValuedFields.isEmpty()
        );
    }

    private static void compareExpectedToActualMappings(
        String fieldName,
        Map<String, Object> actualMappings,
        Map<String, Object> expectedMappings,
        Map<String, String> fieldToWrongMappingType,
        List<String> wronglyIndexedFields,
        List<String> wronglyDocValuedFields
    ) {
        String expectedType = (String) expectedMappings.get("type");
        String actualMappingType = (String) actualMappings.get("type");
        if (actualMappingType.equals(expectedType) == false) {
            fieldToWrongMappingType.put(fieldName, actualMappingType);
        }
        if (expectedMappings.get("index") != actualMappings.get("index")) {
            wronglyIndexedFields.add(fieldName);
        }
        if (expectedMappings.get("doc_values") != actualMappings.get("doc_values")) {
            wronglyDocValuedFields.add(fieldName);
        }
    }
}
