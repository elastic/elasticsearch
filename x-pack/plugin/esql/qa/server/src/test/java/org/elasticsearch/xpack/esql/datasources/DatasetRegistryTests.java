/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for the {@code PUT /_query/dataset/<name>} body {@link DatasetRegistry} builds, which is also the
 * content signature {@link DatasetRegistry#ensureDataset} caches on. Lives in the same qa/server project as the class
 * under test so it can be a plain {@link ESTestCase} unit test — no cluster, no client — alongside
 * {@link FixtureUtilsTests}.
 */
public class DatasetRegistryTests extends ESTestCase {

    /**
     * The regression pin: for a given settings map, a dataset registered without a declared schema must produce the
     * exact bytes this registry produced before declarations were reachable, so every existing dataset-backed spec
     * file and IT is untouched.
     */
    public void testBodyWithoutADeclarationIsUnchanged() throws IOException {
        assertEquals(
            "{\"data_source\":\"ds\",\"resource\":\"s3://b/k\"}",
            DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of(), null)
        );
        assertEquals(
            "{\"data_source\":\"ds\",\"resource\":\"s3://b/k\",\"settings\":{\"header_row\":true}}",
            DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of("header_row", true), null)
        );
    }

    /** A declaration is a sibling of settings, never an entry inside them — settings validation rejects it there. */
    public void testDeclarationIsASiblingOfSettings() throws IOException {
        Map<String, Object> mappings = declaration("emp_no", "integer");
        String body = DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of("header_row", true), mappings);

        assertEquals(
            "{\"data_source\":\"ds\",\"resource\":\"s3://b/k\",\"settings\":{\"header_row\":true},"
                + "\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"emp_no\":{\"type\":\"integer\"}}}}",
            body
        );
    }

    /** A dataset that declares a schema and sets no format option omits {@code settings} entirely. */
    public void testDeclarationWithoutSettingsOmitsSettings() throws IOException {
        String body = DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of(), declaration("emp_no", "integer"));
        assertThat(body, not(containsString("\"settings\"")));
        assertThat(body, containsString("\"mappings\""));
    }

    /**
     * A declared schema's {@code properties} order IS its column order, and a strict declared read emits columns in
     * declaration order — so the order has to survive into the BYTES, not merely into the map handed over. Asserted on
     * the serialized body for that reason.
     */
    public void testPropertyOrderSurvivesIntoTheBody() throws IOException {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("zulu", Map.of("type", "keyword"));
        properties.put("alpha", Map.of("type", "long"));
        properties.put("mike", Map.of("type", "keyword"));
        properties.put("bravo", Map.of("type", "long"));
        Map<String, Object> mappings = new LinkedHashMap<>();
        mappings.put("dynamic", "false");
        mappings.put("properties", properties);

        assertThat(
            DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of(), mappings),
            containsString(
                "\"properties\":{\"zulu\":{\"type\":\"keyword\"},\"alpha\":{\"type\":\"long\"},"
                    + "\"mike\":{\"type\":\"keyword\"},\"bravo\":{\"type\":\"long\"}}"
            )
        );
    }

    /**
     * {@code ensureDataset} caches on the request body, which is what makes a declaration part of a registration's
     * identity. Two registrations of one dataset name that differ only in their schema must differ in signature, or
     * the second silently reuses the first's declaration.
     */
    public void testSignatureDistinguishesDeclarations() throws IOException {
        String none = DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of(), null);
        String strict = DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of(), declaration("emp_no", "integer"));
        String retyped = DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of(), declaration("emp_no", "long"));
        String renamed = DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of(), declaration("id", "integer"));

        assertNotEquals("a declaration must not collapse onto an undeclared registration", none, strict);
        assertNotEquals("a retyped column must re-register", strict, retyped);
        assertNotEquals("a renamed column must re-register", strict, renamed);
    }

    /** The same declaration twice is the same signature, so a repeat registration stays a no-op. */
    public void testSignatureIsStableForAnIdenticalDeclaration() throws IOException {
        assertEquals(
            DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of("header_row", true), declaration("emp_no", "integer")),
            DatasetRegistry.datasetRequestBody("ds", "s3://b/k", Map.of("header_row", true), declaration("emp_no", "integer"))
        );
    }

    /** A directive with no WITH clause declares nothing — no settings, no schema. */
    public void testNoWithClauseDeclaresNothing() throws IOException {
        DatasetRegistry.DatasetOptions o = DatasetRegistry.parseDirectiveOptions(null);
        assertEquals(Map.of(), o.settings());
        assertNull(o.mappings());
    }

    /** The reserved key is lifted OUT of the settings map, so a declaration never reaches settings validation. */
    public void testReservedKeyIsLiftedOutOfSettings() throws IOException {
        String withJson = "{\"header_row\": true, \"mappings\": {\"dynamic\": \"false\", "
            + "\"properties\": {\"id\": {\"type\": \"long\", \"path\": \"emp_no\"}}}}";
        DatasetRegistry.DatasetOptions o = DatasetRegistry.parseDirectiveOptions(withJson);

        assertEquals("settings must not carry the reserved key", Map.of("header_row", true), o.settings());
        assertEquals(Map.of("dynamic", "false", "properties", Map.of("id", Map.of("type", "long", "path", "emp_no"))), o.mappings());
    }

    /**
     * Property order has to survive the PARSE as well as the serialization: an unordered parse would reorder the
     * declared columns before the body was ever built.
     */
    public void testParseKeepsPropertyOrder() throws IOException {
        String withJson = "{\"mappings\": {\"dynamic\": \"false\", \"properties\": {"
            + "\"zulu\": {\"type\": \"keyword\"}, \"alpha\": {\"type\": \"long\"}, "
            + "\"mike\": {\"type\": \"keyword\"}, \"bravo\": {\"type\": \"long\"}}}}";

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) DatasetRegistry.parseDirectiveOptions(withJson).mappings().get("properties");
        assertEquals(List.of("zulu", "alpha", "mike", "bravo"), List.copyOf(properties.keySet()));
    }

    /**
     * A reserved key whose value is not an object is a spec-authoring error, caught where the directive text is still
     * at hand rather than as a type error from the server or a {@link ClassCastException} here. An explicit null counts:
     * it is a mistake, not a directive that declares nothing.
     */
    public void testNonObjectDeclarationIsRejected() {
        for (String value : new String[] { "\"strict\"", "3", "[{\"dynamic\": \"false\"}]", "null" }) {
            String withJson = "{\"mappings\": " + value + "}";
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> DatasetRegistry.parseDirectiveOptions(withJson)
            );
            assertThat(e.getMessage(), containsString("[mappings] in a dataset directive's WITH must be a JSON object"));
        }
    }

    /**
     * The harness guards key off this predicate, so it must answer on the reserved name as a KEY: a setting whose value
     * merely equals {@code "mappings"} is not a declaration.
     */
    public void testDeclaresMappingsAnswersOnTheKeyNotTheText() {
        assertTrue(DatasetRegistry.declaresMappings("{\"mappings\": {\"dynamic\": \"false\"}}"));
        assertTrue(DatasetRegistry.declaresMappings("{\"header_row\": true, \"mappings\": {\"dynamic\": \"true\"}}"));

        assertFalse(DatasetRegistry.declaresMappings(null));
        assertFalse(DatasetRegistry.declaresMappings("{}"));
        assertFalse(DatasetRegistry.declaresMappings("{\"header_row\": true}"));
        assertFalse(DatasetRegistry.declaresMappings("{\"column_prefix\": \"mappings\"}"));
    }

    /**
     * {@code declaresSetting} must not see a same-named key nested inside the declared schema. A declared column may be
     * NAMED after a setting, and a text match would treat that as the setting being set — which is how the
     * trim-spaces injection would silently stop firing and read the column-aligned fixtures untrimmed.
     */
    public void testDeclaresSettingIgnoresASameNamedDeclaredColumn() {
        assertTrue(DatasetRegistry.declaresSetting("{\"trim_spaces\": false}", "trim_spaces"));
        assertFalse(DatasetRegistry.declaresSetting(null, "trim_spaces"));
        assertFalse(DatasetRegistry.declaresSetting("{\"header_row\": true}", "trim_spaces"));
        assertFalse(
            "a declared column named after a setting is not that setting",
            DatasetRegistry.declaresSetting("{\"mappings\": {\"properties\": {\"trim_spaces\": {\"type\": \"keyword\"}}}}", "trim_spaces")
        );
    }

    /** A one-column strict declaration, in the shape {@code DatasetMapping} parses. */
    private static Map<String, Object> declaration(String column, String type) {
        Map<String, Object> mappings = new LinkedHashMap<>();
        mappings.put("dynamic", "false");
        mappings.put("properties", Map.of(column, Map.of("type", type)));
        return mappings;
    }
}
