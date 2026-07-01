/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

public class DatasetMappingTests extends AbstractWireSerializingTestCase<DatasetMapping> {

    @Override
    protected Writeable.Reader<DatasetMapping> instanceReader() {
        return DatasetMapping::new;
    }

    @Override
    protected DatasetMapping createTestInstance() {
        return DatasetTests.randomMapping();
    }

    @Override
    protected DatasetMapping mutateInstance(DatasetMapping instance) {
        return randomValueOtherThan(instance, DatasetTests::randomMapping);
    }

    /**
     * Guard against vocabulary drift from the index mapper's {@code dynamic} parameter. We deliberately do NOT reuse
     * {@link ObjectMapper.Dynamic} (it carries STRICT/RUNTIME, which are meaningless for read-only external data —
     * reusing it would let our type represent invalid states). Instead our {@link DatasetMapping.Dynamic} is the
     * {TRUE, FALSE} subset, and this test pins that relationship: if ES adds, renames, or removes a dynamic value, it
     * fails and forces us to re-decide whether to support it rather than silently diverging.
     */
    public void testDynamicStaysInSyncWithIndexMapperDynamic() {
        Set<String> esValues = Arrays.stream(ObjectMapper.Dynamic.values()).map(Enum::name).collect(Collectors.toSet());
        Set<String> ourValues = Arrays.stream(DatasetMapping.Dynamic.values()).map(Enum::name).collect(Collectors.toSet());

        // Every value we support must exist in the index mapper under the same name.
        assertTrue(
            "DatasetMapping.Dynamic " + ourValues + " must be a subset of ObjectMapper.Dynamic " + esValues,
            esValues.containsAll(ourValues)
        );
        // The index-mapper values we deliberately exclude are exactly STRICT and RUNTIME. If this set changes, the
        // index mapper grew/renamed a dynamic value and we must consciously decide how external datasets treat it.
        Set<String> excluded = new HashSet<>(esValues);
        excluded.removeAll(ourValues);
        assertEquals(Set.of("STRICT", "RUNTIME"), excluded);

        // Parse vocabulary stays aligned: we accept our values case-insensitively and reject the excluded ones.
        assertEquals(DatasetMapping.Dynamic.TRUE, DatasetMapping.Dynamic.fromString("true"));
        assertEquals(DatasetMapping.Dynamic.FALSE, DatasetMapping.Dynamic.fromString("false"));
        expectThrows(IllegalArgumentException.class, () -> DatasetMapping.Dynamic.fromString("strict"));
        expectThrows(IllegalArgumentException.class, () -> DatasetMapping.Dynamic.fromString("runtime"));
    }

    /**
     * The {@code mappings} block deliberately supports only {@code dynamic} and {@code properties}. Every other
     * core mapping-level key must be rejected, so we cannot silently diverge from (or accidentally absorb a divergent
     * reading of) the core mapping vocabulary — supporting a new key has to be a deliberate, test-breaking change.
     */
    public void testRejectsCoreMappingsKeysWeDoNotSupport() throws IOException {
        for (String key : List.of("runtime", "dynamic_templates", "_routing", "_meta", "_field_names", "subobjects", "_size")) {
            String json = "{\"dynamic\":\"true\",\"" + key + "\":{}}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
                parser.nextToken(); // advance to START_OBJECT, where parseMappings expects to begin
                Exception e = expectThrows(Exception.class, () -> DatasetMapping.parseMappings(parser));
                assertThat("core mappings key [" + key + "] must be rejected", e.getMessage(), containsString(key));
            }
        }
    }

    public void testSourceEnabledParsesAndDefaults() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"dynamic\":\"true\",\"_source\":{\"enabled\":false}}")) {
            parser.nextToken();
            DatasetMapping.Mappings m = DatasetMapping.parseMappings(parser);
            assertEquals(Boolean.FALSE, m.sourceEnabled());
            assertFalse(m.sourceAvailable());
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"dynamic\":\"true\"}")) {
            parser.nextToken();
            DatasetMapping.Mappings m = DatasetMapping.parseMappings(parser);
            assertNull("absent _source leaves the knob unset", m.sourceEnabled());
            assertTrue("unset means available by default", m.sourceAvailable());
        }
        // Only [enabled] is supported under _source; other core _source knobs (mode, includes, ...) are rejected.
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"_source\":{\"mode\":\"synthetic\"}}")) {
            parser.nextToken();
            Exception e = expectThrows(Exception.class, () -> DatasetMapping.parseMappings(parser));
            assertThat(e.getMessage(), containsString("_source"));
        }
    }

    public void testAssembleReturnsNullWhenMappingsAbsent() {
        assertNull(DatasetMapping.assemble(null));
    }

    public void testIdPathParsesAndDefaults() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"dynamic\":\"true\",\"_id\":{\"path\":\"request_id\"}}")) {
            parser.nextToken();
            DatasetMapping.Mappings m = DatasetMapping.parseMappings(parser);
            assertEquals("request_id", m.idPath());
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"dynamic\":\"true\"}")) {
            parser.nextToken();
            assertNull("absent _id leaves idPath unset", DatasetMapping.parseMappings(parser).idPath());
        }
        // Only [path] is supported under _id; other keys are rejected.
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"_id\":{\"type\":\"keyword\"}}")) {
            parser.nextToken();
            Exception e = expectThrows(Exception.class, () -> DatasetMapping.parseMappings(parser));
            assertThat(e.getMessage(), containsString("_id"));
        }
    }

    public void testDynamicRejectsUnknownValue() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DatasetMapping.Dynamic.fromString("strict"));
        assertTrue(e.getMessage().contains("strict"));
    }

    public void testDynamicDefaultMappingsPreservesOrderAndSource() throws IOException {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("when", new DatasetFieldMapping("date", "ts"));
        props.put("amount", new DatasetFieldMapping("double", null));
        DatasetMapping.Mappings mappings = new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, props);
        DatasetMapping mapping = new DatasetMapping(mappings);
        DatasetMapping copy = copyInstance(mapping);
        assertEquals(mapping, copy);
        assertEquals("ts", copy.mappings().properties().get("when").path());
        assertEquals(java.util.List.of("when", "amount"), java.util.List.copyOf(copy.mappings().properties().keySet()));
    }
}
