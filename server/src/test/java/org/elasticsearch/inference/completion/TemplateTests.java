/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPLATE_ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPLATE_VARIABLES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEMPLATE_VERSION_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Covers parsing, serialization and memory accounting of the {@code template} object of a chat completion request. The
 * {@code variables} map is caller supplied JSON, so the random generator here only produces values that round trip identically
 * through both the XContent parser and the generic wire format.
 */
public class TemplateTests extends AbstractBWCSerializationTestCase<Template> {

    public void testParsing_MissingId_ThrowsException() throws IOException {
        String json = """
            { "version": 1, "variables": {} }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var e = expectThrows(IllegalArgumentException.class, () -> Template.PARSER.apply(parser, null));
            assertThat(e.getMessage(), containsString("Required [id]"));
        }
    }

    public void testParsing_MissingVersion_ThrowsException() throws IOException {
        String json = """
            { "id": ".summarize", "variables": {} }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var e = expectThrows(IllegalArgumentException.class, () -> Template.PARSER.apply(parser, null));
            assertThat(e.getMessage(), containsString("Required [version]"));
        }
    }

    public void testParsing_MissingVariables_ThrowsException() throws IOException {
        String json = """
            { "id": ".summarize", "version": 1 }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var e = expectThrows(IllegalArgumentException.class, () -> Template.PARSER.apply(parser, null));
            assertThat(e.getMessage(), containsString("Required [variables]"));
        }
    }

    public void testParsing_VersionNotANumber_ThrowsException() throws IOException {
        String json = """
            { "id": ".summarize", "version": "latest", "variables": {} }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var e = expectThrows(XContentParseException.class, () -> Template.PARSER.apply(parser, null));
            assertThat(e.getMessage(), containsString("[version]"));
        }
    }

    public void testParsing_UnknownField_ThrowsException() throws IOException {
        String json = """
            { "id": ".summarize", "version": 1, "variables": {}, "messages": [] }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var e = expectThrows(XContentParseException.class, () -> Template.PARSER.apply(parser, null));
            assertThat(e.getMessage(), containsString("unknown field [messages]"));
        }
    }

    public void testRamBytesUsed_GrowsWithVariables() {
        var small = new Template(".summarize", 1, Map.of());
        var large = new Template(".summarize", 1, Map.of("alerts", List.of(randomAlphaOfLength(2000), randomAlphaOfLength(2000))));
        assertThat(small.ramBytesUsed(), greaterThan(0L));
        assertThat(large.ramBytesUsed(), greaterThan(small.ramBytesUsed() + 4000L));
    }

    /**
     * The RFC's canonical payload is a list of alert objects with long descriptions, i.e. strings at depth two. Lucene's
     * {@code RamUsageEstimator#sizeOfMap} stops recursing at depth one, so this guards against under-counting that shape.
     */
    public void testRamBytesUsed_AccountsForStringsNestedInListsOfObjects() {
        var alertCount = 10;
        var descriptionLength = 4000;
        var shortAlerts = new ArrayList<Map<String, Object>>();
        var longAlerts = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < alertCount; i++) {
            shortAlerts.add(Map.of("id", "a" + i, "description", "x"));
            longAlerts.add(Map.of("id", "a" + i, "description", randomAlphaOfLength(descriptionLength)));
        }
        var small = new Template(".attack-discovery", 1, Map.of("alerts", shortAlerts));
        var large = new Template(".attack-discovery", 1, Map.of("alerts", longAlerts));

        assertThat(large.ramBytesUsed() - small.ramBytesUsed(), greaterThan((long) alertCount * descriptionLength));
    }

    public static Template randomTemplate() {
        return new Template("." + randomAlphaOfLengthBetween(1, 20), randomIntBetween(1, 100), randomVariables(0));
    }

    /**
     * Generates a map containing only values that survive both {@code XContentParser#map()} and
     * {@code StreamOutput#writeGenericMap} unchanged: strings, ints, booleans, doubles, nulls and nested lists and maps of those.
     */
    public static Map<String, Object> randomVariables(int depth) {
        var map = new HashMap<String, Object>();
        var size = randomIntBetween(0, 4);
        for (int i = 0; i < size; i++) {
            map.put(randomAlphaOfLengthBetween(1, 10), randomVariableValue(depth));
        }
        return map;
    }

    private static Object randomVariableValue(int depth) {
        return switch (randomIntBetween(0, depth < 2 ? 6 : 4)) {
            case 0 -> randomAlphaOfLengthBetween(0, 20);
            case 1 -> randomInt();
            case 2 -> randomBoolean();
            case 3 -> randomDouble();
            case 4 -> null;
            case 5 -> randomVariables(depth + 1);
            case 6 -> {
                var list = new ArrayList<>();
                var size = randomIntBetween(0, 3);
                for (int i = 0; i < size; i++) {
                    list.add(randomVariableValue(depth + 1));
                }
                yield list;
            }
            default -> throw new AssertionError("unexpected branch");
        };
    }

    @Override
    protected Template mutateInstanceForVersion(Template instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Template doParseInstance(XContentParser parser) throws IOException {
        return Template.PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<Template> instanceReader() {
        return Template::new;
    }

    @Override
    protected Template createTestInstance() {
        return randomTemplate();
    }

    @Override
    protected Template mutateInstance(Template instance) throws IOException {
        var id = instance.id();
        var version = instance.version();
        var variables = instance.variables();

        switch (randomFrom(Set.of(TEMPLATE_ID_FIELD, TEMPLATE_VERSION_FIELD, TEMPLATE_VARIABLES_FIELD))) {
            case TEMPLATE_ID_FIELD -> id = id + randomAlphaOfLength(1);
            case TEMPLATE_VERSION_FIELD -> version = version + 1;
            case TEMPLATE_VARIABLES_FIELD -> {
                variables = new HashMap<>(variables);
                variables.put(randomValueOtherThanMany(variables::containsKey, () -> randomAlphaOfLength(12)), randomInt());
            }
            default -> throw new AssertionError("Illegal mutation branch");
        }

        return new Template(id, version, variables);
    }
}
