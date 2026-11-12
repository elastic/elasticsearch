/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsRequest;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

/** The source-backed prototype must reject mappings whose indexed values it cannot reproduce. */
public class NestedReductionsSourceValidationTests extends LuceneTestCase {
    private static final Set<String> VISIBLE = Set.of("items", "items.price");

    @BeforeClass
    public static void logging() {
        LogConfigurator.configureESLogging();
    }

    @Test
    public void testCapabilityStaysUnadvertised() {
        assertFalse(EsqlCapabilities.Cap.NESTED_REDUCTIONS.isEnabled());
        assertFalse(EsqlCapabilities.capabilities(new EsqlFunctionRegistry(), false).capabilities().contains("nested_reductions"));
    }

    @Test
    public void testPlainStoredSource() {
        validate(Settings.EMPTY, mapping(Map.of("type", "long")), VISIBLE);
    }

    @Test
    public void testUnavailableOrReshapedSource() {
        for (var source : java.util.List.of(
            Map.of("enabled", false),
            Map.of("includes", java.util.List.of("items")),
            Map.of("excludes", java.util.List.of("items.price")),
            Map.of("mode", "synthetic")
        )) {
            assertThrows(IllegalArgumentException.class, () -> validate(Settings.EMPTY, Map.of("_source", source), VISIBLE));
        }
        for (String mode : java.util.List.of("synthetic", "disabled", "columnar_stored")) {
            assertThrows(
                IllegalArgumentException.class,
                () -> validate(Settings.builder().put("index.mapping.source.mode", mode).build(), mapping(Map.of("type", "long")), VISIBLE)
            );
        }
        assertThrows(
            IllegalArgumentException.class,
            () -> validate(Settings.builder().put("index.mode", "logsdb").build(), mapping(Map.of("type", "long")), VISIBLE)
        );
    }

    @Test
    public void testMappedTransformations() {
        for (var field : java.util.List.of(
            Map.of("type", "long", "null_value", 7),
            Map.of("type", "long", "ignore_malformed", true),
            Map.of("type", "long", "coerce", false),
            Map.of("type", "keyword", "ignore_above", 10),
            Map.of("type", "keyword", "normalizer", "lowercase"),
            Map.of("type", "long", "script", Map.of("source", "emit(1)")),
            Map.of("type", "alias", "path", "items.other")
        )) {
            var error = assertThrows(IllegalArgumentException.class, () -> validate(Settings.EMPTY, mapping(field), VISIBLE));
            assertTrue(error.getMessage(), error.getMessage().contains("items.price"));
        }
    }

    @Test
    public void testExplicitDefaultsThatMatchTheMapper() {
        validate(Settings.EMPTY, mapping(Map.of("type", "long", "coerce", true)), VISIBLE);
        validate(Settings.EMPTY, mapping(Map.of("type", "long", "ignore_malformed", false)), VISIBLE);
    }

    @Test
    public void testMultifieldHasNoIndependentSourceValue() {
        var mapping = mapping(Map.of("type", "text", "fields", Map.of("raw", Map.of("type", "keyword"))));
        assertThrows(
            IllegalArgumentException.class,
            () -> validate(Settings.EMPTY, mapping, Set.of("items", "items.price", "items.price.raw"))
        );
    }

    @Test
    public void testIndexDefaultsCanTransformValues() {
        for (var settings : java.util.List.of(
            Settings.builder().put("index.mapping.ignore_malformed", true).build(),
            Settings.builder().put("index.mapping.coerce", false).build(),
            Settings.builder().put("index.mapping.ignore_above", 10).build()
        )) {
            assertThrows(IllegalArgumentException.class, () -> validate(settings, mapping(Map.of("type", "long")), VISIBLE));
        }
    }

    @Test
    public void testRoundedNumericMappings() {
        for (String type : java.util.List.of("float", "half_float", "scaled_float")) {
            assertThrows(IllegalArgumentException.class, () -> validate(Settings.EMPTY, mapping(Map.of("type", type)), VISIBLE));
        }
    }

    @Test
    public void testHiddenCopySourceCannotChangeVisibleValues() {
        var mapping = Map.of(
            "properties",
            Map.of(
                "items",
                Map.of(
                    "type",
                    "nested",
                    "properties",
                    Map.of("price", Map.of("type", "long"), "secret", Map.of("type", "long", "copy_to", "items.price"))
                )
            )
        );
        var error = assertThrows(IllegalArgumentException.class, () -> validate(Settings.EMPTY, mapping, VISIBLE));
        assertFalse(error.getMessage(), error.getMessage().contains("secret"));
    }

    @Test
    public void testValidationFlagSurvivesTransport() throws Exception {
        var request = new EsqlResolveFieldsRequest(new FieldCapabilitiesRequest().indices("orders").fields("*"), true);
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersion.current());
                var copy = new EsqlResolveFieldsRequest(in);
                assertTrue(copy.nestedReductions());
                assertArrayEquals(request.indices(), copy.indices());
                assertEquals(0, in.available());
            }
        }
    }

    @Test
    public void testOlderPlanningNodeCannotSilentlySkipValidation() throws Exception {
        var caps = new FieldCapabilitiesRequest().indices("orders").fields("*");
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.minimumCompatible());
            assertThrows(IllegalArgumentException.class, () -> new EsqlResolveFieldsRequest(caps, true).writeTo(out));
            assertEquals(0, out.size());
            new EsqlResolveFieldsRequest(caps).writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersion.minimumCompatible());
                assertFalse(new EsqlResolveFieldsRequest(in).nestedReductions());
                assertEquals(0, in.available());
            }
        }
    }

    @Test
    public void testInvisibleFieldsDoNotLeakThroughValidation() {
        var mapping = mapping(Map.of("type", "keyword", "normalizer", "secret_normalizer"));
        validate(Settings.EMPTY, mapping, Set.of("order_id"));
    }

    @Test
    public void testOrdinaryParentMappingsAreUnaffected() {
        validate(Settings.EMPTY, Map.of("properties", Map.of("parent", Map.of("type", "keyword", "null_value", "x"))), Set.of("parent"));
    }

    private static Map<String, Object> mapping(Map<String, ?> field) {
        return Map.of("properties", Map.of("items", Map.of("type", "nested", "properties", Map.of("price", field))));
    }

    private static void validate(Settings settings, Map<String, ?> mapping, Set<String> visible) {
        NestedReductionsSourceValidation.validate("orders", settings, mapping, visible);
    }
}
