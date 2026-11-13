/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * LAST null keys and values follow ordinary LAST. A null value at the maximum key is returned as null.
 * When every key is null, any tied value is valid. An inner EVAL alias is not part of this contract.
 */
@SuppressWarnings("unchecked")
public class NestedReductionsSelectionTests extends LuceneTestCase {
    private final Map<String, Object> fixture;
    private final Map<String, Object> test;
    private final String mode;

    private record FixtureData(Map<String, Object> value) {
        @Override
        public String toString() {
            return value.get("index").toString();
        }
    }

    private record SelectionCase(Map<String, Object> value) {
        @Override
        public String toString() {
            return value.get("name").toString();
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException {
        List<Object[]> parameters = new ArrayList<>();
        for (String file : List.of(
            "selection.json",
            "same_parent.json",
            "scalar_arrays.json",
            "existence_order.json",
            "paging.json",
            "source_decode.json",
            "source_memory.json"
        )) {
            Map<String, Object> fixture;
            try (var stream = NestedReductionsSelectionTests.class.getResourceAsStream("/nested-scope-contract/" + file)) {
                if (stream == null) {
                    throw new IOException("missing nested contract " + file);
                }
                fixture = XContentHelper.convertToMap(new BytesArray(stream.readAllBytes()), true, XContentType.JSON).v2();
            }
            for (var test : (List<Map<String, Object>>) fixture.get("cases")) {
                for (String mode : List.of("optimized", "minimal", "transported")) {
                    parameters.add(new Object[] { new FixtureData(fixture), new SelectionCase(test), mode });
                }
            }
        }
        return parameters;
    }

    public NestedReductionsSelectionTests(
        @Name("fixture") FixtureData fixture,
        @Name("case") SelectionCase test,
        @Name("mode") String mode
    ) {
        this.fixture = fixture.value();
        this.test = test.value();
        this.mode = mode;
    }

    @Before
    public void onlySnapshotBuildsRegisterNested() {
        assumeTrue("NESTED_ANY is registered on snapshot builds", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void logging() {
        LogConfigurator.configureESLogging();
        IndexSettings.MODE.get(Settings.EMPTY);
    }

    public void testSelection() throws Exception {
        String query = test.get("query").toString();
        if ("analysis".equals(test.get("error"))) {
            var error = assertThrows(VerificationException.class, () -> NestedReductionsTestSupport.analyze(query, schema(), Map.of()));
            if (test.containsKey("reason")) {
                assertTrue(error.getMessage(), error.getMessage().contains(test.get("reason").toString()));
            }
            return;
        }
        var analyzed = NestedReductionsTestSupport.analyze(query, schema(), Map.of());
        if (mode.equals("transported")) {
            analyzed = NestedReductionsTestSupport.roundTrip(query, analyzed);
        }
        var plan = analyzed;
        List<Map<String, Object>> docs = (List<Map<String, Object>>) fixture.get("documents");
        Callable<List<List<Object>>> execute = () -> NestedReductionsTestSupport.runLocal(
            query,
            plan,
            relation -> docs.size(),
            (relation, attribute) -> docs.stream().map(doc -> {
                if (attribute instanceof MetadataAttribute) {
                    return new BytesRef(switch (attribute.name()) {
                        case "_source" -> json(doc);
                        case "_index" -> fixture.get("index").toString();
                        default -> throw new AssertionError(attribute);
                    });
                }
                Object value = doc.get(attribute.name());
                if (value == null) {
                    return null;
                }
                return switch (attribute.dataType()) {
                    case KEYWORD, TEXT -> new BytesRef(value.toString());
                    case LONG -> ((Number) value).longValue();
                    case INTEGER -> ((Number) value).intValue();
                    case DOUBLE -> ((Number) value).doubleValue();
                    case BOOLEAN -> (Boolean) value;
                    default -> throw new AssertionError(attribute);
                };
            }).toList(),
            true,
            mode.equals("minimal") == false
        );
        if ("runtime".equals(test.get("error"))) {
            var error = assertThrows(IllegalArgumentException.class, execute::call);
            assertTrue(error.getMessage(), error.getMessage().contains(test.get("reason").toString()));
            return;
        }
        assertEquals(test.get("type"), analyzed.output().getLast().dataType().outputType());
        var rows = execute.call();
        if (test.containsKey("allowed")) {
            assertEquals(1, rows.size());
            assertEquals(2, rows.getFirst().size());
            assertEquals(test.get("expected_id"), rows.getFirst().getFirst());
            assertTrue(rows.toString(), ((List<?>) test.get("allowed")).contains(rows.getFirst().getLast()));
        } else {
            assertEquals(normalize(test.get("expected")), normalize(rows));
        }
    }

    private EsIndex schema() {
        Map<String, IndexFieldCapabilities> fields = new LinkedHashMap<>();
        addFields("", (Map<?, ?>) ((Map<?, ?>) fixture.get("mapping")).get("properties"), fields);
        String index = fixture.get("index").toString();
        var caps = new FieldCapabilitiesResponse(
            List.of(new FieldCapabilitiesIndexResponse(index, index, fields, true, IndexMode.STANDARD, 1, 0, 0, 0)),
            List.of()
        );
        return IndexResolver.mergedMappings(
            index,
            false,
            new IndexResolver.FieldsInfo(caps, TransportVersion.current(), true, false, false, false, false, false, true, null),
            false,
            (name, response) -> Map.of("", List.of(name))
        ).get();
    }

    private static void addFields(String prefix, Map<?, ?> properties, Map<String, IndexFieldCapabilities> fields) {
        properties.forEach((key, value) -> {
            Map<?, ?> mapping = (Map<?, ?>) value;
            String name = prefix + key;
            fields.put(
                name,
                new IndexFieldCapabilities(
                    name,
                    mapping.get("type").toString(),
                    false,
                    true,
                    true,
                    false,
                    false,
                    null,
                    null,
                    Map.of(),
                    null,
                    0,
                    false,
                    false
                )
            );
            if (mapping.get("properties") instanceof Map<?, ?> children) {
                addFields(name + ".", children, fields);
            }
        });
    }

    private static String json(Object value) {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.value(value);
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static Object normalize(Object value) {
        if (value instanceof Number number) {
            return new BigDecimal(number.toString()).stripTrailingZeros();
        }
        return value instanceof List<?> list ? list.stream().map(NestedReductionsSelectionTests::normalize).toList() : value;
    }
}
