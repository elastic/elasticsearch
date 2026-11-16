/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Analysis of nested fields that FROM branches map differently. The 303_nested_branches YAML tests run these shapes against
 * real indices.
 */
public class NestedReductionsBranchTests extends LuceneTestCase {
    @Before
    public void onlySnapshotBuildsRegisterNested() {
        assumeTrue("The nested functions are registered on snapshot builds", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void logging() {
        LogConfigurator.configureESLogging();
        IndexSettings.MODE.get(Settings.EMPTY);
    }

    public void testIncompatibleBranchesRequireConversionBeforeUnion() {
        for (String expression : List.of("items.price", "items.price::long")) {
            assertThrows(
                VerificationException.class,
                () -> NestedReductionsTestSupport.analyze(
                    "FROM (FROM alpha), (FROM beta) | EVAL total = NESTED(SUM(" + expression + "))",
                    schemas(List.of(alpha(), beta("keyword"))),
                    Map.of()
                )
            );
        }
    }

    public void testUserCannotReuseSchemaNamesDuringNestedQuery() {
        String name = "$$nested_reductions.items.price";
        for (String command : List.of(
            "EVAL `" + name + "` = 1",
            "RENAME id AS `" + name + "`",
            "EVAL text = \"forged\" | DISSECT text \"%{" + name + "}\""
        )) {
            var error = assertThrows(
                VerificationException.class,
                () -> NestedReductionsTestSupport.analyze(
                    "FROM alpha | " + command + " | EVAL quantity = NESTED(SUM(items.quantity))",
                    schemas(List.of(alpha())),
                    Map.of()
                )
            );
            assertTrue(error.getMessage(), error.getMessage().contains("reserved internal"));
        }
    }

    public void testDifferentNumericMappingsRemainUnsupported() {
        assertThrows(
            VerificationException.class,
            () -> NestedReductionsTestSupport.analyze(
                "FROM (FROM alpha), (FROM beta) | EVAL total = NESTED(SUM(items.price))",
                schemas(List.of(alpha(), beta("integer"))),
                Map.of()
            )
        );
    }

    private record Fixture(String index, Map<String, String> fields) {}

    private static Fixture alpha() {
        return fixture("alpha", "long");
    }

    private static Fixture beta(String priceType) {
        return fixture("beta", priceType);
    }

    private static Fixture fixture(String index, String priceType) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "keyword");
        fields.put("items", "nested");
        fields.put("items.price", priceType);
        fields.put("items.quantity", "long");
        return new Fixture(index, fields);
    }

    private static Map<String, EsIndex> schemas(List<Fixture> fixtures) {
        Map<String, EsIndex> indices = new LinkedHashMap<>();
        for (Fixture fixture : fixtures) {
            indices.put(fixture.index(), schema(List.of(fixture)));
        }
        EsIndex combined = schema(fixtures);
        indices.put(combined.name(), combined);
        return indices;
    }

    private static EsIndex schema(List<Fixture> fixtures) {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        for (Fixture fixture : fixtures) {
            Map<String, IndexFieldCapabilities> fields = new LinkedHashMap<>();
            fixture.fields()
                .forEach(
                    (name, type) -> fields.put(
                        name,
                        new IndexFieldCapabilities(name, type, false, true, true, false, false, null, null, Map.of(), null, 0, false, false)
                    )
                );
            responses.add(
                new FieldCapabilitiesIndexResponse(fixture.index(), fixture.index(), fields, true, IndexMode.STANDARD, 1, 0, 0, 0)
            );
        }
        String pattern = String.join(",", fixtures.stream().map(Fixture::index).toList());
        return IndexResolver.mergedMappings(
            pattern,
            false,
            new IndexResolver.FieldsInfo(
                new FieldCapabilitiesResponse(responses, List.of()),
                TransportVersion.current(),
                true,
                false,
                false,
                false,
                false,
                false,
                true,
                null
            ),
            false,
            (name, response) -> Map.of("", List.of(name.split(",")))
        ).get();
    }
}
