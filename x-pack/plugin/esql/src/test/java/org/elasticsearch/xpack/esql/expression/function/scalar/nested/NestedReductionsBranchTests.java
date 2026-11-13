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
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Independent FROM branches must merge mapping applicability without merging parent rows. */
public class NestedReductionsBranchTests extends LuceneTestCase {
    private final String mode;

    @ParametersFactory
    public static Iterable<Object[]> modes() {
        return List.of(new Object[] { "optimized" }, new Object[] { "minimal" }, new Object[] { "transported" });
    }

    public NestedReductionsBranchTests(@Name("mode") String mode) {
        this.mode = mode;
    }

    @Before
    public void onlySnapshotBuildsRegisterNested() {
        assumeTrue("The nested functions are registered on snapshot builds", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void logging() {
        LogConfigurator.configureESLogging();
        IndexSettings.MODE.get(Settings.EMPTY);
    }

    public void testCompatibleBranches() throws Exception {
        assertEquals(
            List.of(row("alpha", 26L), row("beta", 14L)),
            run(
                "FROM (FROM alpha), (FROM beta) | EVAL total = NESTED(SUM(items.price * items.quantity)) | KEEP id, total | SORT id",
                List.of(alpha(), beta(true, "long"))
            )
        );
    }

    public void testMissingLeafMappingIsNotReadFromRawSource() throws Exception {
        assertEquals(
            List.of(row("alpha", 26L, 8L, false), row("beta", null, 7L, true)),
            run(
                "FROM (FROM alpha), (FROM beta) | EVAL total = NESTED(SUM(items.price * items.quantity)), "
                    + "price = NESTED(SUM(items.price)), missing = NESTED_ANY(items.quantity IS NULL) "
                    + "| KEEP id, total, price, missing | SORT id",
                List.of(alpha(), beta(false, "long"))
            )
        );
    }

    public void testMissingScopeDoesNotDiscoverAnUnmappedArray() throws Exception {
        assertEquals(
            List.of(row("alpha", 26L, true), row("gamma", null, false)),
            run(
                "FROM (FROM alpha), (FROM gamma) | EVAL total = NESTED(SUM(items.price * items.quantity)), "
                    + "present = NESTED_ANY(items.price > 0) | KEEP id, total, present | SORT id",
                List.of(alpha(), gamma())
            )
        );
    }

    public void testRepeatedBranchIsCountedAgain() throws Exception {
        assertEquals(
            List.of(row(3L, 66L)),
            run(
                "FROM (FROM alpha), (FROM beta), (FROM alpha) | EVAL total = NESTED(SUM(items.price * items.quantity)) "
                    + "| STATS n = COUNT(*), amount = SUM(total)",
                List.of(alpha(), beta(true, "long"))
            )
        );
    }

    public void testNestedUnionRetainsApplicability() throws Exception {
        assertEquals(
            List.of(row(3L, 40L)),
            run(
                "FROM (FROM (FROM alpha), (FROM beta)), (FROM gamma) | EVAL total = NESTED(SUM(items.price * items.quantity)) "
                    + "| STATS n = COUNT(*), amount = SUM(total)",
                List.of(alpha(), beta(true, "long"), gamma())
            )
        );
    }

    public void testMultiIndexFromAndUnionAgree() throws Exception {
        var fixtures = List.of(alpha(), beta(false, "long"), gamma());
        String suffix = " | EVAL total = NESTED(SUM(items.price * items.quantity)), missing = NESTED_ANY(items.quantity IS NULL) "
            + "| KEEP id, total, missing | SORT id";
        var expected = List.of(row("alpha", 26L, false), row("beta", null, true), row("gamma", null, false));
        assertEquals(expected, run("FROM alpha,beta,gamma" + suffix, fixtures));
        assertEquals(expected, run("FROM (FROM alpha), (FROM beta), (FROM gamma)" + suffix, fixtures));
    }

    public void testIncompatibleBranchesRequireConversionBeforeUnion() {
        for (String expression : List.of("items.price", "items.price::long")) {
            assertThrows(
                VerificationException.class,
                () -> NestedReductionsTestSupport.analyze(
                    "FROM (FROM alpha), (FROM beta) | EVAL total = NESTED(SUM(" + expression + "))",
                    schemas(List.of(alpha(), beta(true, "keyword"))),
                    Map.of()
                )
            );
        }
    }

    public void testUnreferencedTypeConflictDoesNotRejectCompatibleKey() throws Exception {
        assertEquals(
            List.of(row("alpha", 6L), row("beta", 2L)),
            run(
                "FROM (FROM alpha), (FROM beta) | EVAL quantity = NESTED(SUM(items.quantity)) | KEEP id, quantity | SORT id",
                List.of(alpha(), beta(true, "keyword"))
            )
        );
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

    public void testOrdinaryQueryCanUseSchemaLikeColumnName() throws Exception {
        assertEquals(
            List.of(row("ordinary")),
            run(
                "FROM alpha | EVAL `$$nested_reductions.items.price` = \"ordinary\" | KEEP `$$nested_reductions.items.price`",
                List.of(fixture("alpha", false, false, "long", "[]"))
            )
        );
    }

    public void testNestedArraysPreserveObjectCorrelationForEveryReduction() throws Exception {
        var fixtures = List.of(
            fixture("nr_nested_arrays", true, true, "long", "[null,[{\"price\":5,\"quantity\":4},null],[{\"price\":3,\"quantity\":2}]]")
        );
        assertEquals(
            List.of(row("nr_nested_arrays", 26L)),
            run("FROM nr_nested_arrays | EVAL total = NESTED(SUM(items.price * items.quantity)) | KEEP id, total", fixtures)
        );
        assertEquals(
            List.of(row("nr_nested_arrays")),
            run("FROM nr_nested_arrays | WHERE NESTED_ANY(items.price == 5 AND items.quantity == 4) | KEEP id", fixtures)
        );
        assertEquals(
            List.of(),
            run("FROM nr_nested_arrays | WHERE NESTED_ANY(items.price == 3 AND items.quantity == 4) | KEEP id", fixtures)
        );
        assertEquals(
            List.of(row("nr_nested_arrays", 5L)),
            run("FROM nr_nested_arrays | EVAL picked = NESTED(LAST(items.price, items.quantity)) | KEEP id, picked", fixtures)
        );
    }

    public void testDifferentNumericMappingsRemainUnsupported() {
        assertThrows(
            VerificationException.class,
            () -> NestedReductionsTestSupport.analyze(
                "FROM (FROM alpha), (FROM beta) | EVAL total = NESTED(SUM(items.price))",
                schemas(List.of(alpha(), beta(true, "integer"))),
                Map.of()
            )
        );
    }

    public void testCompletedConvertedReductionsCanMerge() throws Exception {
        assertEquals(
            List.of(row(2L, 15L)),
            run(
                "FROM (FROM alpha | EVAL total = NESTED(SUM(items.price)) | KEEP total), "
                    + "(FROM beta | EVAL total = NESTED(SUM(items.price::long)) | KEEP total) "
                    + "| STATS n = COUNT(*), amount = SUM(total)",
                List.of(alpha(), beta(true, "keyword"))
            )
        );
    }

    private record Fixture(String index, Map<String, String> fields, String source) {}

    private static Fixture alpha() {
        return fixture("alpha", true, true, "long", "[{\"price\":3,\"quantity\":2},{\"price\":5,\"quantity\":4}]");
    }

    private static Fixture beta(boolean quantity, String priceType) {
        return fixture("beta", true, quantity, priceType, "[{\"price\":7,\"quantity\":2}]");
    }

    private static Fixture gamma() {
        return fixture("gamma", false, false, "long", "[{\"price\":999,\"quantity\":99}]");
    }

    private static Fixture fixture(String index, boolean nested, boolean quantity, String priceType, String items) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "keyword");
        if (nested) {
            fields.put("items", "nested");
            fields.put("items.price", priceType);
            if (quantity) {
                fields.put("items.quantity", "long");
            }
        }
        return new Fixture(index, fields, "{\"id\":\"" + index + "\",\"items\":" + items + "}");
    }

    private List<List<Object>> run(String query, List<Fixture> fixtures) throws Exception {
        var analyzed = NestedReductionsTestSupport.analyze(query, schemas(fixtures), Map.of());
        assertTrue(analyzed.output().stream().noneMatch(a -> a.synthetic()));
        if (mode.equals("transported")) {
            analyzed = NestedReductionsTestSupport.roundTrip(query, analyzed);
        }
        return NestedReductionsTestSupport.runLocal(
            query,
            analyzed,
            relation -> (int) fixtures.stream().filter(f -> relation.indexProperties().containsKey(f.index())).count(),
            (relation, attribute) -> fixtures.stream().filter(f -> relation.indexProperties().containsKey(f.index())).map(f -> {
                if (attribute instanceof MetadataAttribute) {
                    return new BytesRef(switch (attribute.name()) {
                        case "_source" -> f.source();
                        case "_index" -> f.index();
                        default -> throw new AssertionError(attribute);
                    });
                }
                if (attribute.name().equals("id")) {
                    return new BytesRef(f.index());
                }
                throw new AssertionError(attribute);
            }).toList(),
            true,
            mode.equals("minimal") == false
        );
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

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }
}
