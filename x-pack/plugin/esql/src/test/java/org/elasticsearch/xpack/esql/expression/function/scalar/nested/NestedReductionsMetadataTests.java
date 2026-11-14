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
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Public metadata names must not determine which parent source a reduction reads. */
public class NestedReductionsMetadataTests extends LuceneTestCase {
    private final String mode;

    @ParametersFactory
    public static Iterable<Object[]> modes() {
        return List.of(new Object[] { "optimized" }, new Object[] { "minimal" }, new Object[] { "transported" });
    }

    public NestedReductionsMetadataTests(@Name("mode") String mode) {
        this.mode = mode;
    }

    @Before
    public void onlySnapshotBuildsRegisterNested() {
        assumeTrue("The nested functions are registered on snapshot builds", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void logging() {
        LogConfigurator.configureESLogging();
        // Match normal server initialization; initializing IndexMode first exposes its IndexSettings cycle.
        IndexSettings.MODE.get(Settings.EMPTY);
    }

    public void testOverwritingSourceAndIndexDoesNotChangeReduction() throws Exception {
        var rows = run(
            "FROM orders | EVAL _source = \"user source\", _index = \"user index\" "
                + "| EVAL total = NESTED(SUM(items.price * items.quantity)) | KEEP order_id, _source, _index, total | SORT order_id"
        );
        assertEquals(List.of(row("a", "user source", "user index", 24L), row("b", "user source", "user index", null)), rows);
    }

    public void testBooleanCombinationsStayOnTheComputePath() throws Exception {
        // Existence is evaluated inside NestedReduce. Indexed nested-query pushdown is not used:
        // OR, NOT and multivalue children are not proven identical to a Lucene nested query.
        assertEquals(
            List.of(row("a")),
            run("FROM orders | WHERE NESTED_ANY(items.price > 5) OR NESTED_ANY(items.price > 100) | KEEP order_id")
        );
        assertEquals(List.of(row("b")), run("FROM orders | WHERE NOT NESTED_ANY(items.price > 5) | KEEP order_id | SORT order_id"));
    }

    public void testExistenceUsesOriginalSourceAfterPublicSourceIsReplaced() throws Exception {
        var rows = run("FROM orders | EVAL _source = \"user source\" | WHERE NESTED_ANY(items.price > 5) | KEEP order_id");
        assertEquals(List.of(row("a")), rows);
    }

    public void testReductionOutputCanReusePublicMetadataName() throws Exception {
        var rows = run(
            "FROM orders | EVAL _index = NESTED(SUM(items.price * items.quantity)) "
                + "| EVAL second = NESTED(SUM(items.price)) | KEEP order_id, _index, second | SORT order_id"
        );
        assertEquals(List.of(row("a", 24L, 12L), row("b", null, null)), rows);
    }

    public void testExpandedParentRowsKeepIndependentReductions() throws Exception {
        var rows = run(
            "FROM orders | EVAL copies = [10, 20] | MV_EXPAND copies "
                + "| EVAL total = NESTED(SUM(items.price * items.quantity)) | STATS n = COUNT(*), amount = SUM(total)"
        );
        assertEquals(List.of(row(4L, 48L)), rows);
    }

    public void testRenamingAndReplacingExplicitMetadataDoesNotChangeReduction() throws Exception {
        var rows = run(
            "FROM orders METADATA _index | RENAME _index AS origin | EVAL _index = \"replacement\" "
                + "| EVAL total = NESTED(SUM(items.price * items.quantity)) | KEEP order_id, origin, _index, total | SORT order_id"
        );
        assertEquals(List.of(row("a", "orders", "replacement", 24L), row("b", "orders", "replacement", null)), rows);
    }

    public void testDroppingExplicitMetadataDoesNotRemovePrivateInput() throws Exception {
        var rows = run(
            "FROM orders METADATA _index | DROP _index | EVAL total = NESTED(SUM(items.price * items.quantity)) "
                + "| KEEP order_id, total | SORT order_id"
        );
        assertEquals(List.of(row("a", 24L), row("b", null)), rows);
    }

    public void testKeepBeforeReductionRetainsOnlyPrivateDependencies() throws Exception {
        var rows = run("FROM orders | KEEP order_id | EVAL total = NESTED(SUM(items.price * items.quantity)) | SORT order_id");
        assertEquals(List.of(row("a", 24L), row("b", null)), rows);
    }

    public void testForkCanChangePublicMetadataIndependently() throws Exception {
        var rows = run(
            "FROM orders | FORK (EVAL _index = \"left\") (EVAL _index = \"right\") "
                + "| EVAL total = NESTED(SUM(items.price * items.quantity)) | STATS n = COUNT(*), amount = SUM(total)"
        );
        assertEquals(List.of(row(4L, 48L)), rows);
    }

    public void testNestedLookupMappingCannotRedirectLeftReduction() throws Exception {
        var rows = run(
            "FROM orders | LOOKUP JOIN prices ON order_id "
                + "| EVAL total = NESTED(SUM(items.price * items.quantity)) | KEEP order_id, total | SORT order_id",
            lookup("items")
        );
        assertEquals(List.of(row("a", 24L), row("b", null)), rows);
    }

    public void testLaterLookupMappingCannotChangeEarlierReduction() throws Exception {
        var rows = run(
            "FROM orders | EVAL total = NESTED(SUM(items.price * items.quantity)) "
                + "| LOOKUP JOIN prices ON order_id | KEEP order_id, total | SORT order_id",
            lookup("items")
        );
        assertEquals(List.of(row("a", 24L), row("b", null)), rows);
    }

    public void testDisjointNestedLookupMappingCannotRedirectLeftReduction() throws Exception {
        var rows = run(
            "FROM orders | LOOKUP JOIN prices ON order_id "
                + "| EVAL total = NESTED(SUM(items.price * items.quantity)) | KEEP order_id, total | SORT order_id",
            lookup("offers")
        );
        assertEquals(List.of(row("a", 24L), row("b", null)), rows);
    }

    public void testLookupSideNestedScopeIsRejectedExplicitly() {
        var error = assertThrows(
            VerificationException.class,
            () -> NestedReductionsTestSupport.analyze(
                "FROM orders | LOOKUP JOIN prices ON order_id | EVAL total = NESTED(SUM(offers.price))",
                schema(),
                lookup("offers")
            )
        );
        assertTrue(error.getMessage(), error.getMessage().contains("lookup-side nested scope"));
    }

    public void testLookupMappingTypeCannotChangeLeftArithmetic() throws Exception {
        var rows = run(
            "FROM orders | LOOKUP JOIN prices ON order_id "
                + "| EVAL total = NESTED(SUM(items.price * items.quantity)) | KEEP order_id, total | SORT order_id",
            lookup("items", "keyword")
        );
        assertEquals(List.of(row("a", 24L), row("b", null)), rows);
    }

    public void testLookupCannotProvideScopeWhenFromHasNoNestedMapping() {
        EsIndex original = schema();
        EsIndex plain = new EsIndex(
            original.name(),
            Map.of("order_id", original.mapping().get("order_id")),
            original.indexProperties(),
            original.originalIndices(),
            original.concreteIndices()
        );
        var error = assertThrows(
            VerificationException.class,
            () -> NestedReductionsTestSupport.analyze(
                "FROM orders | LOOKUP JOIN prices ON order_id | EVAL total = NESTED(SUM(offers.price))",
                plain,
                lookup("offers")
            )
        );
        assertTrue(error.getMessage(), error.getMessage().contains("lookup-side nested scope"));
    }

    public void testAggregationDoesNotRecreateParentAssociation() {
        assertThrows(
            VerificationException.class,
            () -> NestedReductionsTestSupport.analyze(
                "FROM orders | STATS n = COUNT(*) | EVAL total = NESTED(SUM(items.price * items.quantity))",
                schema(),
                Map.of()
            )
        );
    }

    public void testUserCannotForgePrivateMetadataAliases() {
        for (String role : List.of("_source", "_index")) {
            String name = "$$nested_reductions$metadata$" + role;
            for (String command : List.of(
                "EVAL `" + name + "` = \"forged\"",
                "RENAME order_id AS `" + name + "`",
                "EVAL text = \"forged\" | DISSECT text \"%{" + name + "}\""
            )) {
                var error = assertThrows(
                    VerificationException.class,
                    () -> NestedReductionsTestSupport.analyze(
                        "FROM orders | " + command + " | EVAL total = NESTED(SUM(items.price))",
                        schema(),
                        Map.of()
                    )
                );
                assertTrue(error.getMessage(), error.getMessage().contains("reserved internal metadata"));
            }
        }
    }

    private List<List<Object>> run(String query) throws Exception {
        return run(query, Map.of());
    }

    private List<List<Object>> run(String query, Map<String, IndexResolution> lookups) throws Exception {
        var analyzed = NestedReductionsTestSupport.analyze(query, schema(), lookups);
        assertTrue(analyzed.output().stream().noneMatch(a -> a.synthetic()));
        if (mode.equals("transported")) {
            analyzed = NestedReductionsTestSupport.roundTrip(query, analyzed);
        }
        return NestedReductionsTestSupport.runLocal(query, analyzed, relation -> 2, (relation, attr) -> {
            if (attr instanceof MetadataAttribute) {
                return switch (attr.name()) {
                    case "_source" -> List.of(
                        new BytesRef(
                            relation.indexProperties().containsKey("prices")
                                ? "{\"items\":[{\"price\":999,\"quantity\":2}],\"offers\":[{\"price\":999}]}"
                                : "{\"items\":[{\"price\":3,\"quantity\":2},{\"price\":9,\"quantity\":2}]}"
                        ),
                        new BytesRef(
                            relation.indexProperties().containsKey("prices")
                                ? "{\"items\":[{\"price\":999,\"quantity\":2}],\"offers\":[{\"price\":999}]}"
                                : "{\"items\":[]}"
                        )
                    );
                    case "_index" -> List.of(
                        new BytesRef(relation.indexProperties().keySet().iterator().next()),
                        new BytesRef(relation.indexProperties().keySet().iterator().next())
                    );
                    default -> throw new AssertionError(attr);
                };
            }
            if (attr.name().equals("order_id")) {
                return List.of(new BytesRef("a"), new BytesRef("b"));
            }
            throw new AssertionError(attr);
        }, true, mode.equals("minimal") == false);
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    private static EsIndex schema() {
        return schema("orders", "items");
    }

    private static Map<String, IndexResolution> lookup(String path) {
        return lookup(path, "long");
    }

    private static Map<String, IndexResolution> lookup(String path, String priceType) {
        EsIndex schema = schema("prices", path, priceType);
        return Map.of(
            "prices",
            IndexResolution.valid(
                new EsIndex(
                    schema.name(),
                    schema.mapping(),
                    Map.of("prices", new IndexProperties(IndexMode.LOOKUP, 1)),
                    schema.originalIndices(),
                    schema.concreteIndices()
                )
            )
        );
    }

    private static EsIndex schema(String index, String path) {
        return schema(index, path, "long");
    }

    private static EsIndex schema(String index, String path, String priceType) {
        Map<String, IndexFieldCapabilities> fields = new LinkedHashMap<>();
        Map.of("order_id", "keyword", path, "nested", path + ".price", priceType, path + ".quantity", "long")
            .forEach(
                (name, type) -> fields.put(
                    name,
                    new IndexFieldCapabilities(name, type, false, true, true, false, false, null, null, Map.of(), null, 0, false, false)
                )
            );
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
}
