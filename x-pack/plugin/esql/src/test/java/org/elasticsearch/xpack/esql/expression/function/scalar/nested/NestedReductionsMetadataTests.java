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
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The nested scope comes from the rows of the FROM index, never from a lookup index or from rows a STATS made. The
 * 303_nested_metadata YAML tests run the accepted shapes against real indices.
 */
public class NestedReductionsMetadataTests extends LuceneTestCase {
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
        var error = assertThrows(
            VerificationException.class,
            () -> NestedReductionsTestSupport.analyze(
                "FROM orders | STATS n = COUNT(*) | EVAL total = NESTED(SUM(items.price * items.quantity))",
                schema(),
                Map.of()
            )
        );
        assertTrue(error.getMessage(), error.getMessage().contains("cannot follow [STATS n = COUNT(*)]"));
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
