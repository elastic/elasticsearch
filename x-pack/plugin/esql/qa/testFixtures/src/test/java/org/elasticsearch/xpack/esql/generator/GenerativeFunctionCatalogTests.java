/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class GenerativeFunctionCatalogTests extends ESTestCase {

    public void testEquivalentTypesDateDatetime() {
        assertThat(GenerativeFunctionCatalog.equivalentTypes("date"), containsInAnyOrder("date", "datetime"));
        assertThat(GenerativeFunctionCatalog.equivalentTypes("datetime"), containsInAnyOrder("date", "datetime"));
    }

    public void testEquivalentTypesKeywordAcceptsText() {
        // text can be used wherever keyword is expected, but not vice versa
        assertThat(GenerativeFunctionCatalog.equivalentTypes("keyword"), containsInAnyOrder("keyword", "text"));
        assertThat(GenerativeFunctionCatalog.equivalentTypes("text"), equalTo(Set.of("text")));
    }

    public void testEquivalentTypesUnrelatedTypeIsIdentity() {
        assertThat(GenerativeFunctionCatalog.equivalentTypes("integer"), equalTo(Set.of("integer")));
        assertThat(GenerativeFunctionCatalog.equivalentTypes("double"), equalTo(Set.of("double")));
        assertThat(GenerativeFunctionCatalog.equivalentTypes("boolean"), equalTo(Set.of("boolean")));
    }

    public void testScalarsReturningDateAndDatetimeAreEquivalent() {
        GenerativeFunctionCatalog catalog = GenerativeFunctionCatalog.getInstance();
        // Functions indexed under "date" in Kibana JSONs must be reachable via "datetime" too
        assertFalse("expected functions returning date/datetime", catalog.scalarsReturning("datetime").isEmpty());
        assertFalse("expected functions returning date/datetime", catalog.scalarsReturning("date").isEmpty());
        assertThat(catalog.scalarsReturning("date"), equalTo(catalog.scalarsReturning("datetime")));
    }

    public void testScalarsReturningLoadsAtLeastSomeFunctions() {
        GenerativeFunctionCatalog catalog = GenerativeFunctionCatalog.getInstance();
        assertFalse("expected some integer-returning functions", catalog.scalarsReturning("integer").isEmpty());
        assertFalse("expected some keyword-returning functions", catalog.scalarsReturning("keyword").isEmpty());
        // Excluded functions must not appear
        for (String excluded : GenerativeFunctionCatalog.EXCLUDED_FUNCTIONS) {
            boolean found = catalog.scalarsReturning("keyword").stream().anyMatch(f -> f.name().equals(excluded))
                || catalog.scalarsReturning("integer").stream().anyMatch(f -> f.name().equals(excluded));
            assertFalse("excluded function '" + excluded + "' must not appear in catalog", found);
        }
    }

    public void testSatisfiableSignaturesWithEquivalentColumnType() {
        GenerativeFunctionCatalog catalog = GenerativeFunctionCatalog.getInstance();
        // A column set with "datetime" should satisfy signatures that require "date"
        Set<String> columnTypes = Set.of("datetime", "integer");
        long satisfiable = catalog.scalarsReturning("datetime")
            .stream()
            .flatMap(fn -> catalog.satisfiableSignatures(fn, columnTypes).stream())
            .count();
        assertThat("expected at least one satisfiable signature for datetime columns", satisfiable, not(equalTo(0L)));
    }

    /** Each catalog function has an {@code fn_<name>} capability. */
    public void testEveryFunctionHasClusterCapability() {
        Set<String> capabilities = EsqlCapabilities.capabilities(new EsqlFunctionRegistry(), true).capabilities();
        for (GenerativeFunctionDefinition fn : GenerativeFunctionCatalog.all()) {
            assertThat(fn.name(), capabilities, hasItem(GenerativeFunctionCatalog.capabilityName(fn)));
        }
    }

    /** A directory load keeps that directory's scalar signatures. */
    public void testLoadFromDefinitionsDir() throws IOException {
        Path dir = createTempDir();
        Files.writeString(dir.resolve("to_range.json"), """
            {"name": "to_range", "type": "scalar", "signatures": [
              {"params": [{"name": "from", "type": "date", "optional": false}, {"name": "to", "type": "date", "optional": false}],
               "returnType": "date_range", "variadic": false}]}""");
        Files.writeString(dir.resolve("match.json"), """
            {"name": "match", "type": "scalar", "signatures": [
              {"params": [{"name": "field", "type": "keyword", "optional": false}], "returnType": "boolean", "variadic": false}]}""");
        Files.writeString(dir.resolve("count.json"), """
            {"name": "count", "type": "agg", "signatures": [
              {"params": [{"name": "field", "type": "keyword", "optional": false}], "returnType": "long", "variadic": false}]}""");

        List<GenerativeFunctionDefinition> loaded = GenerativeFunctionCatalog.loadFromDefinitionsDir(dir.toString());

        assertThat(loaded.stream().map(GenerativeFunctionDefinition::name).toList(), equalTo(List.of("to_range")));
        List<GenerativeFunctionSignature> signatures = loaded.getFirst().signatures();
        assertThat(signatures.size(), equalTo(1));
        assertThat(signatures.getFirst().params().stream().map(GenerativeFunctionParam::type).toList(), equalTo(List.of("date", "date")));
        assertThat(signatures.getFirst().returnType(), equalTo("date_range"));
    }

    /** An empty or missing directory fails. */
    public void testLoadFromDefinitionsDirWithoutDefinitions() {
        Path empty = createTempDir();
        expectThrows(IllegalStateException.class, () -> GenerativeFunctionCatalog.loadFromDefinitionsDir(empty.toString()));
        String missing = empty.resolve("missing").toString();
        expectThrows(IllegalStateException.class, () -> GenerativeFunctionCatalog.loadFromDefinitionsDir(missing));
    }
}
