/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class FieldPathFilterTests extends ESTestCase {

    public void testFilteredCopyKeepsOnlyFilteredTopLevelProperties() {
        Map<String, Object> root = Map.of(
            "dynamic",
            "false",
            "properties",
            Map.of("a", Map.of("type", "keyword"), "b", Map.of("type", "keyword"), "obj", Map.of("properties", Map.of("x", Map.of())))
        );

        Map<String, Object> copy = FieldPathFilter.forPatterns(List.of("a", "obj.x")).filteredCopy(root);

        assertThat(copy.get("dynamic"), equalTo("false"));
        @SuppressWarnings("unchecked")
        Map<String, Object> copiedProperties = (Map<String, Object>) copy.get("properties");
        assertThat(copiedProperties.keySet(), equalTo(Set.of("a", "obj")));
        assertThat(copiedProperties.get("a"), equalTo(Map.of("type", "keyword")));
        assertThat(root.size(), equalTo(2));
    }

    public void testFilteredCopyDropsTopLevelFieldsThatCannotLeadToAWantedOne() {
        Map<String, Object> root = Map.of(
            "properties",
            Map.of(
                "leaf",
                Map.of("type", "keyword"),
                "withRaw",
                Map.of("type", "text", "fields", Map.of("raw", Map.of("type", "keyword"))),
                "obj",
                Map.of("properties", Map.of("foo", Map.of("type", "keyword")))
            )
        );

        Map<String, Object> copy = FieldPathFilter.forPatterns(List.of("*.raw")).filteredCopy(root);

        @SuppressWarnings("unchecked")
        Map<String, Object> copiedProperties = (Map<String, Object>) copy.get("properties");
        assertThat(copiedProperties.keySet(), equalTo(Set.of("withRaw", "obj")));
    }

    public void testFilteredCopyPrunesThePropertiesOfObjectAndNestedFields() {
        Map<String, Object> root = Map.of(
            "properties",
            Map.of(
                "obj",
                Map.of("type", "object", "properties", Map.of("x", Map.of("type", "long"), "y", Map.of("type", "long"))),
                "untyped",
                Map.of("properties", Map.of("x", Map.of("type", "long"), "y", Map.of("type", "long"))),
                "nest",
                Map.of(
                    "type",
                    "nested",
                    "properties",
                    Map.of("x", Map.of("type", "long"), "inner", Map.of("properties", Map.of("x", Map.of(), "y", Map.of())))
                )
            )
        );

        Map<String, Object> copy = FieldPathFilter.forPatterns(List.of("*.x")).filteredCopy(root);

        assertThat(properties(property(copy, "obj")).keySet(), equalTo(Set.of("x")));
        assertThat(properties(property(copy, "untyped")).keySet(), equalTo(Set.of("x")));
        Map<String, Object> nest = property(copy, "nest");
        assertThat(properties(nest).keySet(), equalTo(Set.of("x", "inner")));
        assertThat(properties(property(nest, "inner")).keySet(), equalTo(Set.of("x")));
    }

    public void testFilteredCopyDoesNotPruneOtherTypesWithProperties() {
        Map<String, Object> flattened = Map.of("type", "flattened", "properties", Map.of("one", Map.of(), "two", Map.of()));
        Map<String, Object> withMeta = Map.of("type", "keyword", "meta", Map.of("properties", Map.of("one", "1", "two", "2")));
        Map<String, Object> root = Map.of("properties", Map.of("flat", flattened, "kw", withMeta));

        Map<String, Object> copy = FieldPathFilter.forPatterns(List.of("flat.one", "kw")).filteredCopy(root);

        assertThat(property(copy, "flat"), equalTo(flattened));
        assertThat(property(copy, "kw"), equalTo(withMeta));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> properties(Map<String, Object> node) {
        return (Map<String, Object>) node.get("properties");
    }

    private static Map<String, Object> property(Map<String, Object> node, String name) {
        return properties(node).get(name) instanceof Map<?, ?> property ? castToMap(property) : null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castToMap(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }

    public void testPatternOfOnlyWildcardsMatchesAll() {
        assertThat(FieldPathFilter.forPatterns(List.of("*")), sameInstance(FieldPathFilter.matchAll()));
        assertThat(FieldPathFilter.forPatterns(List.of("a", "**")), sameInstance(FieldPathFilter.matchAll()));
        assertThat(FieldPathFilter.forPatterns(List.of("a", "*b")), not(sameInstance(FieldPathFilter.matchAll())));
        assertThat(FieldPathFilter.forPatterns(List.of("*.b")), not(sameInstance(FieldPathFilter.matchAll())));
        assertThat(FieldPathFilter.forPatterns(List.of("a", "b*")), not(sameInstance(FieldPathFilter.matchAll())));
    }

    public void testMatchAllKeepsEverythingWithoutTrackingPosition() {
        FieldPathFilter filter = FieldPathFilter.matchAll();

        assertTrue(filter.enter("a", Map.of()));
        assertTrue(filter.enter("b", Map.of()));
        filter.exit();
        filter.exit();
        filter.exit();
    }

    public void testMatchAllCopyIsIndependentOfTheOriginal() {
        Map<String, Object> root = Map.of("properties", Map.of("a", Map.of("type", "keyword"), "b", Map.of("type", "long")));

        Map<String, Object> copy = FieldPathFilter.matchAll().filteredCopy(root);

        assertThat(copy, equalTo(root));
        assertThat(copy.get("properties"), not(sameInstance(root.get("properties"))));
    }
}
