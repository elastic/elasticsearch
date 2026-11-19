/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.DocumentParserContext.CachedBuilders;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class CachedBuildersTests extends ESTestCase {

    private static Map<String, Object> copyOfPathTo(String fieldPath, Map<String, Object> root) {
        return CachedBuilders.copyOfPathTo(fieldPath, null, root);
    }

    public void testCopyKeepsOnlyTheWayToTheField() {
        Map<String, Object> root = Map.of(
            "dynamic",
            "false",
            "properties",
            Map.of(
                "a",
                Map.of("type", "keyword"),
                "obj",
                Map.of("properties", Map.of("x", Map.of("type", "long"), "y", Map.of("type", "long"))),
                "other",
                Map.of("properties", Map.of("x", Map.of("type", "long")))
            )
        );

        Map<String, Object> copy = copyOfPathTo("obj.x", root);

        assertThat(copy.get("dynamic"), equalTo("false"));
        assertThat(properties(copy).keySet(), equalTo(Set.of("obj")));
        assertThat(properties(property(copy, "obj")).keySet(), equalTo(Set.of("x")));
        assertThat(root.size(), equalTo(2));
    }

    public void testCopyOmitsRuntimeFieldsAndDynamicTemplatesOfTheRoot() {
        Map<String, Object> root = Map.of(
            "dynamic",
            "false",
            "runtime",
            Map.of("rt", Map.of("type", "keyword")),
            "dynamic_templates",
            List.of(Map.of("strings", Map.of("match_mapping_type", "string", "mapping", Map.of("type", "keyword")))),
            "properties",
            Map.of("a", Map.of("type", "keyword"))
        );

        Map<String, Object> copy = copyOfPathTo("a", root);

        assertThat(copy.keySet(), equalTo(Set.of("dynamic", "properties")));
    }

    public void testCopyKeepsEntriesCalledLikeThoseOfTheRootBelowIt() {
        Map<String, Object> obj = Map.of(
            "runtime",
            Map.of("kept", "yes"),
            "properties",
            Map.of("x", Map.of("type", "long"), "meta", Map.of("type", "keyword"))
        );
        Map<String, Object> root = Map.of(
            "properties",
            Map.of("obj", Map.of("type", "object", "dynamic_templates", List.of()), "obj2", obj)
        );

        assertThat(property(copyOfPathTo("obj.x", root), "obj").keySet(), equalTo(Set.of("type", "dynamic_templates")));
        assertThat(property(copyOfPathTo("obj2.x", root), "obj2").keySet(), equalTo(Set.of("runtime", "properties")));
    }

    public void testCopyLooksUpNamesWithDots() {
        Map<String, Object> root = Map.of(
            "properties",
            Map.of(
                "a.b",
                Map.of("type", "keyword"),
                "a",
                Map.of("properties", Map.of("b", Map.of("type", "keyword"), "c", Map.of("type", "keyword"))),
                "z",
                Map.of("type", "keyword")
            )
        );

        Map<String, Object> copy = copyOfPathTo("a.b", root);

        assertThat(properties(copy).keySet(), equalTo(Set.of("a.b", "a")));
        assertThat(properties(property(copy, "a")).keySet(), equalTo(Set.of("b")));
    }

    public void testCopyPrunesThePropertiesOfObjectAndNestedFields() {
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

        assertThat(properties(property(copyOfPathTo("obj.x", root), "obj")).keySet(), equalTo(Set.of("x")));
        assertThat(properties(property(copyOfPathTo("untyped.y", root), "untyped")).keySet(), equalTo(Set.of("y")));
        Map<String, Object> nest = property(copyOfPathTo("nest.inner.y", root), "nest");
        assertThat(properties(nest).keySet(), equalTo(Set.of("inner")));
        assertThat(properties(property(nest, "inner")).keySet(), equalTo(Set.of("y")));
    }

    public void testCopyPrunesThePropertiesOfPassthroughFields() {
        Map<String, Object> root = Map.of(
            "properties",
            Map.of(
                "labels",
                Map.of(
                    "type",
                    "passthrough",
                    "priority",
                    0,
                    "properties",
                    Map.of("a", Map.of("type", "keyword"), "b", Map.of("type", "keyword"))
                )
            )
        );

        Map<String, Object> labels = property(copyOfPathTo("labels.a", root), "labels");

        assertThat(properties(labels).keySet(), equalTo(Set.of("a")));
        assertThat(labels.get("type"), equalTo("passthrough"));
        assertThat(labels.get("priority"), equalTo(0));
    }

    public void testCopyDoesNotPruneOtherTypesWithProperties() {
        Map<String, Object> flattened = Map.of("type", "flattened", "properties", Map.of("one", Map.of(), "two", Map.of()));
        Map<String, Object> withMeta = Map.of("type", "keyword", "meta", Map.of("properties", Map.of("one", "1", "two", "2")));
        Map<String, Object> root = Map.of("properties", Map.of("flat", flattened, "kw", withMeta));

        assertThat(property(copyOfPathTo("flat.one", root), "flat"), equalTo(flattened));
        assertThat(property(copyOfPathTo("kw", root), "kw"), equalTo(withMeta));
    }

    public void testCopyKeepsTheParentOfAMultiField() {
        Map<String, Object> root = Map.of(
            "properties",
            Map.of(
                "text",
                Map.of("type", "text", "fields", Map.of("raw", Map.of("type", "keyword", "fields", Map.of("sort", Map.of())))),
                "other",
                Map.of("type", "text", "fields", Map.of("raw", Map.of("type", "keyword")))
            )
        );

        for (String fieldPath : new String[] { "text.raw", "text.raw.sort" }) {
            Map<String, Object> copy = copyOfPathTo(fieldPath, root);

            assertThat(properties(copy).keySet(), equalTo(Set.of("text")));
            assertThat(property(copy, "text"), equalTo(property(root, "text")));
        }
    }

    public void testCopyOfAnObjectIsWholeWhenItIsTheFieldPath() {
        Map<String, Object> obj = Map.of("properties", Map.of("x", Map.of("type", "long"), "y", Map.of("type", "long")));
        Map<String, Object> root = Map.of("properties", Map.of("obj", obj));

        assertThat(property(copyOfPathTo("obj", root), "obj"), equalTo(obj));
    }

    public void testCopyIsIndependentOfTheOriginal() {
        Map<String, Object> root = Map.of("properties", Map.of("a", Map.of("type", "keyword", "meta", Map.of("k", "v"))));

        Map<String, Object> copy = copyOfPathTo("a", root);

        assertThat(copy, equalTo(root));
        assertThat(property(copy, "a"), not(sameInstance(property(root, "a"))));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> properties(Map<String, Object> node) {
        return (Map<String, Object>) node.get("properties");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> property(Map<String, Object> node, String name) {
        return (Map<String, Object>) properties(node).get(name);
    }
}
