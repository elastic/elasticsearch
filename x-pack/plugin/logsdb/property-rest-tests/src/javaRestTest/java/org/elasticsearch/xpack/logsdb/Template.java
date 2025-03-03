/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Tuple;

import org.elasticsearch.logsdb.datageneration.FieldType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public record Template(Map<String, Entry> template) {
    public sealed interface Entry permits Leaf, Object {}

    public record Leaf(String name, FieldType type) implements Entry {}

    public record Object(String name, boolean nested, Map<String, Entry> children) implements Entry {}

    public static Arbitrary<Template> generate(int maxDepth, int maxChildren) {
        var topObject = object(maxDepth, maxChildren, false);

        return topObject.map(o -> new Template(o.children()));
    }

    private static Arbitrary<Object> object(int maxDepth, int maxChildren, boolean nested) {
        var childCount = Arbitraries.integers().between(0, maxChildren);

        // We are at the bottom, leafs only here
        if (maxDepth == 0) {
            var leafFields = childCount.flatMap(c -> fieldName().list().uniqueElements().ofSize(c))
                .flatMap(names -> Combinators.combine(names.stream().map(Template::leaf).toList()).as(Function.identity()));

            return Combinators.combine(fieldName(), leafFields).as((name, ls) -> {
                var children = new HashMap<String, Entry>();
                for (var l : ls) {
                    children.put(l.name(), l);
                }

                return new Object(name, nested, children);
            });
        }

        Arbitrary<List<String>> childTypes = childCount.flatMap(c -> childType().list().ofSize(c));

        var leafsAtThisLevel = childTypes.flatMap(types -> {
            var count = types.stream().filter(t -> t.equals("leaf")).count();
            return fieldName().list()
                .uniqueElements()
                .ofSize((int) count)
                .flatMap(names -> Combinators.combine(names.stream().map(Template::leaf).toList()).as(Function.identity()));
        });
        var objectsAtThisLevel = childTypes.flatMap(types -> {
            var count = types.stream().filter(t -> t.equals("object")).count();
            return object(maxDepth - 1, maxChildren, false).list().ofSize((int) count);
        });
        var nestedAtThisLevel = childTypes.flatMap(types -> {
            var count = types.stream().filter(t -> t.equals("nested")).count();
            return object(maxDepth - 1, maxChildren, true).list().ofSize((int) count);
        });

        return Combinators.combine(fieldName(), leafsAtThisLevel, objectsAtThisLevel, nestedAtThisLevel).as((name, ls, os, ns) -> {
            var children = new HashMap<String, Entry>();
            for (var l : ls) {
                children.put(l.name(), l);
            }
            for (var o : os) {
                children.put(o.name(), o);
            }
            for (var n : ns) {
                children.put(n.name(), n);
            }

            return new Object(name, nested, children);
        });
    }

    private static Arbitrary<Leaf> leaf(String name) {
        var type = Arbitraries.of(FieldType.values());

        return type.map(t -> new Leaf(name, t));
    }

    private static Arbitrary<String> fieldName() {
        return Arbitraries.strings().ofLength(5).withCharRange('a', 'z');
    }

    private static Arbitrary<String> childType() {
        // TODO enum this
        return Arbitraries.frequency(Tuple.of(10, "object"), Tuple.of(10, "nested"), Tuple.of(80, "leaf"));
    }
}
