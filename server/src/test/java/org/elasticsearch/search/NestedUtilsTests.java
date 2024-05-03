/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class NestedUtilsTests extends ESTestCase {

    public void testPartitionByChild() {
        List<String> children = List.of("child1", "child2", "stepchild");
        List<String> inputs = List.of(
            "a",
            "b",
            "child1.grandchild",
            "child1.grandchild2",
            "child11",
            "child12",
            "child2.grandchild",
            "frog"
        );
        Map<String, List<String>> partitioned = NestedUtils.partitionByChildren("", children, inputs, s -> s);
        assertEquals(
            Map.of(
                "",
                List.of("a", "b", "child11", "child12", "frog"),
                "child1",
                List.of("child1.grandchild", "child1.grandchild2"),
                "child2",
                List.of("child2.grandchild"),
                "stepchild",
                List.of()
            ),
            partitioned
        );
    }

    public void testScopedPartitionByChild() {
        List<String> children = List.of("a.child1", "a.child2", "a.stepchild");
        List<String> inputs = List.of(
            "a.a",
            "a.b",
            "a.child1.grandchild",
            "a.child1.grandchild2",
            "a.child11",
            "a.child2.grandchild",
            "a.frog"
        );
        Map<String, List<String>> partitioned = NestedUtils.partitionByChildren("a", children, inputs, s -> s);
        assertEquals(
            Map.of(
                "a",
                List.of("a.a", "a.b", "a.child11", "a.frog"),
                "a.child1",
                List.of("a.child1.grandchild", "a.child1.grandchild2"),
                "a.child2",
                List.of("a.child2.grandchild"),
                "a.stepchild",
                List.of()
            ),
            partitioned
        );
    }

    public void testScopedPartitionWithMultifields() {
        List<String> children = List.of("user.address");
        List<String> inputs = List.of("user.address.city", "user.address.zip", "user.first", "user.last", "user.last.keyword");
        Map<String, List<String>> partitioned = NestedUtils.partitionByChildren("user", children, inputs, s -> s);
        assertEquals(
            Map.of(
                "user",
                List.of("user.first", "user.last", "user.last.keyword"),
                "user.address",
                List.of("user.address.city", "user.address.zip")
            ),
            partitioned
        );
    }

    public void testEmptyCases() {
        // No children, everything gets mapped under the scope
        assertEquals(Map.of("scope", List.of("foo")), NestedUtils.partitionByChildren("scope", List.of(), List.of("foo"), s -> s));
        // No inputs, we get an empty map under the scope
        assertEquals(
            Map.of("scope", List.of(), "scope.child", List.of()),
            NestedUtils.partitionByChildren("scope", List.of("scope.child"), List.<String>of(), s -> s)
        );
    }

}
