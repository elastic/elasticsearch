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
        List<String> inputs = List.of("a", "b", "child1.grandchild", "child1.grandchild2", "child11", "child2.grandchild", "frog");
        Map<String, List<String>> partitioned = NestedUtils.partitionByChildren("", children, inputs, s-> s);
        assertEquals(
            Map.of("", List.of("a", "b", "child11", "frog"), "child1", List.of("child1.grandchild", "child1.grandchild2"), "child2", List.of("child2.grandchild")),
            partitioned
        );
    }

    public void testScopedPartitionByChild() {
        List<String> children = List.of("a.child1", "a.child2", "a.stepchild");
        List<String> inputs = List.of("a.a", "a.b", "a.child1.grandchild", "a.child1.grandchild2", "a.child11", "a.child2.grandchild", "a.frog");
        Map<String, List<String>> partitioned = NestedUtils.partitionByChildren("a", children, inputs, s -> s);
        assertEquals(
            Map.of("a", List.of("a.a", "a.b", "a.child11", "a.frog"), "a.child1", List.of("a.child1.grandchild", "a.child1.grandchild2"), "a.child2", List.of("a.child2.grandchild")),
            partitioned
        );
    }

}
