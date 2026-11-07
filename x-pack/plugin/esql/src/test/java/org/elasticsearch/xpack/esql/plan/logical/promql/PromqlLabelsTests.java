/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** The labels a PromQL node's residual metadata leaves out, as its output declares them. */
public class PromqlLabelsTests extends ESTestCase {

    /** A label name may contain the separator, so leaving out {@code a$b} differs from leaving out {@code a} and {@code b}. */
    public void testMetadataViewNamesAreDistinct() {
        List<Set<String>> views = List.of(
            Set.of(),
            Set.of("a"),
            Set.of("b"),
            Set.of("a", "b"),
            Set.of("a$b"),
            Set.of("a\\"),
            Set.of("a\\", "b"),
            Set.of("a\\$b"),
            Set.of("$"),
            Set.of("$", "a")
        );
        var names = new HashSet<String>();
        for (Set<String> view : views) {
            assertTrue(view.toString(), names.add(PromqlLabels.metadataLabelName(view)));
        }
        assertEquals("_timeseries$pod", PromqlLabels.metadataLabelName(Set.of("pod")));
    }
}
