/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.any;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.sub;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class TranslationConstraintTests extends ESTestCase {

    public void testUnionMergesNamesAndOneOpenSelection() {
        TranslationConstraint required = union(of("cluster"), sub(any(), of("pod")), sub(any(), of("pod")), of("cluster", "region"));

        assertThat(required.names(), contains("cluster", "region"));
        assertThat(required.excluded(), equalTo(Set.of("pod")));
        assertThat(union(required, of()), equalTo(required));
        assertThat(union(of(), required), equalTo(required));
        assertTrue(of().isEmpty());
        assertFalse(of().isOpen());
        assertTrue(required.isOpen());
        assertFalse(of("cluster").isOpen());
        assertTrue(any().isOpen());
        assertThat(any().names(), empty());
        assertThat(any().excluded(), equalTo(Set.of()));
    }

    public void testSubDropsNamesAndWidensComplements() {
        TranslationConstraint above = union(of("cluster", "pod"), sub(any(), of("region")));

        TranslationConstraint below = sub(above, of("pod"));

        assertThat(below.names(), contains("cluster"));
        assertThat(below.excluded(), equalTo(Set.of("region", "pod")));
        assertTrue(below.excludes(Set.of("pod")));
        assertFalse(above.excludes(Set.of("pod")));
        // two complements widened onto the same exclusion set are one
        TranslationConstraint merged = sub(union(sub(any(), of("pod")), any()), of("pod"));
        assertThat(merged.excluded(), equalTo(Set.of("pod")));
        // subtracting nothing changes nothing
        assertThat(sub(above, of()), equalTo(above));
    }

    public void testSubIsSetDifferenceOverOpenConstraintsToo() {
        // everything less everything is nothing, names included
        assertThat(sub(any(), any()), equalTo(of()));
        assertThat(sub(union(of("cluster"), any()), any()), equalTo(of()));
        // everything less "everything but x" is exactly x
        assertThat(sub(any(), sub(any(), of("pod"))), equalTo(of("pod")));
        assertThat(sub(any(), sub(any(), of("pod", "region"))), equalTo(of("pod", "region")));
        // a complement that already excludes x cannot deliver it back
        assertThat(sub(sub(any(), of("pod")), sub(any(), of("pod", "region"))), equalTo(of("region")));
        // a name the open side of b covers goes, a name it leaves out stays
        assertThat(sub(of("cluster", "pod"), sub(any(), of("pod"))), equalTo(of("pod")));
        // nothing less anything is nothing
        assertThat(sub(of(), any()), equalTo(of()));
    }

    public void testWithoutIsTheSameOperationDownAndUp() {
        // what `without (pod)` requires of its child, given what is required of it
        TranslationConstraint required = union(of("cluster", "pod"), sub(any(), of("region")));
        TranslationConstraint below = sub(union(any(), required), of("pod"));
        assertThat(below.names(), contains("cluster"));
        assertThat(below.excluded(), equalTo(Set.of("pod")));
        assertTrue(below.excludes(Set.of("pod")));
        // and what it groups by, given what the child delivered: the same labels, so the same expression
        TranslationConstraint delivered = union(of("cluster"), sub(any(), of("pod")));
        assertThat(sub(delivered, of("pod")), equalTo(delivered));
    }

    public void testIncomparableOpenSelectionsDoNotCreateMultipleRecords() {
        assertEquals(any(), union(sub(any(), of("a")), sub(any(), of("b"))));
        assertEquals(sub(any(), of("a")), union(sub(any(), of("a", "b")), sub(any(), of("a", "c"))));
    }

    public void testEqualityIgnoresOrder() {
        assertThat(sub(any(), of("pod", "region")), equalTo(sub(any(), of("region", "pod"))));
        assertThat(of("a", "b"), equalTo(of("b", "a")));
        assertThat(union(of("a"), any()), equalTo(union(any(), of("a"))));
    }

    public void testTermsAreImmutable() {
        expectThrows(UnsupportedOperationException.class, () -> of("cluster").names().clear());
        expectThrows(UnsupportedOperationException.class, () -> any().excluded().clear());
        expectThrows(UnsupportedOperationException.class, () -> sub(any(), of("pod")).excluded().clear());
    }

    /** Named reads can also occur in the open selection's exclusions; set operations must respect the union of both. */
    public void testSelectionAlgebraExhaustively() {
        List<String> names = List.of("pod", "service.name", "地域");
        var selections = new ArrayList<TranslationConstraint>();
        for (int named = 0; named < 1 << names.size(); named++) {
            Set<String> explicit = subset(names, named);
            selections.add(of(explicit));
            for (int excluded = 0; excluded < 1 << names.size(); excluded++) {
                selections.add(new TranslationConstraint(explicit, subset(names, excluded)));
            }
        }
        // An unseen name distinguishes an open selection from an enumeration of every known label.
        var universe = new LinkedHashSet<>(names);
        universe.add("not_known_to_the_planner");
        for (var left : selections) {
            for (var right : selections) {
                Set<String> expectedUnion = selected(left, universe);
                expectedUnion.addAll(selected(right, universe));
                assertEquals(expectedUnion, selected(union(left, right), universe));
                Set<String> expectedDifference = selected(left, universe);
                expectedDifference.removeAll(selected(right, universe));
                assertEquals(expectedDifference, selected(sub(left, right), universe));
            }
        }
    }

    private static Set<String> selected(TranslationConstraint selection, Set<String> universe) {
        var selected = new LinkedHashSet<String>();
        for (String name : universe) {
            if (selection.excludes(Set.of(name)) == false) selected.add(name);
        }
        return selected;
    }

    private static Set<String> subset(List<String> names, int mask) {
        var subset = new LinkedHashSet<String>();
        for (int i = 0; i < names.size(); i++) {
            if ((mask & (1 << i)) != 0) subset.add(names.get(i));
        }
        return subset;
    }
}
