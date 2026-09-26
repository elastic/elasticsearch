/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.any;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.sub;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class TranslationConstraintTests extends ESTestCase {

    public void testUnionMergesNamesAndComplements() {
        TranslationConstraint required = union(of("cluster"), sub(any(), of("pod")), sub(any(), of("pod")), of("cluster", "region"));

        assertThat(required.names(), contains("cluster", "region"));
        assertThat(required.allBut(), contains(Set.of("pod")));
        assertThat(union(required, of()), equalTo(required));
        assertThat(union(of(), required), equalTo(required));
        assertTrue(of().isEmpty());
        assertFalse(of().isOpen());
        assertTrue(required.isOpen());
        assertFalse(of("cluster").isOpen());
        assertTrue(any().isOpen());
        assertThat(any().names(), empty());
        assertThat(any().allBut(), contains(Set.of()));
    }

    public void testSubDropsNamesAndWidensComplements() {
        TranslationConstraint above = union(of("cluster", "pod"), sub(any(), of("region")));

        TranslationConstraint below = sub(above, of("pod"));

        assertThat(below.names(), contains("cluster"));
        assertThat(below.allBut(), contains(Set.of("region", "pod")));
        assertTrue(below.excludes(Set.of("pod")));
        assertFalse(above.excludes(Set.of("pod")));
        // two complements widened onto the same exclusion set are one
        TranslationConstraint merged = sub(union(sub(any(), of("pod")), any()), of("pod"));
        assertThat(merged.allBut(), contains(Set.of("pod")));
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
        assertThat(below.allBut(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
        assertTrue(below.excludes(Set.of("pod")));
        // and what it groups by, given what the child delivered: the same labels, so the same expression
        TranslationConstraint delivered = union(of("cluster"), sub(any(), of("pod")));
        assertThat(sub(delivered, of("pod")), equalTo(delivered));
    }

    public void testEqualityIgnoresOrder() {
        assertThat(sub(any(), of("pod", "region")), equalTo(sub(any(), of("region", "pod"))));
        assertThat(of("a", "b"), equalTo(of("b", "a")));
        assertThat(union(of("a"), any()), equalTo(union(any(), of("a"))));
    }

    public void testTermsAreImmutable() {
        expectThrows(UnsupportedOperationException.class, () -> of("cluster").names().clear());
        expectThrows(UnsupportedOperationException.class, () -> any().allBut().clear());
        expectThrows(UnsupportedOperationException.class, () -> sub(any(), of("pod")).allBut().iterator().next().clear());
    }
}
