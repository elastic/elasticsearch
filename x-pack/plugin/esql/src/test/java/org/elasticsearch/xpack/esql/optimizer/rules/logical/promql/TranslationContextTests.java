/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.Header;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.finite;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.open;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class TranslationContextTests extends ESTestCase {

    public void testUnionMergesLabelsAndSkipSets() {
        Header header = finite(List.of("cluster")).union(TranslationContext.open(Set.of("pod")))
            .union(TranslationContext.open(Set.of("pod")))
            .union(finite(List.of("cluster", "region")));

        assertThat(header.labels(), contains("cluster", "region"));
        assertThat(header.skips(), contains(Set.of("pod")));
        assertThat(header.union(Header.EMPTY), equalTo(header));
    }

    public void testSubtractDropsLabelsAndWidensSkipSets() {
        Header above = finite(List.of("cluster", "pod")).union(TranslationContext.open(Set.of("region")));

        Header below = above.subtract(List.of("pod"));
        assertThat(below.labels(), contains("cluster"));
        assertThat(below.skips(), contains(Set.of("region", "pod")));

        // the regroup's own column composes as a second, finer skip set
        Header child = below.union(TranslationContext.open(Set.of("pod")));
        assertThat(child.skips(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
        assertThat(child.finestSkip(), equalTo(Set.of("pod")));
    }

    public void testIntersectIsTheUpwardCounterpartOfSubtract() {
        Header required = finite(List.of("cluster", "pod")).union(TranslationContext.open(Set.of("region")));
        Header child = required.subtract(List.of("pod")).union(TranslationContext.open(Set.of("pod")));

        Header lifted = child.intersect(List.of("pod"));

        // every column the parent required, apart from the dropped label, comes back; so does the regroup's own
        // packing, which already excludes the dropped label and fixes the grain of the result
        assertThat(lifted.labels(), contains("cluster"));
        assertThat(lifted.skips(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
        // the regroup's own full label space does not survive dropping a label it still carries
        assertFalse(open().intersect(List.of("pod")).isOpen());
        // without () keeps everything
        assertThat(child.intersect(List.of()), equalTo(child));
    }

    public void testProjectKeepsSkipSets() {
        Header header = finite(List.of("cluster", "pod", "region")).union(TranslationContext.open(Set.of("pod")));

        Header retained = header.project(List.of("cluster", "missing"));

        assertThat(retained.labels(), contains("cluster"));
        assertThat(retained.skips(), contains(Set.of("pod")));
    }

    public void testPackedNameDerivesFromTheSkipSet() {
        assertThat(TranslationContext.mapOpen(Set.of()), equalTo(MetadataAttribute.TIMESERIES));
        assertThat(TranslationContext.mapOpen(Set.of("region", "pod")), equalTo(MetadataAttribute.TIMESERIES + "$pod$region"));
        assertThat(TranslationContext.mapOpen(Set.of("pod", "region")), equalTo(TranslationContext.mapOpen(Set.of("region", "pod"))));
    }

    public void testFindByNameMatchesCanonicalNamesAndPrefersPassthroughFields() {
        Attribute bare = attr("cluster");
        Attribute prefixed = new ReferenceAttribute(Source.EMPTY, "labels.cluster", DataType.KEYWORD);
        Attribute packed = attr(TranslationContext.mapOpen(Set.of("pod")));

        assertThat(TranslationContext.find(List.of(bare, prefixed), "cluster"), sameInstance(prefixed));
        assertThat(TranslationContext.find(List.of(bare), "cluster"), sameInstance(bare));
        assertThat(TranslationContext.find(List.of(bare, packed), TranslationContext.mapOpen(Set.of("pod"))), sameInstance(packed));
        assertNull(TranslationContext.find(List.of(bare), "pod"));
        assertThat(TranslationContext.mapFinite(List.of(bare, prefixed, attr("pod"))), contains("cluster", "pod"));
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }
}
