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

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.newFinite;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslationContext.newOpen;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class TranslationContextTests extends ESTestCase {

    public void testUnionMergesLabelsAndSkipSets() {
        Header header = newFinite(List.of("cluster")).union(newOpen(Set.of("pod")))
            .union(newOpen(Set.of("pod")))
            .union(newFinite(List.of("cluster", "region")));

        assertThat(header.labels(), contains("cluster", "region"));
        assertThat(header.skips(), contains(Set.of("pod")));
        assertThat(header.union(Header.EMPTY), equalTo(header));
    }

    public void testDifferenceDropsLabelsAndWidensSkipSets() {
        Header above = newFinite(List.of("cluster", "pod")).union(newOpen(Set.of("region")));

        Header below = above.difference(List.of("pod"));
        assertThat(below.labels(), contains("cluster"));
        assertThat(below.skips(), contains(Set.of("region", "pod")));

        // the regroup's own column composes as a second, finer skip set
        Header child = below.union(newOpen(Set.of("pod")));
        assertThat(child.skips(), containsInAnyOrder(Set.of("region", "pod"), Set.of("pod")));
        assertThat(child.finestSkip(), equalTo(Set.of("pod")));
    }

    public void testSurvivingIsTheUpwardCounterpartOfDifference() {
        Header required = newFinite(List.of("cluster", "pod")).union(newOpen(Set.of("region")));
        Header child = required.difference(List.of("pod")).union(newOpen(Set.of("pod")));

        Header lifted = child.surviving(List.of("pod"));

        // every column the parent required, apart from the dropped label, comes back
        assertThat(lifted.labels(), contains("cluster"));
        assertThat(lifted.skips(), contains(Set.of("region", "pod")));
        // the regroup's own full label space does not survive dropping a label it still carries
        assertFalse(newOpen().surviving(List.of("pod")).hasPacked());
        // without () keeps everything
        assertThat(child.surviving(List.of()), equalTo(child));
    }

    public void testRetainLabelsKeepsSkipSets() {
        Header header = newFinite(List.of("cluster", "pod", "region")).union(newOpen(Set.of("pod")));

        Header retained = header.retainLabels(List.of("cluster", "missing"));

        assertThat(retained.labels(), contains("cluster"));
        assertThat(retained.skips(), contains(Set.of("pod")));
    }

    public void testPackedNameDerivesFromTheSkipSet() {
        assertThat(TranslationContext.packedName(Set.of()), equalTo(MetadataAttribute.TIMESERIES));
        assertThat(TranslationContext.packedName(Set.of("region", "pod")), equalTo(MetadataAttribute.TIMESERIES + "$pod$region"));
        assertThat(TranslationContext.packedName(Set.of("pod", "region")), equalTo(TranslationContext.packedName(Set.of("region", "pod"))));
    }

    public void testFindByNameMatchesCanonicalNamesAndPrefersPassthroughFields() {
        Attribute bare = attr("cluster");
        Attribute prefixed = new ReferenceAttribute(Source.EMPTY, "labels.cluster", DataType.KEYWORD);
        Attribute packed = attr(TranslationContext.packedName(Set.of("pod")));

        assertThat(TranslationContext.findByName(List.of(bare, prefixed), "cluster"), sameInstance(prefixed));
        assertThat(TranslationContext.findByName(List.of(bare), "cluster"), sameInstance(bare));
        assertThat(
            TranslationContext.findByName(List.of(bare, packed), TranslationContext.packedName(Set.of("pod"))),
            sameInstance(packed)
        );
        assertNull(TranslationContext.findByName(List.of(bare), "pod"));
        assertThat(TranslationContext.mapToNames(List.of(bare, prefixed, attr("pod"))), contains("cluster", "pod"));
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }
}
