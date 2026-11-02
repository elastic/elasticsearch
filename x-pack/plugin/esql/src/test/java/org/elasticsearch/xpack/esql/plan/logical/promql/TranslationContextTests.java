/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class TranslationContextTests extends ESTestCase {

    public void testPackedNameDerivesFromTheSkipSet() {
        assertThat(PromqlLabels.metadataLabelName(Set.of()), equalTo(MetadataAttribute.TIMESERIES));
        assertThat(PromqlLabels.metadataLabelName(Set.of("region", "pod")), equalTo(MetadataAttribute.TIMESERIES + "$pod$region"));
        assertThat(
            PromqlLabels.metadataLabelName(Set.of("pod", "region")),
            equalTo(PromqlLabels.metadataLabelName(Set.of("region", "pod")))
        );
    }

    public void testFindByNameMatchesCanonicalNamesAndPrefersPassthroughFields() {
        Attribute bare = attr("cluster");
        Attribute prefixed = new ReferenceAttribute(Source.EMPTY, "labels.cluster", DataType.KEYWORD);
        Attribute packed = attr(PromqlLabels.metadataLabelName(Set.of("pod")));

        assertThat(PromqlLabels.getLabel(List.of(bare, prefixed), "cluster"), sameInstance(prefixed));
        assertThat(PromqlLabels.getLabel(List.of(bare), "cluster"), sameInstance(bare));
        assertThat(PromqlLabels.getLabel(List.of(bare, packed), PromqlLabels.metadataLabelName(Set.of("pod"))), sameInstance(packed));
        assertNull(PromqlLabels.getLabel(List.of(bare), "pod"));
    }

    /** A result rebuilt around a new plan and value keeps its step, pending filter and lifecycle. */
    public void testWithKeepsTheResultState() {
        Attribute step = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
        Attribute value = new ReferenceAttribute(Source.EMPTY, "value", DataType.DOUBLE);
        var plan = new LocalRelation(Source.EMPTY, List.of(step, value), EmptyLocalSupplier.EMPTY);
        var result = new IntermediateResult(plan, value, step, Literal.TRUE, IntermediateResult.Kind.BEFORE_INITIAL_AGGREGATE);

        var updatedValue = new Literal(Source.EMPTY, 42.0, DataType.DOUBLE);
        var updated = result.with(plan, updatedValue);
        assertSame(updatedValue, updated.value());
        assertSame(step, updated.step());
        assertSame(result.pendingFilter(), updated.pendingFilter());
        assertSame(result.kind(), updated.kind());
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }
}
