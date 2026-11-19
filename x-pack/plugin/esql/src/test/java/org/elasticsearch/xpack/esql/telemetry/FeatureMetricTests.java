/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * {@link FeatureMetric#set} throws for any plan node it cannot map and that is not on the exclusion list. Since the
 * exclusion list matches the {@code UnresolvedSourceRelation} marker, both {@code FROM}-style leaf shapes must be
 * excluded together — neither should raise. This pins the "telemetered together (i.e. not at all)" decision for the
 * two unresolved source relations.
 */
public class FeatureMetricTests extends ESTestCase {

    public void testIndexRelationIsExcluded() {
        UnresolvedRelation index = new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, "idx"),
            false,
            List.of(),
            IndexMode.STANDARD,
            null
        );
        // No throw == excluded as expected; an excluded node must not flip any feature bit.
        BitSet bitset = new BitSet();
        FeatureMetric.set(index, bitset);
        assertTrue("excluded plans must not set any telemetry bit", bitset.isEmpty());
    }

    public void testExternalRelationIsExcluded() {
        UnresolvedExternalRelation external = new UnresolvedExternalRelation(
            Source.EMPTY,
            Literal.keyword(Source.EMPTY, "s3://bucket/table"),
            Map.of()
        );
        // The external shape must be excluded via the shared marker, exactly like the index shape; a regression that
        // dropped it from the exclusion list would surface here as an EsqlIllegalArgumentException.
        BitSet bitset = new BitSet();
        FeatureMetric.set(external, bitset);
        assertTrue("excluded plans must not set any telemetry bit", bitset.isEmpty());
    }
}
