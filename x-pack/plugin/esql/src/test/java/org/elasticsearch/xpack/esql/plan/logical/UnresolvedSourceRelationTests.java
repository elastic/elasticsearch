/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Pins the {@link UnresolvedSourceRelation} marker contract: both {@code FROM}-style leaf shapes implement it, and a
 * traversal keyed on the marker sees both shapes while a traversal keyed on the concrete index relation sees only the
 * index shape. The latter assertion is the drift the marker exists to make explicit — a site that needs every
 * FROM-style leaf must match the marker, not {@link UnresolvedRelation}.
 */
public class UnresolvedSourceRelationTests extends ESTestCase {

    private static UnresolvedRelation indexRelation(String index) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, index), false, List.of(), IndexMode.STANDARD, null);
    }

    private static UnresolvedExternalRelation externalRelation(String path) {
        return new UnresolvedExternalRelation(Source.EMPTY, Literal.keyword(Source.EMPTY, path), Map.of());
    }

    public void testBothShapesImplementMarker() {
        assertThat(indexRelation("idx"), instanceOf(UnresolvedSourceRelation.class));
        assertThat(externalRelation("s3://bucket/table"), instanceOf(UnresolvedSourceRelation.class));
    }

    public void testMarkerTraversalFindsBothShapes() {
        UnresolvedRelation index = indexRelation("idx");
        UnresolvedExternalRelation external = externalRelation("s3://bucket/table");
        LogicalPlan plan = new UnionAll(Source.EMPTY, List.of(index, external), List.of());

        // The marker is not a LogicalPlan subtype, so it is matched via instanceof inside a plain traversal rather
        // than the typed forEachUp(Class, ...) overload. Both FROM-style leaves are found.
        List<LogicalPlan> found = plan.collect(p -> p instanceof UnresolvedSourceRelation);

        assertThat(found, containsInAnyOrder(index, external));
    }

    public void testConcreteIndexTraversalMissesExternalShape() {
        UnresolvedRelation index = indexRelation("idx");
        UnresolvedExternalRelation external = externalRelation("s3://bucket/table");
        LogicalPlan plan = new UnionAll(Source.EMPTY, List.of(index, external), List.of());

        List<UnresolvedRelation> indexOnly = new ArrayList<>();
        plan.forEachUp(UnresolvedRelation.class, indexOnly::add);

        // The external leaf is deliberately invisible to an UnresolvedRelation-keyed traversal; this is the parity
        // gap that index-only audit sites rely on and that the marker closes for sites needing both shapes.
        assertEquals(List.of(index), indexOnly);
    }
}
