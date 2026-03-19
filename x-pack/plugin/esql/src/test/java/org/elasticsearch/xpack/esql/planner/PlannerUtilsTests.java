/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class PlannerUtilsTests extends ESTestCase {

    private static final EsField INT_FIELD = new EsField("val", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
    private static final Attribute ATTR = new FieldAttribute(Source.EMPTY, null, null, "val", INT_FIELD);
    private static final Source VIEW_SOURCE = new Source(1, 0, "FROM idx").withViewName("my_view");

    /**
     * The request-level QueryDSL filter must not be attached to a FragmentExec
     * whose logical plan contains only EsRelation nodes from within a view definition.
     */
    public void testRequestFilterNotAppliedToViewFragments() {
        EsRelation viewRelation = new EsRelation(
            VIEW_SOURCE,
            "view_idx",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("view_idx", IndexMode.STANDARD),
            List.of(ATTR)
        );
        assertTrue("EsRelation with view source should report fromView=true", viewRelation.fromView());

        FragmentExec fragment = new FragmentExec(viewRelation);
        PhysicalPlan result = PlannerUtils.integrateEsFilterIntoFragment(fragment, new TermQueryBuilder("val", 42));

        assertThat(result, instanceOf(FragmentExec.class));
        FragmentExec resultFragment = (FragmentExec) result;
        assertNull("Request filter should not be attached to view-sourced fragment", resultFragment.esFilter());
    }

    /**
     * The request-level QueryDSL filter must be attached to a FragmentExec
     * whose logical plan contains regular (non-view) EsRelation nodes.
     */
    public void testRequestFilterAppliedToRegularFragments() {
        EsRelation regularRelation = new EsRelation(
            Source.EMPTY,
            "regular_idx",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("regular_idx", IndexMode.STANDARD),
            List.of(ATTR)
        );
        assertFalse("EsRelation without view source should report fromView=false", regularRelation.fromView());

        FragmentExec fragment = new FragmentExec(regularRelation);
        PhysicalPlan result = PlannerUtils.integrateEsFilterIntoFragment(fragment, new TermQueryBuilder("val", 42));

        assertThat(result, instanceOf(FragmentExec.class));
        FragmentExec resultFragment = (FragmentExec) result;
        assertNotNull("Request filter should be attached to regular fragment", resultFragment.esFilter());
    }

    /**
     * {@link EsRelation#fromView()} derives its answer from the Source's viewName.
     */
    public void testFromViewDerivedFromSourceViewName() {
        EsRelation withView = new EsRelation(VIEW_SOURCE, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
        assertTrue(withView.fromView());

        EsRelation withoutView = new EsRelation(Source.EMPTY, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
        assertFalse(withoutView.fromView());
    }

    /**
     * When no filter is provided, the fragment is returned unchanged regardless of view status.
     */
    public void testNullFilterPassesThrough() {
        EsRelation viewRelation = new EsRelation(
            VIEW_SOURCE,
            "view_idx",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("view_idx", IndexMode.STANDARD),
            List.of(ATTR)
        );
        FragmentExec fragment = new FragmentExec(viewRelation);
        PhysicalPlan result = PlannerUtils.integrateEsFilterIntoFragment(fragment, null);
        assertSame("Null filter should return same plan", fragment, result);
    }
}
