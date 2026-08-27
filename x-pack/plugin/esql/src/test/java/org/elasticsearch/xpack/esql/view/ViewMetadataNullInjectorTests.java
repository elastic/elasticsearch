/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class ViewMetadataNullInjectorTests extends ESTestCase {

    private static final List<NamedExpression> ALL_META_FIELDS = MetadataAttribute.ATTRIBUTES_MAP.keySet()
        .stream()
        .sorted()
        .map(name -> MetadataAttribute.create(Source.EMPTY, name))
        .toList();

    private static final LogicalPlan BODY_NO_META = new UnresolvedRelation(
        Source.EMPTY,
        new IndexPattern(Source.EMPTY, "languages"),
        false,
        List.of(),
        IndexMode.STANDARD,
        null,
        "FROM"
    );

    private static final LogicalPlan BODY_WITH_ALL_META = new UnresolvedRelation(
        Source.EMPTY,
        new IndexPattern(Source.EMPTY, "languages"),
        false,
        ALL_META_FIELDS,
        IndexMode.STANDARD,
        null,
        "FROM"
    );

    public void testNoOuterMetadata_returnsSameInstance() {
        assertThat(ViewMetadataNullInjector.inject(BODY_NO_META, List.of()), sameInstance(BODY_NO_META));
    }

    public void testBodyLacksAllFields_injectsNullsForAllOuterRequested() {
        LogicalPlan result = ViewMetadataNullInjector.inject(BODY_NO_META, ALL_META_FIELDS);

        assertThat(result, instanceOf(Eval.class));
        Eval eval = (Eval) result;
        assertThat(eval.child(), sameInstance(BODY_NO_META));
        assertThat(eval.fields(), hasSize(ALL_META_FIELDS.size()));
        for (int i = 0; i < ALL_META_FIELDS.size(); i++) {
            NamedExpression field = ALL_META_FIELDS.get(i);
            Alias alias = eval.fields().get(i);
            assertEquals(field.name(), alias.name());
            assertThat(alias.child(), instanceOf(Literal.class));
            Literal lit = (Literal) alias.child();
            assertNull(lit.value());
            assertEquals(field.dataType(), lit.dataType());
        }
    }

    public void testBodyHasAllFields_outerRequestsAll_returnsSameInstance() {
        assertThat(ViewMetadataNullInjector.inject(BODY_WITH_ALL_META, ALL_META_FIELDS), sameInstance(BODY_WITH_ALL_META));
    }
}
