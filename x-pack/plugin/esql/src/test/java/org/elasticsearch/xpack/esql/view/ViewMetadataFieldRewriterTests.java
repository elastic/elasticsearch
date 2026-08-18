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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class ViewMetadataFieldRewriterTests extends ESTestCase {

    private static final LogicalPlan BODY = new UnresolvedRelation(
        Source.EMPTY,
        new IndexPattern(Source.EMPTY, "languages"),
        false,
        List.of(),
        IndexMode.STANDARD,
        null,
        "FROM"
    );

    public void testNoOuterMetadataReturnsSameInstance() {
        LogicalPlan result = ViewMetadataFieldRewriter.rewrite("my_view", BODY, List.of());
        assertThat(result, sameInstance(BODY));
    }

    public void testOuterIndexMetadataWrapsBodyWithEval() {
        MetadataAttribute indexField = new MetadataAttribute(Source.EMPTY, MetadataAttribute.INDEX, DataType.KEYWORD, false);
        LogicalPlan result = ViewMetadataFieldRewriter.rewrite("my_view", BODY, List.of(indexField));

        assertThat(result, instanceOf(Eval.class));
        Eval eval = (Eval) result;
        assertThat(eval.child(), sameInstance(BODY));
        assertThat(eval.fields(), hasSize(1));
        Alias alias = eval.fields().getFirst();
        assertEquals(MetadataAttribute.INDEX, alias.name());
        assertEquals("my_view", alias.child().toString());
    }

    public void testUnrecognizedOuterMetadataReturnsSameInstance() {
        MetadataAttribute scoreField = new MetadataAttribute(Source.EMPTY, "_score", DataType.DOUBLE, false);
        LogicalPlan result = ViewMetadataFieldRewriter.rewrite("my_view", BODY, List.of(scoreField));
        assertThat(result, sameInstance(BODY));
    }
}
