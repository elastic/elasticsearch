/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Which joins let the {@code _doc} of their rows through on the data nodes, see {@link Join#docInput()}.
 */
public class JoinTests extends ESTestCase {

    /** A {@code LOOKUP JOIN} keeps each row of its left side, so the rows keep their {@code _doc}. */
    public void testALookupJoinPassesTheDocOfItsLeftRows() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        LogicalPlan left = relation("idx", IndexMode.STANDARD, List.of(leftKey));
        Join join = leftJoin(left, relation("lookup", IndexMode.LOOKUP, List.of(rightKey)), leftKey, rightKey);

        assertThat(join.docInput(), sameInstance(left));
    }

    /** A join whose right side isn't a lookup index isn't a {@code LOOKUP JOIN}, so it doesn't pass the {@code _doc}. */
    public void testAJoinOverAnotherRelationDoesNotPassTheDoc() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        LogicalPlan right = new LocalRelation(Source.EMPTY, List.of(rightKey), LocalSupplier.of(new Page(0)));
        Join join = leftJoin(relation("idx", IndexMode.STANDARD, List.of(leftKey)), right, leftKey, rightKey);

        assertThat(join.docInput(), nullValue());
    }

    /** {@code INLINE STATS} joins in another way, so it doesn't pass the {@code _doc}, even over a lookup index. */
    public void testAnInlineJoinDoesNotPassTheDoc() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        InlineJoin join = new InlineJoin(
            Source.EMPTY,
            relation("idx", IndexMode.STANDARD, List.of(leftKey)),
            relation("lookup", IndexMode.LOOKUP, List.of(rightKey)),
            new JoinConfig(JoinTypes.LEFT, List.of(leftKey), List.of(rightKey), null)
        );

        assertThat(join.docInput(), nullValue());
    }

    private static Join leftJoin(LogicalPlan left, LogicalPlan right, Attribute leftKey, Attribute rightKey) {
        return new Join(
            Source.EMPTY,
            left,
            right,
            JoinTypes.LEFT,
            List.of(leftKey),
            List.of(rightKey),
            null,
            ExecutesOn.ExecuteLocation.ANY
        );
    }

    private static EsRelation relation(String index, IndexMode mode, List<Attribute> output) {
        return new EsRelation(
            Source.EMPTY,
            index,
            mode,
            Map.of("", List.of(index)),
            Map.of("", List.of(index)),
            Map.of(index, new IndexProperties(mode, 0)),
            output
        );
    }
}
