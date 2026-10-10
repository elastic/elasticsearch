/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class SubqueryLookupJoinTests extends ESTestCase {

    public void testOutputUsesRightReferencesAndPreservesLeftJoinKey() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        FieldAttribute leftValue = getFieldAttribute("value", DataType.INTEGER);
        FieldAttribute rightValue = getFieldAttribute("value", DataType.INTEGER);
        SubqueryLookupJoin join = new SubqueryLookupJoin(
            Source.EMPTY,
            relation(List.of(key, leftValue)),
            relation(List.of(key, rightValue)),
            List.of(key)
        );

        assertThat(join.config().type(), sameInstance(JoinTypes.LEFT));
        assertThat(join.output().stream().filter(attribute -> attribute.name().equals("key")).toList(), contains(key));

        Attribute outputValue = join.output().stream().filter(attribute -> attribute.name().equals("value")).findFirst().orElseThrow();
        assertThat(outputValue, instanceOf(ReferenceAttribute.class));
        assertThat(outputValue.id(), equalTo(rightValue.id()));
    }

    public void testRebuildsRetainTheSubqueryLookupJoinType() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        LocalRelation left = relation(List.of(key));
        LocalRelation right = relation(List.of(key));
        SubqueryLookupJoin join = new SubqueryLookupJoin(Source.EMPTY, left, right, List.of(key));

        assertThat(join.replaceChildren(left, right), instanceOf(SubqueryLookupJoin.class));
        assertThat(join.withConfig(join.config()), instanceOf(SubqueryLookupJoin.class));
    }

    public void testIsNotSerialized() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        SubqueryLookupJoin join = new SubqueryLookupJoin(Source.EMPTY, relation(List.of(key)), relation(List.of(key)), List.of(key));

        expectThrows(UnsupportedOperationException.class, () -> join.writeTo((StreamOutput) null));
        expectThrows(UnsupportedOperationException.class, join::getWriteableName);
    }

    private static LocalRelation relation(List<Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, LocalSupplier.of(new Page(0)));
    }
}
