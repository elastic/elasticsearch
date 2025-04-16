/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class JoinTests extends ESTestCase {
    @AwaitsFix(bugUrl = "Test needs updating to the new JOIN planning")
    public void testExpressionsAndReferences() {
        int numMatchFields = between(1, 10);

        List<Attribute> matchFields = new ArrayList<>(numMatchFields);
        List<Alias> leftFields = new ArrayList<>(numMatchFields);
        List<Attribute> leftAttributes = new ArrayList<>(numMatchFields);
        List<Alias> rightFields = new ArrayList<>(numMatchFields);
        List<Attribute> rightAttributes = new ArrayList<>(numMatchFields);

        for (int i = 0; i < numMatchFields; i++) {
            Alias left = aliasForLiteral("left" + i);
            Alias right = aliasForLiteral("right" + i);

            leftFields.add(left);
            leftAttributes.add(left.toAttribute());
            rightFields.add(right);
            rightAttributes.add(right.toAttribute());
            matchFields.add(randomBoolean() ? left.toAttribute() : right.toAttribute());
        }

        Row left = new Row(Source.EMPTY, leftFields);
        Row right = new Row(Source.EMPTY, rightFields);

        JoinConfig joinConfig = new JoinConfig(JoinTypes.LEFT, matchFields, leftAttributes, rightAttributes);
        Join join = new Join(Source.EMPTY, left, right, joinConfig);

        // matchfields are a subset of the left and right fields, so they don't contribute to the size of the references set.
        // assertEquals(2 * numMatchFields, join.references().size());

        AttributeSet refs = join.references();
        assertTrue(refs.containsAll(matchFields));
        assertTrue(refs.containsAll(leftAttributes));
        assertTrue(refs.containsAll(rightAttributes));

        Set<Expression> exprs = Set.copyOf(join.expressions());
        assertTrue(exprs.containsAll(matchFields));
        assertTrue(exprs.containsAll(leftAttributes));
        assertTrue(exprs.containsAll(rightAttributes));
    }

    public void testTransformExprs() {
        int numMatchFields = between(1, 10);

        List<Attribute> matchFields = new ArrayList<>(numMatchFields);
        List<Alias> leftFields = new ArrayList<>(numMatchFields);
        List<Attribute> leftAttributes = new ArrayList<>(numMatchFields);
        List<Alias> rightFields = new ArrayList<>(numMatchFields);
        List<Attribute> rightAttributes = new ArrayList<>(numMatchFields);

        for (int i = 0; i < numMatchFields; i++) {
            Alias left = aliasForLiteral("left" + i);
            Alias right = aliasForLiteral("right" + i);

            leftFields.add(left);
            leftAttributes.add(left.toAttribute());
            rightFields.add(right);
            rightAttributes.add(right.toAttribute());
            matchFields.add(randomBoolean() ? left.toAttribute() : right.toAttribute());
        }

        Row left = new Row(Source.EMPTY, leftFields);
        Row right = new Row(Source.EMPTY, rightFields);

        JoinConfig joinConfig = new JoinConfig(JoinTypes.LEFT, matchFields, leftAttributes, rightAttributes);
        Join join = new Join(Source.EMPTY, left, right, joinConfig);
        assertTrue(join.config().matchFields().stream().allMatch(ref -> ref.dataType().equals(DataType.INTEGER)));

        Join transformedJoin = (Join) join.transformExpressionsOnly(Attribute.class, attr -> attr.withDataType(DataType.BOOLEAN));
        assertTrue(transformedJoin.config().matchFields().stream().allMatch(ref -> ref.dataType().equals(DataType.BOOLEAN)));
    }

    private static Alias aliasForLiteral(String name) {
        return new Alias(Source.EMPTY, name, new Literal(Source.EMPTY, 1, DataType.INTEGER));
    }
}
