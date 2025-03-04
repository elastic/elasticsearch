/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import java.util.Collections;
import java.util.Map;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

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

    public void testLookupJoinErrorMessage() {
        Failures failures = new Failures();

        String indexPattern = "test1";
        Map<String, IndexMode> indexNameWithModes = Collections.emptyMap();

        EsRelation fakeRelation = new EsRelation(Source.EMPTY, indexPattern, IndexMode.LOOKUP, indexNameWithModes, List.of());

        EsField empNoEsField = new EsField("emp_no", DataType.INTEGER, Collections.emptyMap(), false);
        FieldAttribute empNoField = new FieldAttribute(Source.EMPTY, "emp_no", empNoEsField);
        Alias empNoAlias = new Alias(Source.EMPTY, "emp_no", empNoField);
        LogicalPlan left = new Row(Source.EMPTY, List.of(empNoAlias));

        LookupJoin lookupJoin = new LookupJoin(Source.EMPTY, left, fakeRelation,
            new JoinConfig(JoinTypes.LEFT, List.of(), List.of(), List.of()));

        lookupJoin.postAnalysisVerification(failures);

        String expectedMessage = "Index [test1] exists, but no valid fields for LOOKUP JOIN were found";
        assertTrue(failures.toString().contains(expectedMessage));
    }

    public void testLookupJoinErrorMessage_MultipleIndices() {
        Failures failures = new Failures();

        String indexPattern = "test1";
        Map<String, IndexMode> indexNameWithModes = Map.of(
            "test1", IndexMode.LOOKUP,
            "test2", IndexMode.LOOKUP
        );

        EsField languagesEsField = new EsField("languages", DataType.KEYWORD, Collections.emptyMap(), true);
        FieldAttribute languagesField = new FieldAttribute(Source.EMPTY, "languages", languagesEsField);
        EsRelation fakeRelation = new EsRelation(Source.EMPTY, indexPattern, IndexMode.LOOKUP, indexNameWithModes, List.of(languagesField));

        EsField empNoEsField = new EsField("emp_no", DataType.INTEGER, Collections.emptyMap(), false);
        FieldAttribute empNoField = new FieldAttribute(Source.EMPTY, "emp_no", empNoEsField);
        Alias empNoAlias = new Alias(Source.EMPTY, "emp_no", empNoField);
        LogicalPlan left = new Row(Source.EMPTY, List.of(empNoAlias));

        LookupJoin lookupJoin = new LookupJoin(Source.EMPTY, left, fakeRelation,
            new JoinConfig(JoinTypes.LEFT, List.of(), List.of(), List.of()));

        lookupJoin.postAnalysisVerification(failures);

        String expectedMessage = "Invalid [test1] resolution in lookup mode to [2] indices";
        assertTrue(failures.toString().contains(expectedMessage));
    }
}
