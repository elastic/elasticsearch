/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LookupJoinExecSerializationTests extends AbstractPhysicalPlanSerializationTests<LookupJoinExec> {

    public static Expression randomJoinOnExpression() {
        int randomInt = between(1, 20);
        List<Expression> expressionList = new ArrayList<>();
        for (int i = 0; i < randomInt; i++) {
            expressionList.add(randomJoinPredicate());
        }
        return Predicates.combineAnd(expressionList);

    }

    private static Expression randomJoinPredicate() {
        return randomBinaryComparisonOperator().buildNewInstance(randomSource(), randomAttribute(), randomAttribute());
    }

    private static EsqlBinaryComparison.BinaryComparisonOperation randomBinaryComparisonOperator() {
        return randomFrom(EsqlBinaryComparison.BinaryComparisonOperation.values());
    }

    private static Attribute randomAttribute() {
        return randomFieldAttributes(1, 1, false).get(0);
    }

    public static LookupJoinExec randomLookupJoinExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        PhysicalPlan lookup = randomChild(depth);
        List<Attribute> leftFields = randomFields();
        List<Attribute> rightFields = randomFields();
        List<Attribute> addedFields = randomFields();
        return new LookupJoinExec(source, child, lookup, leftFields, rightFields, addedFields, randomJoinOnExpression());
    }

    private static List<Attribute> randomFields() {
        return randomFieldAttributes(1, 5, false);
    }

    @Override
    protected LookupJoinExec createTestInstance() {
        return randomLookupJoinExec(0);
    }

    @Override
    protected LookupJoinExec mutateInstance(LookupJoinExec instance) throws IOException {
        PhysicalPlan child = instance.left();
        PhysicalPlan lookup = instance.lookup();
        List<Attribute> leftFields = randomFields();
        List<Attribute> rightFields = randomFields();
        List<Attribute> addedFields = randomFields();
        Expression joinOnConditions = instance.joinOnConditions();
        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> lookup = randomValueOtherThan(lookup, () -> randomChild(0));
            case 2 -> leftFields = randomValueOtherThan(leftFields, LookupJoinExecSerializationTests::randomFields);
            case 3 -> rightFields = randomValueOtherThan(rightFields, LookupJoinExecSerializationTests::randomFields);
            case 4 -> addedFields = randomValueOtherThan(addedFields, LookupJoinExecSerializationTests::randomFields);
            case 5 -> joinOnConditions = randomValueOtherThan(joinOnConditions, LookupJoinExecSerializationTests::randomJoinOnExpression);
            default -> throw new UnsupportedOperationException();
        }
        return new LookupJoinExec(instance.source(), child, lookup, leftFields, rightFields, addedFields, joinOnConditions);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
