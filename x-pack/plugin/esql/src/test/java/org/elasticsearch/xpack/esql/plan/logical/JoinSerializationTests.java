/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinSerializationTests extends AbstractLogicalPlanSerializationTests<Join> {
    @Override
    protected Join createTestInstance() {
        Source source = randomSource();
        LogicalPlan left = randomChild(0);
        LogicalPlan right = randomChild(0);
        JoinConfig config = randomJoinConfig();
        return new Join(source, left, right, config);
    }

    private static JoinConfig randomJoinConfig() {
        JoinType type = randomFrom(JoinTypes.LEFT, JoinTypes.RIGHT, JoinTypes.INNER, JoinTypes.FULL, JoinTypes.CROSS);
        List<Attribute> matchFields = randomFieldAttributes(1, 10, false);
        List<Attribute> leftFields = randomFieldAttributes(1, 10, false);
        List<Attribute> rightFields = randomFieldAttributes(1, 10, false);
        return new JoinConfig(type, matchFields, leftFields, rightFields, randomJoinOnExpression());
    }

    private static Expression randomJoinOnExpression() {
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

    @Override
    protected Join mutateInstance(Join instance) throws IOException {
        LogicalPlan left = instance.left();
        LogicalPlan right = instance.right();
        JoinConfig config = instance.config();
        switch (between(0, 2)) {
            case 0 -> left = randomValueOtherThan(left, () -> randomChild(0));
            case 1 -> right = randomValueOtherThan(right, () -> randomChild(0));
            case 2 -> config = randomValueOtherThan(config, JoinSerializationTests::randomJoinConfig);
        }
        return new Join(instance.source(), left, right, config);
    }
}
