/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.esql.session.EmptyExecutable;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ql.TestUtils.of;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class LogicalPlanOptimizerTests extends ESTestCase {

    public void testCombineLimits() throws Exception {
        var limitValues = new int[] { randomIntBetween(10, 99), randomIntBetween(100, 1000) };
        var firstLimit = randomBoolean() ? 0 : 1;
        var secondLimit = firstLimit == 0 ? 1 : 0;
        var oneLimit = new Limit(EMPTY, L(limitValues[firstLimit]), emptySource());
        var anotherLimit = new Limit(EMPTY, L(limitValues[secondLimit]), oneLimit);
        assertEquals(
            new Limit(EMPTY, L(Math.min(limitValues[0], limitValues[1])), emptySource()),
            new LogicalPlanOptimizer.CombineLimits().rule(anotherLimit)
        );
    }

    public void testMultipleCombineLimits() throws Exception {
        var numberOfLimits = randomIntBetween(3, 10);
        var minimum = randomIntBetween(10, 99);
        var limitWithMinimum = randomIntBetween(0, numberOfLimits);

        var plan = emptySource();
        for (int i = 0; i < numberOfLimits; i++) {
            var value = i == limitWithMinimum ? minimum : randomIntBetween(100, 1000);
            plan = new Limit(EMPTY, L(value), plan);
        }
        assertEquals(new Limit(EMPTY, L(minimum), emptySource()), optimizer().optimize(plan));
    }

    private static Literal L(Object value) {
        return of(value);
    }

    private static LogicalPlan emptySource() {
        return new LocalRelation(EMPTY, new EmptyExecutable(emptyList()));
    }

    private static LogicalPlanOptimizer optimizer() {
        return new LogicalPlanOptimizer();
    }
}
