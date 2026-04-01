/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.time.Duration;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AggregateFunctionWindowTests extends ESTestCase {

    public void testNegativeDurationProducesTrailingWindow() {
        var folded = Expressions.foldMap(
            Literal.timeDuration(Source.EMPTY, Duration.ofMinutes(-10)),
            FoldContext.small(),
            AggregateFunction.TSWindow.TYPE
        );

        assertThat(folded, instanceOf(AggregateFunction.TSBack.class));
        assertThat(((AggregateFunction.TSBack) folded).length(), equalTo(Duration.ofMinutes(10)));
    }

    public void testPositiveDurationProducesFollowingWindow() {
        var folded = Expressions.foldMap(
            Literal.timeDuration(Source.EMPTY, Duration.ofMinutes(10)),
            FoldContext.small(),
            AggregateFunction.TSWindow.TYPE
        );

        assertThat(folded, instanceOf(AggregateFunction.TSFront.class));
        assertThat(((AggregateFunction.TSFront) folded).length(), equalTo(Duration.ofMinutes(10)));
    }

    public void testZeroDurationHasNoWindow() {
        var folded = Expressions.foldMap(
            Literal.timeDuration(Source.EMPTY, Duration.ZERO),
            FoldContext.small(),
            AggregateFunction.TSWindow.TYPE
        );

        assertThat(folded, instanceOf(AggregateFunction.TSFront.class));
        assertThat(((AggregateFunction.TSFront) folded).length(), equalTo(Duration.ZERO));
    }
}
