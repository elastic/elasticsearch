/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.session.Configuration;

import static org.hamcrest.Matchers.equalTo;

public class LogicalOptimizerContextTests extends ESTestCase {
    public void testToString() {
        // Random looking numbers for FoldContext are indeed random. Just so we have consistent numbers to assert on in toString.
        LogicalOptimizerContext ctx = new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, new FoldContext(102));
        ctx.foldCtx().trackAllocation(Source.EMPTY, 99);
        assertThat(
            ctx.toString(),
            equalTo("LogicalOptimizerContext[configuration=" + EsqlTestUtils.TEST_CFG + ", foldCtx=FoldContext[3/102]]")
        );
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(randomLogicalOptimizerContext(), this::copy, this::mutate);
    }

    private LogicalOptimizerContext randomLogicalOptimizerContext() {
        return new LogicalOptimizerContext(ConfigurationTestUtils.randomConfiguration(), randomFoldContext());
    }

    private LogicalOptimizerContext copy(LogicalOptimizerContext c) {
        return new LogicalOptimizerContext(c.configuration(), c.foldCtx());
    }

    private LogicalOptimizerContext mutate(LogicalOptimizerContext c) {
        Configuration configuration = c.configuration();
        FoldContext foldCtx = c.foldCtx();
        if (randomBoolean()) {
            configuration = randomValueOtherThan(configuration, ConfigurationTestUtils::randomConfiguration);
        } else {
            foldCtx = randomValueOtherThan(foldCtx, this::randomFoldContext);
        }
        return new LogicalOptimizerContext(configuration, foldCtx);
    }

    private FoldContext randomFoldContext() {
        FoldContext ctx = new FoldContext(randomNonNegativeLong());
        if (randomBoolean()) {
            ctx.trackAllocation(Source.EMPTY, randomLongBetween(0, ctx.initialAllowedBytes()));
        }
        return ctx;
    }
}
